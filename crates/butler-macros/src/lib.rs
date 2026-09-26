//! `#[job]` attribute macro for butler.
//!
//! Turns
//!
//! ```ignore
//! #[butler::job]
//! async fn send_email(to: String, subject: String) -> Result<(), MyError> { ... }
//! ```
//!
//! into:
//! - `fn send_email(to: impl JobArg<String>, subject: impl JobArg<String>)`,
//!   returning a `Send + 'static` future of `Result<JobHandle, butler::Error>`.
//!   Awaiting it pushes a job onto the queue (like `perform_async`). The
//!   arguments are converted and serialized during the call, so the future
//!   never borrows them.
//! - a hidden `__butler_perform_send_email` holding the original body.
//! - a registration entry so any `Worker` in the same binary can dispatch it by name.
//!
//! Attributes: `#[job(name = "billing.charge")]` sets the name workers look the
//! job up by (default: the function name), and `#[job(queue = "mailers")]` the
//! queue it is enqueued on (default: `"default"`).
//! - `send_email::prepare(...)`, which builds the job without enqueueing it,
//!   for `butler::enqueue_all` or `.on_queue(..)`.
//! - `send_email::JOB`, a handle for `Worker::register`. Jobs defined in another
//!   crate need it: the linker drops that crate's automatic registration unless
//!   the binary references something from it.
//!
//! A parameter of type `butler::Progress<S>` makes the job resumable: the
//! worker provides it from saved progress, and callers don't pass it.
//!
//! The function may be `async` (runs as a task on the worker's runtime) or a
//! plain `fn` (runs on the blocking thread pool, for CPU-bound work). Job
//! bodies must be `Send`, so a tokio worker can spawn them.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, ItemFn, LitStr, Pat, ReturnType, Type, parse_macro_input, parse_quote, spanned::Spanned,
};

#[proc_macro_attribute]
pub fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    let mut job_name: Option<LitStr> = None;
    let mut queue: Option<LitStr> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            job_name = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("queue") {
            let value: LitStr = meta.value()?.parse()?;
            if !is_valid_queue_name(&value.value()) {
                return Err(syn::Error::new(
                    value.span(),
                    "queue names are 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)",
                ));
            }
            queue = Some(value);
            Ok(())
        } else {
            Err(meta
                .error("unsupported job attribute, expected `name = \"...\"` or `queue = \"...\"`"))
        }
    });
    parse_macro_input!(attr with parser);

    match expand(func, job_name, queue) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

/// Whether a parameter is the job's `Progress`, which the worker provides.
fn is_progress(ty: &Type) -> bool {
    matches!(ty, Type::Path(path)
        if path.qself.is_none()
            && path.path.segments.last().is_some_and(|segment| segment.ident == "Progress"))
}

/// Same rule as `butler::is_valid_queue_name`, checked at compile time.
fn is_valid_queue_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

fn expand(
    func: ItemFn,
    job_name: Option<LitStr>,
    queue: Option<LitStr>,
) -> syn::Result<proc_macro2::TokenStream> {
    let sig = &func.sig;
    let is_async = sig.asyncness.is_some();
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new(
            sig.generics.span(),
            "#[job] functions cannot be generic",
        ));
    }

    // Arguments callers pass (serialized), and every argument in order (the call).
    let mut idents = Vec::new();
    let mut types = Vec::new();
    let mut call_args = Vec::new();
    let mut progress = None;
    for input in &sig.inputs {
        let FnArg::Typed(pat_type) = input else {
            return Err(syn::Error::new(
                input.span(),
                "#[job] functions cannot take self",
            ));
        };
        let Pat::Ident(pat_ident) = &*pat_type.pat else {
            return Err(syn::Error::new(
                pat_type.pat.span(),
                "#[job] arguments must be plain identifiers",
            ));
        };
        call_args.push(pat_ident.ident.clone());
        if is_progress(&pat_type.ty) {
            if progress.is_some() {
                return Err(syn::Error::new(
                    pat_type.ty.span(),
                    "#[job] functions take at most one Progress",
                ));
            }
            if !is_async {
                return Err(syn::Error::new(
                    pat_type.ty.span(),
                    "Progress needs an async #[job]: its checkpoints are awaited",
                ));
            }
            progress = Some((pat_ident.ident.clone(), (*pat_type.ty).clone()));
            continue;
        }
        idents.push(pat_ident.ident.clone());
        types.push((*pat_type.ty).clone());
    }
    // Built by the worker from saved progress, never passed by callers.
    let progress_init = progress.as_ref().map(|(ident, ty)| {
        quote! {
            let #ident: #ty = <#ty>::resume(&call.checkpoints)?;
        }
    });

    let vis = &func.vis;
    let attrs = &func.attrs;
    let name = &sig.ident;
    let job_name = job_name.unwrap_or_else(|| LitStr::new(&name.to_string(), name.span()));
    let queue = match queue {
        Some(queue) => quote!(#queue),
        None => quote!(::butler::DEFAULT_QUEUE),
    };
    let perform = format_ident!("__butler_perform_{}", name);
    let dispatch = format_ident!("__butler_dispatch_{}", name);
    let inputs = &sig.inputs;
    let output = &sig.output;
    let body = &func.block;
    let indices = 0..idents.len();
    let returns: Type = match output {
        ReturnType::Default => parse_quote!(()),
        ReturnType::Type(_, ty) => (**ty).clone(),
    };

    // Async bodies run as they are, on the worker's runtime. Sync bodies go to
    // the blocking pool, so CPU-heavy work doesn't stall async worker threads.
    let (perform_fn, run) = if is_async {
        (
            quote! { #vis async fn #perform(#inputs) #output #body },
            quote! { #perform(#(#call_args),*).await },
        )
    } else {
        (
            quote! { #vis fn #perform(#inputs) #output #body },
            quote! { ::butler::__private::run_blocking(move || #perform(#(#call_args),*)).await? },
        )
    };

    Ok(quote! {
        #(#attrs)*
        #vis fn #name(#(#idents: impl ::butler::JobArg<#types>),*)
            -> impl ::core::future::Future<
                Output = ::core::result::Result<
                    ::butler::JobHandle<<#returns as ::butler::IntoJobResult>::Output>,
                    ::butler::Error,
                >,
            > + ::core::marker::Send + 'static
        {
            #(let #idents: #types = ::butler::JobArg::into_arg(#idents);)*
            let args = ::butler::__private::args([
                #(::butler::__private::serde_json::to_value(&#idents)),*
            ]);
            async move { ::butler::__private::enqueue(&#name::JOB, args?).await }
        }

        #[doc(hidden)]
        #[allow(non_snake_case)]
        #perform_fn

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #dispatch(call: ::butler::__private::Invocation) -> ::butler::__private::BoxFuture {
            ::std::boxed::Box::pin(async move {
                let mut args = call.args.into_iter();
                #(
                    let #idents: #types = ::butler::__private::arg(&mut args, #job_name, #indices)?;
                )*
                #progress_init
                ::butler::__private::output(#run)
            })
        }

        #[doc(hidden)]
        #vis mod #name {
            // The job's argument and output types are named as in the parent.
            #[allow(unused_imports)]
            use super::*;

            /// Pass to `Worker::register` when the job lives in another crate.
            pub const JOB: ::butler::JobDef =
                ::butler::JobDef { name: #job_name, queue: #queue, perform: super::#dispatch };

            /// Builds this job without enqueueing it, for `butler::enqueue_all`
            /// (or `.on_queue(..)`, then `.enqueue()`).
            pub fn prepare(#(#idents: impl ::butler::JobArg<#types>),*)
                -> ::core::result::Result<
                    ::butler::PreparedJob<<#returns as ::butler::IntoJobResult>::Output>,
                    ::butler::Error,
                >
            {
                #(let #idents: #types = ::butler::JobArg::into_arg(#idents);)*
                let args = ::butler::__private::args([
                    #(::butler::__private::serde_json::to_value(&#idents)),*
                ])?;
                ::core::result::Result::Ok(::butler::PreparedJob::new(&JOB, args))
            }
        }

        ::butler::__private::inventory::submit! { #name::JOB }
    })
}
