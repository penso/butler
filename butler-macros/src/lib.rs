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
//! - `async fn send_email(to, subject) -> Result<JobId, butler::Error>`: awaiting it
//!   serializes the arguments and pushes a job onto the queue (like `perform_async`).
//! - a hidden `__butler_perform_send_email` holding the original body.
//! - a registration entry so any `Worker` in the same binary can dispatch it by name.
//! - `send_email::JOB`, a handle for `Worker::register`. Jobs defined in another
//!   crate need it: the linker drops that crate's automatic registration unless
//!   the binary references something from it.
//!
//! Job bodies must be `Send`, so a tokio worker can spawn them.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{FnArg, ItemFn, LitStr, Pat, parse_macro_input, spanned::Spanned};

#[proc_macro_attribute]
pub fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    let mut job_name: Option<LitStr> = None;
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            job_name = Some(meta.value()?.parse()?);
            Ok(())
        } else {
            Err(meta.error("unsupported job attribute, expected `name = \"...\"`"))
        }
    });
    parse_macro_input!(attr with parser);

    match expand(func, job_name) {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn expand(func: ItemFn, job_name: Option<LitStr>) -> syn::Result<proc_macro2::TokenStream> {
    let sig = &func.sig;
    if sig.asyncness.is_none() {
        return Err(syn::Error::new(sig.fn_token.span(), "#[job] functions must be async"));
    }
    if !sig.generics.params.is_empty() {
        return Err(syn::Error::new(sig.generics.span(), "#[job] functions cannot be generic"));
    }

    let mut idents = Vec::new();
    let mut types = Vec::new();
    for input in &sig.inputs {
        let FnArg::Typed(pat_type) = input else {
            return Err(syn::Error::new(input.span(), "#[job] functions cannot take self"));
        };
        let Pat::Ident(pat_ident) = &*pat_type.pat else {
            return Err(syn::Error::new(
                pat_type.pat.span(),
                "#[job] arguments must be plain identifiers",
            ));
        };
        idents.push(pat_ident.ident.clone());
        types.push((*pat_type.ty).clone());
    }

    let vis = &func.vis;
    let attrs = &func.attrs;
    let name = &sig.ident;
    let job_name = job_name.unwrap_or_else(|| LitStr::new(&name.to_string(), name.span()));
    let perform = format_ident!("__butler_perform_{}", name);
    let dispatch = format_ident!("__butler_dispatch_{}", name);
    let inputs = &sig.inputs;
    let output = &sig.output;
    let body = &func.block;
    let indices = 0..idents.len();

    Ok(quote! {
        #(#attrs)*
        #vis async fn #name(#(#idents: #types),*)
            -> ::core::result::Result<::butler::JobId, ::butler::Error>
        {
            let args = ::std::vec![
                #(::butler::__private::serde_json::to_value(&#idents)?),*
            ];
            ::butler::__private::enqueue(#job_name, args).await
        }

        #[doc(hidden)]
        #[allow(non_snake_case)]
        #vis async fn #perform(#inputs) #output #body

        #[doc(hidden)]
        #[allow(non_snake_case)]
        fn #dispatch(args: ::std::vec::Vec<::butler::__private::serde_json::Value>)
            -> ::butler::__private::BoxFuture
        {
            ::std::boxed::Box::pin(async move {
                let mut args = args.into_iter();
                #(
                    let #idents: #types = ::butler::__private::arg(&mut args, #job_name, #indices)?;
                )*
                ::butler::IntoJobResult::into_job_result(#perform(#(#idents),*).await)
            })
        }

        #[doc(hidden)]
        #vis mod #name {
            /// Pass to `Worker::register` when the job lives in another crate.
            pub const JOB: ::butler::JobDef =
                ::butler::JobDef { name: #job_name, perform: super::#dispatch };
        }

        ::butler::__private::inventory::submit! { #name::JOB }
    })
}
