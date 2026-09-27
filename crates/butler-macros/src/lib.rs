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
//!   returning a `Send + 'static` `butler::JobCall`. Awaiting it pushes a job
//!   onto the queue (like `perform_async`) and returns a `JobHandle`;
//!   `.now().await` runs the body here instead and returns its output (like
//!   `perform_now`). The arguments are converted and serialized during the
//!   call, so the `JobCall` never borrows them.
//! - a hidden `__butler_perform_send_email` holding the original body.
//! - a registration entry so any `Worker` in the same binary can dispatch it by name.
//!
//! Attributes: `#[job(name = "billing.charge")]` sets the name workers look the
//! job up by (default: the function name). That name is stored with every
//! queued job, so give jobs a stable `name` before their functions get renamed;
//! `#[job(aliases = ["old_name"])]` keeps running jobs queued under earlier
//! names after a rename. Names and aliases can't be empty or repeat one another
//! (checked here; the worker also rejects two jobs sharing one).
//!
//! `#[job(queue = "mailers")]` sets the queue it is enqueued on (default:
//! `"default"`), and
//! `#[job(retries = 10, backoff = "exponential")]` how often and how late it is
//! retried (default: the worker's settings; backoff is `"exponential"`,
//! `"polynomial"` or `"fixed:30s"`). If the job's error type implements
//! `butler::Retryable`, each error decides whether and when to retry; for
//! errors wrapped in a `BoxError` or `anyhow::Error`, register the inner type
//! with `butler::retryable!`.
//!
//! `#[job(concurrency_key = "account_id", limit = 2)]` runs at most two jobs
//! with the same `account_id` at once, across every worker (`limit` defaults
//! to 1; the key may name several arguments, separated by commas), and
//! `#[job(unique = "until_started")]` (or `"until_finished"`) makes
//! enqueueing an identical job, same arguments, return the one already
//! waiting. Argument names are checked here, at compile time.
//! - `send_email::prepare(...)`, which builds the job without enqueueing it,
//!   for `butler::enqueue_all`, `.on_queue(..)`, or `.run_in(..)` to schedule it.
//! - `send_email::JOB`, a handle for `Worker::register`. Jobs defined in another
//!   crate need it: the linker drops that crate's automatic registration unless
//!   the binary references something from it.
//!
//! A parameter of type `butler::Progress<S>` makes the job resumable: the
//! worker provides it from saved progress, and callers don't pass it. Such a
//! job may set `#[job(max_resumptions = N)]`: after `N` worker shutdowns
//! interrupted it at a checkpoint, the next interruption counts as a failed
//! attempt (default: the worker's setting, unlimited unless configured).
//!
//! The function may be `async` (runs as a task on the worker's runtime) or a
//! plain `fn` (runs on the blocking thread pool, for CPU-bound work). Job
//! bodies must be `Send`, so a tokio worker can spawn them.

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    FnArg, ItemFn, LitInt, LitStr, Pat, ReturnType, Type, parse_macro_input, parse_quote,
    spanned::Spanned,
};

/// What `#[job(...)]` sets.
#[derive(Default)]
struct Attrs {
    name: Option<LitStr>,
    /// Earlier names that still run this job.
    aliases: Vec<LitStr>,
    queue: Option<LitStr>,
    retries: Option<LitInt>,
    backoff: Option<proc_macro2::TokenStream>,
    max_resumptions: Option<LitInt>,
    /// The argument names the concurrency key is made of.
    concurrency_key: Option<LitStr>,
    limit: Option<LitInt>,
    unique: Option<proc_macro2::TokenStream>,
}

#[proc_macro_attribute]
pub fn job(attr: TokenStream, item: TokenStream) -> TokenStream {
    let func = parse_macro_input!(item as ItemFn);

    let mut attrs = Attrs::default();
    let parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("name") {
            let value: LitStr = meta.value()?.parse()?;
            if value.value().is_empty() {
                return Err(syn::Error::new(value.span(), "job names can't be empty"));
            }
            attrs.name = Some(value);
            Ok(())
        } else if meta.path.is_ident("aliases") {
            let list: syn::ExprArray = meta.value()?.parse()?;
            for element in list.elems {
                match element {
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(alias),
                        ..
                    }) => attrs.aliases.push(alias),
                    other => {
                        return Err(syn::Error::new(
                            other.span(),
                            "aliases are string literals, like aliases = [\"old_name\"]",
                        ));
                    }
                }
            }
            Ok(())
        } else if meta.path.is_ident("retries") {
            let value: LitInt = meta.value()?.parse()?;
            value.base10_parse::<u32>()?;
            attrs.retries = Some(value);
            Ok(())
        } else if meta.path.is_ident("max_resumptions") {
            let value: LitInt = meta.value()?.parse()?;
            value.base10_parse::<u32>()?;
            attrs.max_resumptions = Some(value);
            Ok(())
        } else if meta.path.is_ident("backoff") {
            let value: LitStr = meta.value()?.parse()?;
            attrs.backoff = Some(backoff(&value.value()).ok_or_else(|| {
                syn::Error::new(
                    value.span(),
                    "backoff is \"exponential\", \"polynomial\", or \"fixed:<n><unit>\" \
                     with a unit of ms, s, m, h or d, like \"fixed:30s\"",
                )
            })?);
            Ok(())
        } else if meta.path.is_ident("concurrency_key") {
            attrs.concurrency_key = Some(meta.value()?.parse()?);
            Ok(())
        } else if meta.path.is_ident("limit") {
            let value: LitInt = meta.value()?.parse()?;
            if value.base10_parse::<u32>()? == 0 {
                return Err(syn::Error::new(
                    value.span(),
                    "a concurrency limit starts at 1",
                ));
            }
            attrs.limit = Some(value);
            Ok(())
        } else if meta.path.is_ident("unique") {
            let value: LitStr = meta.value()?.parse()?;
            attrs.unique = Some(unique(&value.value()).ok_or_else(|| {
                syn::Error::new(
                    value.span(),
                    "unique is \"until_started\" or \"until_finished\"",
                )
            })?);
            Ok(())
        } else if meta.path.is_ident("queue") {
            let value: LitStr = meta.value()?.parse()?;
            if !is_valid_queue_name(&value.value()) {
                return Err(syn::Error::new(
                    value.span(),
                    "queue names are 1 to 64 of A-Z a-z 0-9 _ - . (not starting with a dot)",
                ));
            }
            attrs.queue = Some(value);
            Ok(())
        } else {
            Err(meta.error(
                "unsupported job attribute, expected `name`, `aliases`, `queue`, `retries`, \
                 `backoff`, `max_resumptions`, `concurrency_key`, `limit` or `unique`",
            ))
        }
    });
    parse_macro_input!(attr with parser);

    match expand(func, attrs) {
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

/// Same rule as `butler::JobDef::check_names`, checked at compile time: no
/// alias is empty, repeats the job's name, or repeats another alias.
fn check_aliases(name: &str, aliases: &[LitStr]) -> syn::Result<()> {
    for (i, alias) in aliases.iter().enumerate() {
        let value = alias.value();
        let reason = if value.is_empty() {
            "job names and aliases can't be empty"
        } else if value == name {
            "an alias repeats the job's name"
        } else if aliases[..i].iter().any(|earlier| earlier.value() == value) {
            "an alias is listed twice"
        } else {
            continue;
        };
        return Err(syn::Error::new(alias.span(), reason));
    }
    Ok(())
}

/// Same rule as `butler::is_valid_queue_name`, checked at compile time.
fn is_valid_queue_name(name: &str) -> bool {
    (1..=64).contains(&name.len())
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
}

/// Same names as `butler::Unique::parse`, checked at compile time, as the
/// `butler::Unique` to put in the job's `JobDef`.
fn unique(value: &str) -> Option<proc_macro2::TokenStream> {
    match value {
        "until_started" => Some(quote!(::butler::Unique::UntilStarted)),
        "until_finished" => Some(quote!(::butler::Unique::UntilFinished)),
        _ => None,
    }
}

/// The positions, among the arguments callers pass, of the names in `key`.
fn key_positions(
    key: &LitStr,
    args: &[syn::Ident],
    progress: Option<&syn::Ident>,
) -> syn::Result<Vec<usize>> {
    let mut positions = Vec::new();
    for name in key.value().split(',').map(str::trim) {
        if name.is_empty() {
            return Err(syn::Error::new(
                key.span(),
                "concurrency_key names the job's arguments, separated by commas",
            ));
        }
        if progress.is_some_and(|progress| progress == name) {
            return Err(syn::Error::new(
                key.span(),
                format!("`{name}` is the job's Progress, not an argument callers pass"),
            ));
        }
        let Some(position) = args.iter().position(|arg| arg == name) else {
            let names: Vec<String> = args.iter().map(ToString::to_string).collect();
            return Err(syn::Error::new(
                key.span(),
                format!(
                    "`{name}` is not an argument of this job; its arguments are: {}",
                    if names.is_empty() {
                        "none".to_owned()
                    } else {
                        names.join(", ")
                    }
                ),
            ));
        };
        if positions.contains(&position) {
            return Err(syn::Error::new(
                key.span(),
                format!("`{name}` is named twice in concurrency_key"),
            ));
        }
        positions.push(position);
    }
    Ok(positions)
}

/// Same forms as `butler::Backoff`'s `FromStr`, checked at compile time, as
/// the `butler::Backoff` expression to put in the job's `JobDef`.
fn backoff(value: &str) -> Option<proc_macro2::TokenStream> {
    match value {
        "exponential" => return Some(quote!(::butler::Backoff::Exponential)),
        "polynomial" => return Some(quote!(::butler::Backoff::Polynomial)),
        _ => {}
    }
    let delay = value.strip_prefix("fixed:")?;
    let split = delay.find(|c: char| !c.is_ascii_digit())?;
    let (number, unit) = delay.split_at(split);
    let scale: u64 = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => return None,
    };
    let ms = number.parse::<u64>().ok()?.checked_mul(scale)?;
    Some(quote!(::butler::Backoff::Fixed(::core::time::Duration::from_millis(#ms))))
}

fn expand(func: ItemFn, attrs: Attrs) -> syn::Result<proc_macro2::TokenStream> {
    let Attrs {
        name: job_name,
        aliases,
        queue,
        retries,
        backoff,
        max_resumptions,
        concurrency_key,
        limit,
        unique,
    } = attrs;
    if let (None, Some(limit)) = (&concurrency_key, &limit) {
        return Err(syn::Error::new(
            limit.span(),
            "limit caps jobs per concurrency_key: add concurrency_key = \"<argument>\"",
        ));
    }
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
    if let (None, Some(max)) = (&progress, &max_resumptions) {
        return Err(syn::Error::new(
            max.span(),
            "max_resumptions limits how often a job resumes from its checkpoints: \
             add a `Progress` parameter",
        ));
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
    check_aliases(&job_name.value(), &aliases)?;
    let queue = match queue {
        Some(queue) => quote!(#queue),
        None => quote!(::butler::DEFAULT_QUEUE),
    };
    let retries = match retries {
        Some(retries) => quote!(::core::option::Option::Some(#retries)),
        None => quote!(::core::option::Option::None),
    };
    let backoff = match backoff {
        Some(backoff) => quote!(::core::option::Option::Some(#backoff)),
        None => quote!(::core::option::Option::None),
    };
    let max_resumptions = match max_resumptions {
        Some(max) => quote!(::core::option::Option::Some(#max)),
        None => quote!(::core::option::Option::None),
    };
    let concurrency = match &concurrency_key {
        Some(key) => {
            let positions = key_positions(key, &idents, progress.as_ref().map(|(ident, _)| ident))?;
            let limit = match &limit {
                Some(limit) => quote!(#limit),
                None => quote!(1),
            };
            quote!(::core::option::Option::Some(::butler::ConcurrencyLimit {
                args: &[#(#positions),*],
                limit: #limit,
            }))
        }
        None => quote!(::core::option::Option::None),
    };
    let unique = match unique {
        Some(unique) => quote!(::core::option::Option::Some(#unique)),
        None => quote!(::core::option::Option::None),
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
            -> ::butler::JobCall<<#returns as ::butler::IntoJobResult>::Output>
        {
            #(let #idents: #types = ::butler::JobArg::into_arg(#idents);)*
            ::butler::__private::call(&#name::JOB, [
                #(::butler::__private::serde_json::to_value(&#idents)),*
            ])
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
                // The error's own `Retryable` classification, if its type has one.
                #[allow(unused_imports)]
                use ::butler::__private::{ClassifyRetry as _, DefaultRetry as _};
                ::butler::__private::output(
                    #run,
                    |error: &<#returns as ::butler::IntoJobResult>::Error| {
                        (&::butler::__private::Classify(error)).retry_policy()
                    },
                )
            })
        }

        #[doc(hidden)]
        #vis mod #name {
            // The job's argument and output types are named as in the parent.
            #[allow(unused_imports)]
            use super::*;

            /// Pass to `Worker::register` when the job lives in another crate.
            pub const JOB: ::butler::JobDef =
                ::butler::JobDef {
                    name: #job_name,
                    aliases: &[#(#aliases),*],
                    queue: #queue,
                    retries: #retries,
                    backoff: #backoff,
                    max_resumptions: #max_resumptions,
                    concurrency: #concurrency,
                    unique: #unique,
                    perform: super::#dispatch,
                };

            /// Builds this job without enqueueing it, for `butler::enqueue_all`
            /// (or `.on_queue(..)` or `.run_in(..)`, then `.enqueue()`).
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
