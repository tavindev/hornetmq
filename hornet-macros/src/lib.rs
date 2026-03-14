extern crate proc_macro;

use proc_macro::TokenStream;
use quote::{format_ident, quote};
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, ItemFn, ReturnType, Type,
};

struct WorkerOpts {
    queue: String,
    concurrency: u32,
    backoff: Option<BackoffConfig>,
    lock_duration: u64,
    limiter: Option<LimiterConfig>,
}

struct LimiterConfig {
    max: u32,
    duration: u64,
}

fn parse_limiter(s: &str, span: proc_macro2::Span) -> syn::Result<LimiterConfig> {
    let parts: Vec<&str> = s.split(',').collect();
    if parts.len() != 2 {
        return Err(syn::Error::new(
            span,
            "limiter requires two arguments: max, duration (e.g. \"10, 1000\")",
        ));
    }
    let max: u32 = parts[0].trim().parse().map_err(|_| {
        syn::Error::new(span, format!("invalid limiter max: {}", parts[0].trim()))
    })?;
    let duration: u64 = parts[1].trim().parse().map_err(|_| {
        syn::Error::new(span, format!("invalid limiter duration: {}", parts[1].trim()))
    })?;
    Ok(LimiterConfig { max, duration })
}

enum BackoffConfig {
    Fixed(u64),
    Exponential { base: u64, max: u64 },
}

fn parse_backoff(s: &str, span: proc_macro2::Span) -> syn::Result<BackoffConfig> {
    let s = s.trim();
    if let Some(inner) = s.strip_prefix("fixed(").and_then(|s| s.strip_suffix(')')) {
        let ms: u64 = inner.trim().parse().map_err(|_| {
            syn::Error::new(span, format!("invalid fixed backoff value: {inner}"))
        })?;
        Ok(BackoffConfig::Fixed(ms))
    } else if let Some(inner) = s.strip_prefix("exponential(").and_then(|s| s.strip_suffix(')')) {
        let parts: Vec<&str> = inner.split(',').collect();
        if parts.len() != 2 {
            return Err(syn::Error::new(
                span,
                "exponential backoff requires two arguments: base, max",
            ));
        }
        let base: u64 = parts[0].trim().parse().map_err(|_| {
            syn::Error::new(span, format!("invalid exponential base: {}", parts[0].trim()))
        })?;
        let max: u64 = parts[1].trim().parse().map_err(|_| {
            syn::Error::new(span, format!("invalid exponential max: {}", parts[1].trim()))
        })?;
        Ok(BackoffConfig::Exponential { base, max })
    } else {
        Err(syn::Error::new(
            span,
            "backoff must be \"fixed(<ms>)\" or \"exponential(<base>, <max>)\"",
        ))
    }
}

struct WorkerOptsBuilder {
    queue: Option<String>,
    concurrency: Option<u32>,
    backoff: Option<BackoffConfig>,
    lock_duration: Option<u64>,
    limiter: Option<LimiterConfig>,
    queue_span: Option<proc_macro2::Span>,
}

impl WorkerOptsBuilder {
    fn new() -> Self {
        WorkerOptsBuilder {
            queue: None,
            concurrency: None,
            backoff: None,
            lock_duration: None,
            limiter: None,
            queue_span: None,
        }
    }

    fn build(self) -> syn::Result<WorkerOpts> {
        let queue = self.queue.ok_or_else(|| {
            syn::Error::new(
                self.queue_span.unwrap_or_else(proc_macro2::Span::call_site),
                "missing required option: queue",
            )
        })?;
        Ok(WorkerOpts {
            queue,
            concurrency: self.concurrency.unwrap_or(1),
            backoff: self.backoff,
            lock_duration: self.lock_duration.unwrap_or(30_000),
            limiter: self.limiter,
        })
    }
}

impl Parse for WorkerOpts {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut opts = WorkerOptsBuilder::new();
        opts.queue_span = Some(input.span());

        while !input.is_empty() {
            let ident: syn::Ident = input.parse()?;
            input.parse::<syn::Token![=]>()?;

            match ident.to_string().as_str() {
                "queue" => {
                    let val: syn::LitStr = input.parse()?;
                    opts.queue = Some(val.value());
                }
                "concurrency" => {
                    let val: syn::LitInt = input.parse()?;
                    opts.concurrency = Some(val.base10_parse()?);
                }
                "backoff" => {
                    let val: syn::LitStr = input.parse()?;
                    opts.backoff = Some(parse_backoff(&val.value(), val.span())?);
                }
                "lock_duration" => {
                    let val: syn::LitInt = input.parse()?;
                    opts.lock_duration = Some(val.base10_parse()?);
                }
                "limiter" => {
                    let val: syn::LitStr = input.parse()?;
                    opts.limiter = Some(parse_limiter(&val.value(), val.span())?);
                }
                _ => {
                    return Err(syn::Error::new(
                        ident.span(),
                        format!("unexpected option: {ident}"),
                    ))
                }
            }

            if !input.is_empty() {
                input.parse::<syn::Token![,]>()?;
            }
        }

        opts.build()
    }
}

/// Extract the inner type `T` from `&Job<T>` in the first function argument.
fn extract_job_data_type(func: &ItemFn) -> syn::Result<&Type> {
    let first_arg = func.sig.inputs.first().ok_or_else(|| {
        syn::Error::new_spanned(&func.sig, "worker function must take &Job<Data> as first argument")
    })?;

    let pat_type = match first_arg {
        syn::FnArg::Typed(pt) => pt,
        _ => {
            return Err(syn::Error::new_spanned(
                first_arg,
                "expected typed argument, not self",
            ))
        }
    };

    // Expect &Job<T>
    let reference = match pat_type.ty.as_ref() {
        Type::Reference(r) => r,
        _ => {
            return Err(syn::Error::new_spanned(
                &pat_type.ty,
                "expected &Job<Data>",
            ))
        }
    };

    let path = match reference.elem.as_ref() {
        Type::Path(tp) => tp,
        _ => {
            return Err(syn::Error::new_spanned(
                &reference.elem,
                "expected Job<Data>",
            ))
        }
    };

    let last_segment = path.path.segments.last().ok_or_else(|| {
        syn::Error::new_spanned(&path.path, "expected Job<Data>")
    })?;

    let args = match &last_segment.arguments {
        syn::PathArguments::AngleBracketed(args) => args,
        _ => {
            return Err(syn::Error::new_spanned(
                last_segment,
                "expected Job<Data> with type parameter",
            ))
        }
    };

    let first_generic = args.args.first().ok_or_else(|| {
        syn::Error::new_spanned(&args.args, "Job requires a type parameter")
    })?;

    match first_generic {
        syn::GenericArgument::Type(ty) => Ok(ty),
        _ => Err(syn::Error::new_spanned(
            first_generic,
            "expected type argument",
        )),
    }
}

/// Extract the inner type `T` from `Result<T>` in the return type.
fn extract_return_type(func: &ItemFn) -> syn::Result<&Type> {
    let return_type = match &func.sig.output {
        ReturnType::Type(_, ty) => ty.as_ref(),
        ReturnType::Default => {
            return Err(syn::Error::new_spanned(
                &func.sig,
                "worker function must return Result<T>",
            ))
        }
    };

    let path = match return_type {
        Type::Path(tp) => tp,
        _ => {
            return Err(syn::Error::new_spanned(
                return_type,
                "expected Result<T>",
            ))
        }
    };

    let last_segment = path.path.segments.last().ok_or_else(|| {
        syn::Error::new_spanned(&path.path, "expected Result<T>")
    })?;

    let args = match &last_segment.arguments {
        syn::PathArguments::AngleBracketed(args) => args,
        _ => {
            return Err(syn::Error::new_spanned(
                last_segment,
                "expected Result<T> with type parameter",
            ))
        }
    };

    let first_generic = args.args.first().ok_or_else(|| {
        syn::Error::new_spanned(&args.args, "Result requires a type parameter")
    })?;

    match first_generic {
        syn::GenericArgument::Type(ty) => Ok(ty),
        _ => Err(syn::Error::new_spanned(
            first_generic,
            "expected type argument",
        )),
    }
}

fn to_pascal_case(s: &str) -> String {
    s.split('_')
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => c.to_uppercase().to_string() + &chars.as_str().to_lowercase(),
            }
        })
        .collect()
}

#[proc_macro_attribute]
pub fn worker(args: TokenStream, input: TokenStream) -> TokenStream {
    let opts = parse_macro_input!(args as WorkerOpts);
    let func = parse_macro_input!(input as ItemFn);

    let result = generate_worker(&opts, &func);
    match result {
        Ok(tokens) => tokens.into(),
        Err(err) => err.to_compile_error().into(),
    }
}

fn generate_worker(
    opts: &WorkerOpts,
    func: &ItemFn,
) -> syn::Result<proc_macro2::TokenStream> {
    let data_type = extract_job_data_type(func)?;
    let return_type = extract_return_type(func)?;

    let fn_name = &func.sig.ident;
    let struct_name = format_ident!("{}Worker", to_pascal_case(&fn_name.to_string()));

    let queue_name = &opts.queue;
    let concurrency = opts.concurrency as usize;
    let lock_duration = opts.lock_duration;

    // Build the worker constructor chain
    let backoff_chain = match &opts.backoff {
        Some(BackoffConfig::Fixed(ms)) => {
            quote! { .with_backoff(hornetmq::BackoffStrategy::Fixed(#ms)) }
        }
        Some(BackoffConfig::Exponential { base, max }) => {
            quote! { .with_backoff(hornetmq::BackoffStrategy::Exponential { base: #base, max: #max }) }
        }
        None => quote! {},
    };

    let limiter_chain = match &opts.limiter {
        Some(LimiterConfig { max, duration }) => {
            quote! { .with_limiter(#max, #duration) }
        }
        None => quote! {},
    };

    Ok(quote! {
        #func

        pub struct #struct_name {
            inner: hornetmq::Worker<#data_type, #return_type>,
        }

        impl #struct_name {
            pub fn new(redis_url: &str) -> anyhow::Result<Self> {
                let worker = hornetmq::Worker::new(
                    #queue_name,
                    redis_url,
                    #concurrency,
                    #fn_name,
                )?
                .with_lock_duration(#lock_duration)
                #backoff_chain
                #limiter_chain;

                Ok(Self { inner: worker })
            }

            pub async fn run(&mut self) -> anyhow::Result<()> {
                self.inner.run().await
            }
        }
    })
}
