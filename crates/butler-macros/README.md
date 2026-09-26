# butler-macros

The `#[job]` attribute macro for [butler](https://crates.io/crates/butler).
Don't depend on this crate directly: use `butler` and write `#[butler::job]`.
The macro expands to calls into `butler`'s internals, so the two crates are
always released with the same version.
