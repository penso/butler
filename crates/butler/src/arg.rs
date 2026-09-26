use std::{
    borrow::Cow,
    path::{Path, PathBuf},
};

/// A value that can be passed where a `#[job]` function takes a `T`.
///
/// The enqueue function that `#[job]` generates takes `impl JobArg<T>` for each
/// argument of type `T`, so callers can pass borrowed or literal values:
///
/// ```ignore
/// #[butler::job]
/// async fn send_email(to: String, subject: String, retries: u32) { /* ... */ }
///
/// send_email("ada@example.com", "Welcome", 3).await?;
/// ```
///
/// Every `T` accepts itself, and `&T` for any `T: Clone`. On top of that:
///
/// | Parameter type | Also accepts |
/// |---|---|
/// | `String` | `&str`, `Cow<str>`, `Box<str>` |
/// | `PathBuf` | `&Path`, `&str` |
/// | `Vec<T>` | `&[T]` where `T: Clone` |
///
/// This is deliberately narrower than `Into<T>`. With `impl Into<u32>`, a bare
/// `3` fails to compile (the literal falls back to `i32`, and `u32` has no
/// `From<i32>`). Here only `u32` itself matches, so literals infer as expected.
pub trait JobArg<T> {
    fn into_arg(self) -> T;
}

impl<T> JobArg<T> for T {
    fn into_arg(self) -> T {
        self
    }
}

impl<T: Clone> JobArg<T> for &T {
    fn into_arg(self) -> T {
        self.clone()
    }
}

impl JobArg<String> for &str {
    fn into_arg(self) -> String {
        self.to_owned()
    }
}

impl JobArg<String> for Cow<'_, str> {
    fn into_arg(self) -> String {
        self.into_owned()
    }
}

impl JobArg<String> for Box<str> {
    fn into_arg(self) -> String {
        self.into_string()
    }
}

impl JobArg<PathBuf> for &Path {
    fn into_arg(self) -> PathBuf {
        self.to_path_buf()
    }
}

impl JobArg<PathBuf> for &str {
    fn into_arg(self) -> PathBuf {
        PathBuf::from(self)
    }
}

impl<T: Clone> JobArg<Vec<T>> for &[T] {
    fn into_arg(self) -> Vec<T> {
        self.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn arg<T>(value: impl JobArg<T>) -> T {
        value.into_arg()
    }

    #[test]
    fn strings_accept_borrowed_forms() {
        let owned = String::from("owned");
        assert_eq!(arg::<String>("lit"), "lit");
        assert_eq!(arg::<String>(&owned), "owned");
        assert_eq!(arg::<String>(Cow::Borrowed("cow")), "cow");
        assert_eq!(arg::<String>(Box::<str>::from("boxed")), "boxed");
        assert_eq!(arg::<String>(owned), "owned");
    }

    #[test]
    fn literals_infer_the_parameter_type() {
        assert_eq!(arg::<u32>(3), 3);
        assert_eq!(arg::<i64>(-3), -3);
        assert_eq!(arg::<f64>(1.5), 1.5);
        assert_eq!(arg::<Option<String>>(None), None);
    }

    #[test]
    fn paths_and_slices() {
        assert_eq!(arg::<PathBuf>("a/b"), PathBuf::from("a/b"));
        assert_eq!(arg::<PathBuf>(Path::new("c")), PathBuf::from("c"));
        assert_eq!(arg::<Vec<u8>>(&[1, 2][..]), vec![1, 2]);
    }
}
