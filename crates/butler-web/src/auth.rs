//! Optional HTTP basic auth, for running the dashboard without a proxy in
//! front. Every route answers 401 without the right credentials, including the
//! assets and the live stream: browsers send the credentials they were asked
//! for with same-origin `EventSource` and `fetch` requests too.

use std::sync::Arc;

use axum::{
    extract::{Request, State},
    http::{HeaderValue, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use base64::{Engine, engine::general_purpose::STANDARD};
use subtle::{Choice, ConstantTimeEq};

/// The one username and password the dashboard accepts.
#[derive(Clone)]
pub(crate) struct BasicAuth {
    /// `base64(user:password)`, as a browser sends it after `Basic `.
    token: Vec<u8>,
}

impl BasicAuth {
    pub fn new(user: &str, password: &str) -> Self {
        Self {
            token: STANDARD.encode(format!("{user}:{password}")).into_bytes(),
        }
    }

    /// Whether `authorization` carries the expected credentials. The time it
    /// takes depends on the lengths only, never on how much of it matched.
    fn allows(&self, authorization: Option<&HeaderValue>) -> bool {
        let Some((scheme, token)) = authorization
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.trim().split_once(' '))
        else {
            return false;
        };
        if !scheme.eq_ignore_ascii_case("basic") {
            return false;
        }
        let given = token.trim_start().as_bytes();
        // Walks the whole expected token whatever was sent, so a wrong length
        // costs as long as a wrong byte.
        let mut equal: Choice = self.token.len().ct_eq(&given.len());
        for (i, expected) in self.token.iter().enumerate() {
            equal &= expected.ct_eq(given.get(i).unwrap_or(&0));
        }
        equal.into()
    }
}

pub(crate) async fn require(
    State(auth): State<Arc<BasicAuth>>,
    request: Request,
    next: Next,
) -> Response {
    if auth.allows(request.headers().get(header::AUTHORIZATION)) {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        [(
            header::WWW_AUTHENTICATE,
            r#"Basic realm="butler", charset="UTF-8""#,
        )],
        "authentication required",
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used)]

    use super::*;

    fn header(value: &str) -> HeaderValue {
        HeaderValue::from_str(value).unwrap()
    }

    #[test]
    fn accepts_only_the_exact_credentials() {
        let auth = BasicAuth::new("admin", "s3cret");
        // base64("admin:s3cret")
        assert!(auth.allows(Some(&header("Basic YWRtaW46czNjcmV0"))));
        assert!(
            auth.allows(Some(&header("basic YWRtaW46czNjcmV0"))),
            "scheme is case-insensitive"
        );
        assert!(!auth.allows(None));
        assert!(
            !auth.allows(Some(&header("Basic YWRtaW46czNjcmV1"))),
            "last byte differs"
        );
        assert!(
            !auth.allows(Some(&header("Basic YWRtaW46czNjcmV0AA=="))),
            "longer"
        );
        assert!(!auth.allows(Some(&header("Basic YWRtaW46"))), "shorter");
        assert!(!auth.allows(Some(&header("Basic "))));
        assert!(!auth.allows(Some(&header("Bearer YWRtaW46czNjcmV0"))));
        assert!(!auth.allows(Some(&header("YWRtaW46czNjcmV0"))), "no scheme");
    }

    #[test]
    fn passwords_may_hold_colons_and_non_ascii() {
        let auth = BasicAuth::new("ops", "a:b é");
        let token = STANDARD.encode("ops:a:b é");
        assert!(auth.allows(Some(&header(&format!("Basic {token}")))));
    }
}
