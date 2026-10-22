use axum::{
    body::{to_bytes, Body, Bytes},
    extract::State,
    http::{HeaderMap, Request, StatusCode},
    routing::post,
    Json, Router,
};
use lenso_plugin_core::{App, BoxError, Instance, Plugin, Setup};
use lenso_web_axum::{json_input, HttpError, Limits, Validate, WebPlugin};
use serde::Deserialize;
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;

#[derive(Default)]
struct Notes(Mutex<Vec<String>>);
impl Plugin for Notes {
    type Service = Self;
    async fn setup(self, _: &mut Setup) -> Result<Arc<Self>, BoxError> {
        Ok(Arc::new(self))
    }
}
#[derive(Deserialize)]
struct NewNote {
    text: String,
}
impl Validate for NewNote {
    fn validate(&self) -> Result<(), HttpError> {
        if self.text.trim().is_empty() || self.text.len() > 100 {
            Err(HttpError::Invalid)
        } else {
            Ok(())
        }
    }
}
async fn create(
    State(notes): State<Arc<Notes>>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>, HttpError> {
    // Explicit test-only host identity fixture. Production hosts verify identity
    // once in middleware and pass a typed Extension; services still authorize rows.
    if headers.get("authorization").and_then(|v| v.to_str().ok())
        != Some("Bearer TEST_ONLY_NOT_A_REAL_CREDENTIAL")
    {
        return Err(HttpError::Unauthorized);
    }
    let input: NewNote = json_input(&body)?;
    let mut entries = notes.0.lock().await;
    entries.push(input.text);
    Ok(Json(
        serde_json::json!({"id": entries.len(), "text": entries.last()}),
    ))
}
#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let notes = Instance::new("notes", Notes::default());
    let web = Instance::new(
        "http",
        WebPlugin {
            service: notes.handle(),
            routes: |notes| {
                Router::new()
                    .route("/notes", post(create))
                    .with_state(notes)
            },
            limits: Limits::default(),
            listen: None,
        },
    );
    let handle = web.handle();
    let app = App::new().with(notes).with(web).start().await?;
    let result = async {
        let response = app
            .get(&handle)?
            .router
            .clone()
            .oneshot(
                Request::post("/notes")
                    .header("authorization", "Bearer TEST_ONLY_NOT_A_REAL_CREDENTIAL")
                    .body(Body::from(r#"{"text":"First real note"}"#))?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        println!(
            "{}",
            String::from_utf8(to_bytes(response.into_body(), 4096).await?.to_vec())?
        );
        Ok::<_, BoxError>(())
    }
    .await;
    let shutdown = app.shutdown().await;
    result?;
    shutdown?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn business_validation_auth_and_missing_route() {
        let notes = Instance::new("notes", Notes::default());
        let web = Instance::new(
            "http",
            WebPlugin {
                service: notes.handle(),
                routes: |notes| {
                    Router::new()
                        .route("/notes", post(create))
                        .with_state(notes)
                },
                limits: Limits {
                    body_bytes: 128,
                    ..Limits::default()
                },
                listen: None,
            },
        );
        let handle = web.handle();
        let app = App::new().with(notes).with(web).start().await.unwrap();
        let router = app.get(&handle).unwrap().router.clone();
        for (path, token, body, expected) in [
            ("/notes", "", "{}".to_owned(), StatusCode::UNAUTHORIZED),
            (
                "/notes",
                "Bearer TEST_ONLY_NOT_A_REAL_CREDENTIAL",
                r#"{"text":""}"#.to_owned(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "/notes",
                "Bearer TEST_ONLY_NOT_A_REAL_CREDENTIAL",
                "not json".to_owned(),
                StatusCode::BAD_REQUEST,
            ),
            (
                "/notes",
                "Bearer TEST_ONLY_NOT_A_REAL_CREDENTIAL",
                "x".repeat(129),
                StatusCode::PAYLOAD_TOO_LARGE,
            ),
            ("/missing", "", "{}".to_owned(), StatusCode::NOT_FOUND),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::post(path)
                        .header("authorization", token)
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), expected);
            drop(response);
        }
        app.shutdown().await.unwrap();
    }
}
