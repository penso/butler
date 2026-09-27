#![allow(clippy::unwrap_used, clippy::expect_used)]

//! The dashboard against a real (in-memory) queue: pages render real data,
//! actions change jobs, cross-site posts are refused, and the live stream and
//! JSON endpoints answer.

use std::time::{Duration, SystemTime};

use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode, header},
};
use butler::{JobState, MemoryQueue, Queue, monitor::JobMetric};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

/// A queue with one job in each interesting state.
struct Fixture {
    queue: Queue,
    app: Router,
    dead: String,
    pending: String,
    done: String,
    scheduled: String,
}

fn fixture(base: &str) -> Fixture {
    let queue: Queue = MemoryQueue::new().into();
    queue.heartbeat("w1", Duration::from_secs(60)).unwrap();
    let done = queue
        .push("send_email", "mailers", vec![json!("ada@example.com")])
        .unwrap();
    let dead = queue
        .push("charge_card", "default", vec![json!(42)])
        .unwrap();
    let pending = queue.push("resize", "default", vec![]).unwrap();
    let scheduled = queue
        .schedule(
            "send_reminder",
            "mailers",
            vec![json!("ada@example.com")],
            SystemTime::now() + Duration::from_secs(3600),
        )
        .unwrap();
    let job = queue
        .claim("w1", &["mailers"], Duration::ZERO)
        .unwrap()
        .unwrap();
    queue
        .complete("w1", job, json!({ "message_id": "m-1" }))
        .unwrap();
    let job = queue
        .claim("w1", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    queue
        .fail("w1", job, "card declined: insufficient funds".into(), 0)
        .unwrap();
    for (failed, ms) in [(false, 120), (true, 40)] {
        queue
            .record_metric(&JobMetric::now("charge_card", "default", failed, ms))
            .unwrap();
    }
    let app = butler_web::Dashboard::new(queue.clone())
        .base_path(base)
        .router();
    Fixture {
        queue,
        app,
        dead,
        pending,
        done,
        scheduled,
    }
}

async fn get(app: &Router, uri: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(Request::get(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn post(app: &Router, uri: &str, site: &str, form: &str) -> (StatusCode, Option<String>) {
    let response = app
        .clone()
        .oneshot(
            Request::post(uri)
                .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded")
                .header("sec-fetch-site", site)
                .body(Body::from(form.to_owned()))
                .unwrap(),
        )
        .await
        .unwrap();
    let location = response
        .headers()
        .get(header::LOCATION)
        .map(|value| value.to_str().unwrap().to_owned());
    (response.status(), location)
}

#[tokio::test]
async fn pages_show_real_data() {
    let f = fixture("");
    let (status, html) = get(&f.app, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Live throughput"));
    assert!(html.contains("charge_card"), "busiest jobs table");
    assert!(html.contains(r#"data-stat="dead">1<"#), "dead count");

    let (status, html) = get(&f.app, "/jobs?state=dead").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("card declined: insufficient funds"));
    assert!(html.contains(&format!("/jobs/{}/retry", f.dead)));

    let (status, html) = get(&f.app, &format!("/jobs/{}", f.done)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("message_id"), "the job's result");
    assert!(html.contains("ada@example.com"), "its arguments");

    let (status, html) = get(&f.app, "/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("w1"));

    let (status, _) = get(&f.app, "/jobs/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn html_from_jobs_is_escaped() {
    let f = fixture("");
    let id = f
        .queue
        .push(
            "<script>alert(1)</script>",
            "default",
            vec![json!("<img src=x>")],
        )
        .unwrap();
    let (_, html) = get(&f.app, &format!("/jobs/{id}")).await;
    assert!(!html.contains("<script>alert(1)</script>"));
    // Askama escapes with numeric entities.
    assert!(html.contains("&#60;script&#62;alert(1)"));
    assert!(!html.contains("<img src=x>"), "arguments are escaped too");
    assert!(!html.contains("Metadata"), "no metadata, no card");

    let mut job = butler::NewJob::new("tagged", "default", vec![]);
    job.meta.insert("tenant".into(), json!("<b>acme</b>"));
    let id = f.queue.push_job(job).unwrap();
    let (_, html) = get(&f.app, &format!("/jobs/{id}")).await;
    assert!(html.contains("Metadata"));
    assert!(html.contains("&#60;b&#62;acme&#60;/b&#62;"));
    assert!(!html.contains("<b>acme</b>"), "metadata is escaped too");
}

#[tokio::test]
async fn actions_change_jobs_and_redirect_back() {
    let f = fixture("");
    let back = "return_to=%2Fjobs%3Fstate%3Ddead";
    let (status, location) = post(
        &f.app,
        &format!("/jobs/{}/retry", f.dead),
        "same-origin",
        back,
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(location.as_deref(), Some("/jobs?state=dead"));
    assert_eq!(f.queue.state(&f.dead), Some(JobState::Pending));

    let (status, _) = post(
        &f.app,
        &format!("/jobs/{}/cancel", f.pending),
        "same-origin",
        "",
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(f.queue.state(&f.pending), Some(JobState::Cancelled));

    let (_, _) = post(
        &f.app,
        &format!("/jobs/{}/discard", f.done),
        "same-origin",
        "",
    )
    .await;
    assert_eq!(f.queue.state(&f.done), None);
}

#[tokio::test]
async fn bulk_retry_moves_every_dead_job() {
    let f = fixture("");
    let (status, _) = post(
        &f.app,
        "/jobs/retry-all",
        "same-origin",
        "state=dead&queue=",
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(f.queue.stats().unwrap().dead, 0);
    assert_eq!(f.queue.state(&f.dead), Some(JobState::Pending));
}

#[tokio::test]
async fn cross_site_posts_and_open_redirects_are_refused() {
    let f = fixture("");
    let (status, _) = post(&f.app, &format!("/jobs/{}/retry", f.dead), "cross-site", "").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(f.queue.state(&f.dead), Some(JobState::Dead), "unchanged");

    let (_, location) = post(
        &f.app,
        &format!("/jobs/{}/retry", f.dead),
        "same-origin",
        "return_to=https%3A%2F%2Fevil.example%2F",
    )
    .await;
    assert_eq!(location.as_deref(), Some("/"), "stays on the dashboard");
}

#[tokio::test]
async fn a_base_path_prefixes_every_link() {
    let f = fixture("/admin/jobs");
    let app = Router::new().nest("/admin/jobs", f.app.clone());
    let (status, html) = get(&app, "/admin/jobs/jobs?state=dead").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains(r#"href="/admin/jobs/assets/app.css""#));
    assert!(html.contains(&format!(r#"href="/admin/jobs/jobs/{}""#, f.dead)));
    let (_, location) = post(
        &app,
        &format!("/admin/jobs/jobs/{}/retry", f.dead),
        "same-origin",
        "",
    )
    .await;
    assert_eq!(location.as_deref(), Some("/admin/jobs/"));
}

#[tokio::test]
async fn json_endpoints_assets_and_the_live_stream() {
    let f = fixture("");
    let (status, body) = get(&f.app, "/api/stats").await;
    assert_eq!(status, StatusCode::OK);
    let stats: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        (stats["dead"].as_u64(), stats["processed_total"].as_u64()),
        (Some(1), Some(2))
    );

    let (_, body) = get(&f.app, "/api/metrics?minutes=5").await;
    let series: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(series["minutes"].as_array().unwrap().len(), 5);
    assert_eq!(
        series["processed"].as_array().unwrap().last(),
        Some(&json!(2))
    );
    assert_eq!(series["failed"].as_array().unwrap().last(), Some(&json!(1)));
    assert_eq!(
        series["max_ms"].as_array().unwrap().last(),
        Some(&json!(120))
    );

    for asset in ["app.css", "app.js", "uplot.min.js", "uplot.min.css"] {
        let (status, body) = get(&f.app, &format!("/assets/{asset}")).await;
        assert_eq!(status, StatusCode::OK, "{asset}");
        assert!(!body.is_empty(), "{asset}");
    }

    // A job running on mailers, which has none pending.
    f.queue
        .push("send_email", "mailers", vec![json!("bob@example.com")])
        .unwrap();
    f.queue
        .claim("w1", &["mailers"], Duration::ZERO)
        .unwrap()
        .unwrap();
    let (_, html) = get(&f.app, "/").await;
    assert!(
        html.contains(r#"data-queue-running="mailers">1<"#),
        "{html}"
    );
    assert!(html.contains(r#"data-queue-running="default">0<"#));

    let response = f
        .app
        .clone()
        .oneshot(Request::get("/events").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(
        response.headers().get(header::CONTENT_TYPE).unwrap(),
        "text/event-stream"
    );
    // The first snapshot with real data arrives within a couple of seconds.
    let mut body = response.into_body();
    let snapshot = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = body.frame().await.unwrap().unwrap();
            let Ok(text) = frame.into_data() else {
                continue;
            };
            let text = String::from_utf8_lossy(&text).into_owned();
            if let Some(data) = text.lines().find_map(|line| line.strip_prefix("data: ")) {
                let snapshot: Value = serde_json::from_str(data).unwrap();
                if snapshot["ts_ms"].as_u64().unwrap_or(0) > 0 {
                    return snapshot;
                }
            }
        }
    })
    .await
    .expect("a live snapshot");
    assert_eq!(snapshot["dead"], json!(1));
    assert_eq!(
        snapshot["queues"],
        json!([["default", 1, 0], ["mailers", 0, 1]])
    );
}

#[tokio::test]
async fn scheduled_jobs_have_their_own_tab_and_actions() {
    let f = fixture("");
    let (status, html) = get(&f.app, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        html.contains(r#"data-stat="scheduled">1<"#),
        "scheduled count"
    );

    let (status, html) = get(&f.app, "/jobs?state=scheduled").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("send_reminder"));
    assert!(
        html.contains("in 59m") || html.contains("in 1h"),
        "when it runs"
    );
    assert!(html.contains(&format!("/jobs/{}/run-now", f.scheduled)));
    assert!(html.contains(&format!("/jobs/{}/cancel", f.scheduled)));

    let (status, html) = get(&f.app, &format!("/jobs/{}", f.scheduled)).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("badge-scheduled"));
    assert!(
        html.contains("data-until-ms="),
        "its run time, kept current"
    );
    assert!(html.contains(&format!("/jobs/{}/run-now", f.scheduled)));

    // Cross-site: refused, and the job stays scheduled.
    let run_now = format!("/jobs/{}/run-now", f.scheduled);
    let (status, _) = post(&f.app, &run_now, "cross-site", "").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(f.queue.state(&f.scheduled), Some(JobState::Scheduled));

    let back = "return_to=%2Fjobs%3Fstate%3Dscheduled";
    let (status, location) = post(&f.app, &run_now, "same-origin", back).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(location.as_deref(), Some("/jobs?state=scheduled"));
    assert_eq!(f.queue.state(&f.scheduled), Some(JobState::Pending));

    // Cancel works while scheduled.
    let later = f
        .queue
        .schedule(
            "send_reminder",
            "mailers",
            vec![],
            SystemTime::now() + Duration::from_secs(60),
        )
        .unwrap();
    let (status, _) = post(
        &f.app,
        &format!("/jobs/{later}/cancel"),
        "same-origin",
        back,
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(f.queue.state(&later), Some(JobState::Cancelled));
}

#[tokio::test]
async fn scheduled_actions_keep_the_base_path() {
    let f = fixture("/admin/jobs");
    let app = Router::new().nest("/admin/jobs", f.app.clone());
    let (status, html) = get(&app, "/admin/jobs/jobs?state=scheduled").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains(&format!(
        r#"action="/admin/jobs/jobs/{}/run-now""#,
        f.scheduled
    )));
    let (_, location) = post(
        &app,
        &format!("/admin/jobs/jobs/{}/run-now", f.scheduled),
        "same-origin",
        "return_to=%2Fadmin%2Fjobs%2Fjobs%3Fstate%3Dscheduled",
    )
    .await;
    assert_eq!(
        location.as_deref(),
        Some("/admin/jobs/jobs?state=scheduled")
    );
    assert_eq!(f.queue.state(&f.scheduled), Some(JobState::Pending));
}

#[tokio::test]
async fn a_retry_waiting_its_turn_shows_its_error_and_next_attempt() {
    let f = fixture("");
    // The fixture's pending job fails, with ten minutes before its retry.
    let job = f
        .queue
        .claim("w1", &["default"], Duration::ZERO)
        .unwrap()
        .unwrap();
    assert_eq!(job.id(), f.pending);
    let policy = butler::RetryPolicy::new(3, butler::Backoff::Fixed(Duration::from_secs(600)));
    f.queue
        .fail("w1", job, "503 from the CRM".into(), policy)
        .unwrap();
    assert_eq!(f.queue.state(&f.pending), Some(JobState::Scheduled));

    let (_, html) = get(&f.app, "/jobs?state=scheduled").await;
    assert!(html.contains("503 from the CRM"), "the error, in the list");
    let (_, html) = get(&f.app, &format!("/jobs/{}", f.pending)).await;
    assert!(html.contains("Next attempt"));
    assert!(
        html.contains("in 10m") || html.contains("in 11m"),
        "when it runs"
    );
    assert!(html.contains("503 from the CRM"));
}

#[tokio::test]
async fn a_job_page_shows_its_concurrency_and_unique_keys_escaped() {
    let f = fixture("");
    let mut job = butler::NewJob::new("sync", "default", vec![json!("<b>7</b>")]);
    job.concurrency = Some(butler::ConcurrencyKey {
        key: r#"sync:["<b>7</b>"]"#.into(),
        limit: 2,
    });
    job.unique = Some(butler::UniqueKey {
        key: r#"sync:["<b>7</b>"]"#.into(),
        until: butler::Unique::UntilFinished,
    });
    let id = f.queue.push_job(job).unwrap();
    let (status, html) = get(&f.app, &format!("/jobs/{id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("at most 2 running at once"));
    assert!(html.contains("Unique until finished"));
    assert!(html.contains("&#60;b&#62;7&#60;/b&#62;"));
    assert!(!html.contains("<b>7</b>"), "keys are escaped");

    let (_, html) = get(&f.app, &format!("/jobs/{}", f.pending)).await;
    assert!(!html.contains(">Keys<"), "no keys, no card");
}

#[tokio::test]
async fn queues_can_be_paused_and_resumed_from_the_dashboard() {
    let f = fixture("");
    let (_, html) = get(&f.app, "/").await;
    assert!(html.contains(r#"action="/queues/mailers/pause""#));
    assert!(!html.contains(">paused<"));

    let (status, _) = post(&f.app, "/queues/mailers/pause", "cross-site", "").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(f.queue.paused_queues().unwrap().is_empty(), "unchanged");

    let (status, location) = post(
        &f.app,
        "/queues/mailers/pause",
        "same-origin",
        "return_to=%2F",
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(location.as_deref(), Some("/"));
    assert_eq!(f.queue.paused_queues().unwrap(), ["mailers"]);
    // A queue that never had a job can be paused ahead of time, and is listed.
    post(&f.app, "/queues/imports/pause", "same-origin", "").await;
    let (_, html) = get(&f.app, "/").await;
    assert!(html.contains(">paused<"));
    assert!(html.contains(r#"action="/queues/mailers/resume""#));
    assert!(html.contains(r#"href="/jobs?state=pending&queue=imports""#));

    let (_, location) = post(
        &f.app,
        "/queues/mailers/resume",
        "same-origin",
        "return_to=https%3A%2F%2Fevil.example%2F",
    )
    .await;
    assert_eq!(location.as_deref(), Some("/"), "never an open redirect");
    assert_eq!(f.queue.paused_queues().unwrap(), ["imports"]);

    let (status, _) = post(&f.app, "/queues/..bad/pause", "same-origin", "").await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "not a queue name");
}

#[tokio::test]
async fn pause_and_resume_keep_the_base_path() {
    let f = fixture("/admin/jobs");
    let app = Router::new().nest("/admin/jobs", f.app.clone());
    let (status, html) = get(&app, "/admin/jobs").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains(r#"action="/admin/jobs/queues/mailers/pause""#));
    assert!(html.contains(r#"name="return_to" value="/admin/jobs""#));
    let (_, location) = post(
        &app,
        "/admin/jobs/queues/mailers/pause",
        "same-origin",
        "return_to=%2Fadmin%2Fjobs",
    )
    .await;
    assert_eq!(location.as_deref(), Some("/admin/jobs"));
    let (status, html) = get(&app, location.as_deref().unwrap()).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the redirect lands on the dashboard"
    );
    assert!(html.contains(r#"action="/admin/jobs/queues/mailers/resume""#));
}

/// Registers two recurring schedules on `queue`: "nightly", seen by a
/// worker just now and run once, and "retired", last seen an hour ago.
/// Returns the id of nightly's last job.
fn recurring_schedules(queue: &Queue) -> String {
    let schedule = |name: &str, key: &str| {
        butler::Recurring::from_parts(
            name.into(),
            "reports".into(),
            vec![json!("<script>alert(1)</script>")],
            butler::Cron::parse("0 3 * * *")
                .unwrap()
                .in_time_zone("Europe/Paris")
                .unwrap(),
        )
        .unwrap()
        .with_key(key)
        .unwrap()
    };
    let now = SystemTime::now();
    queue
        .register_recurring(&[
            schedule("nightly_report", "nightly").record(now),
            schedule("old_report", "retired").record(now - Duration::from_secs(3600)),
        ])
        .unwrap();
    queue
        .push_recurring(
            "nightly",
            now - Duration::from_secs(60),
            butler::NewJob::new("nightly_report", "reports", vec![]),
        )
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn recurring_schedules_show_their_next_and_last_run() {
    let f = fixture("");
    let (status, html) = get(&f.app, "/recurring").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("No recurring schedules"));

    let last_job = recurring_schedules(&f.queue);
    let (status, html) = get(&f.app, "/recurring").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains(r#"href="/recurring" class="nav-link active""#));
    assert!(html.contains("nightly_report") && html.contains("old_report"));
    assert!(html.contains("0 3 * * *") && html.contains("Europe/Paris"));
    assert!(html.contains("data-until-ms="), "next run");
    assert!(
        html.contains(&format!(r#"href="/jobs/{last_job}""#)),
        "last run links to its job"
    );
    assert!(html.contains("not yet"), "retired never ran");
    assert!(
        !html.contains("<script>alert(1)</script>"),
        "arguments are escaped"
    );
    assert!(html.contains("&#60;script&#62;"));
    // Only the schedule no worker runs anymore can be removed.
    assert!(html.contains(r#"action="/recurring/retired/remove""#));
    assert!(!html.contains(r#"action="/recurring/nightly/remove""#));
}

#[tokio::test]
async fn removing_a_recurring_schedule_is_a_same_site_post_that_redirects_back() {
    let f = fixture("");
    recurring_schedules(&f.queue);
    let keys = |queue: &Queue| {
        queue
            .recurring()
            .unwrap()
            .into_iter()
            .map(|s| s.key)
            .collect::<Vec<_>>()
    };

    let (status, _) = post(&f.app, "/recurring/retired/remove", "cross-site", "").await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(keys(&f.queue), ["nightly", "retired"], "unchanged");

    let (status, location) = post(
        &f.app,
        "/recurring/retired/remove",
        "same-origin",
        "return_to=%2Frecurring",
    )
    .await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    assert_eq!(location.as_deref(), Some("/recurring"));
    assert_eq!(keys(&f.queue), ["nightly"]);

    let (_, location) = post(
        &f.app,
        "/recurring/nightly/remove",
        "same-origin",
        "return_to=https%3A%2F%2Fevil.example%2F",
    )
    .await;
    assert_eq!(location.as_deref(), Some("/"), "never an open redirect");
}

#[tokio::test]
async fn recurring_links_and_actions_keep_the_base_path() {
    let f = fixture("/admin/jobs");
    let last_job = recurring_schedules(&f.queue);
    let app = Router::new().nest("/admin/jobs", f.app.clone());
    let (status, html) = get(&app, "/admin/jobs/recurring").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains(r#"href="/admin/jobs/recurring" class="nav-link active""#));
    assert!(html.contains(&format!(r#"href="/admin/jobs/jobs/{last_job}""#)));
    assert!(html.contains(r#"action="/admin/jobs/recurring/retired/remove""#));
    assert!(html.contains(r#"name="return_to" value="/admin/jobs/recurring""#));
    let (_, location) = post(
        &app,
        "/admin/jobs/recurring/retired/remove",
        "same-origin",
        "return_to=%2Fadmin%2Fjobs%2Frecurring",
    )
    .await;
    assert_eq!(location.as_deref(), Some("/admin/jobs/recurring"));
    assert_eq!(f.queue.recurring().unwrap().len(), 1);
}

#[tokio::test]
async fn removing_recurring_rejects_encoded_path_separators_and_preserves_files() {
    let dir = std::env::temp_dir().join(format!(
        "butler-dashboard-recurring-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let queue: Queue = butler::FileQueue::new(&dir).unwrap().into();
    let id = queue.push("report", "default", vec![]).unwrap();
    recurring_schedules(&queue);
    for base in ["", "/admin/jobs"] {
        let app = butler_web::Dashboard::new(queue.clone())
            .base_path(base)
            .router();
        let app = if base.is_empty() {
            app
        } else {
            Router::new().nest(base, app)
        };
        for key in ["..%2F..%2Fpending", "%2Fpending", "a%5Cb"] {
            let (status, _) = post(
                &app,
                &format!("{base}/recurring/{key}/remove"),
                "same-origin",
                "",
            )
            .await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{key}");
            assert!(dir.join("pending").is_dir());
            assert_eq!(queue.state(&id), Some(JobState::Pending));
            assert_eq!(queue.recurring().unwrap().len(), 2);
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}
