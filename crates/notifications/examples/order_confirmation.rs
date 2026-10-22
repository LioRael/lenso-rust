//! Local capture only. Does not send email or read credentials.
use lenso_notifications::*;
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{EnqueueOptions, RetryPolicy, Task, TasksPlugin};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Capture(Mutex<Vec<Message>>);
impl Transport for Capture {
    fn identity(&self) -> &str {
        "local-capture/orders"
    }
    fn send(&self, _: &str, message: &Message) -> Outcome {
        self.0.lock().unwrap().push(message.clone());
        Outcome::Accepted("capture-only-42".into())
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("notifications.db");
    let tasks_path = temp.path().join("tasks.db");
    provision(&path).await?;
    lenso_tasks::provision(&tasks_path, "order-notifications").await?;
    let capture = Arc::new(Capture::default());
    let plugin = Instance::new(
        "notifications",
        NotificationsPlugin::new(&path, capture.clone()),
    );
    let handle = plugin.handle();
    let app = App::new().with(plugin).start().await?;
    let service = app.get(&handle)?;
    let business = async {
        // A trusted host derives this scope and chooses the template, never raw HTTP callers.
        let scope = Scope {
            tenant: "demo-shop".into(),
            owner: "demo-customer".into(),
        };
        let request = Request {
            scope: scope.clone(),
            business_key: "order-42-confirmation".into(),
            to: "customer@example.invalid".into(),
            template: Template {
                name: "order-confirmation".into(),
                version: 1,
                category: "orders".into(),
                required: true,
                subject: "Order {{order}} confirmed".into(),
                text: "Thank you. Order {{order}} is confirmed.".into(),
            },
            variables: [("order".into(), "42".into())].into(),
        };
        let first = service.create(request.clone()).await?;
        assert_eq!(first, service.create(request).await?);
        let sender = service.clone();
        let task_scope = scope.clone();
        let task = Task::new(
            "deliver-order",
            RetryPolicy::default(),
            move |id: i64, _| {
                let sender = sender.clone();
                let scope = task_scope.clone();
                async move {
                    let receipt = sender
                        .deliver(scope, id)
                        .await
                        .map_err(|_| lenso_tasks::Failure::HandlerFailed)?;
                    // Unknown is a successful inspection outcome: Tasks must not force a resend.
                    serde_json::to_value(receipt).map_err(|_| lenso_tasks::Failure::InvalidResult)
                }
            },
        );
        let queue = Instance::new(
            "tasks",
            TasksPlugin::new(&tasks_path, "order-notifications", vec![task]),
        );
        let qh = queue.handle();
        let tasks_app = App::new().with(queue).start().await?;
        let tasks = tasks_app.get(&qh)?;
        let delivery = async {
            let id = tasks
                .enqueue(
                    "deliver-order",
                    serde_json::json!(first.id),
                    EnqueueOptions {
                        deduplication_key: Some("order-42".into()),
                        ..Default::default()
                    },
                )
                .await?;
            tasks.run_one().await?;
            assert_eq!(
                tasks.get(id).await?.unwrap().state,
                lenso_tasks::JobState::Succeeded
            );
            let receipt = service.status(scope.clone(), first.id).await?;
            assert_eq!(receipt.state, State::Accepted);
            assert_eq!(capture.0.lock().unwrap().len(), 1);
            println!("Local capture receipt (not actual email delivery): {receipt:?}");
            Ok::<_, Box<dyn std::error::Error>>((scope, receipt))
        }
        .await;
        let cleanup = tasks_app.shutdown().await;
        if let Err(e) = cleanup {
            return Err(format!("delivery={delivery:?}; Tasks shutdown={e}").into());
        }
        delivery
    }
    .await;
    let cleanup = app.shutdown().await;
    if let Err(e) = cleanup {
        return Err(format!("business={business:?}; shutdown={e}").into());
    }
    let (scope, receipt) = business?;
    let reopened = Notifications::open(&path, capture).await?;
    let result = reopened.status(scope, receipt.id).await;
    let closed = reopened.close().await;
    assert_eq!(result?, receipt);
    closed?;
    println!("Restart preserved receipt; no second send.");
    Ok(())
}
