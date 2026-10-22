use lenso_i18n::{Catalog, I18nPlugin, Variables};
use lenso_notifications::{
    Message, NotificationsPlugin, Outcome, Request, Scope, Template, Transport,
};
use lenso_plugin_core::{App, Instance};
use std::sync::{Arc, Mutex};
#[derive(Default)]
struct Capture(Mutex<Vec<Message>>);
impl Transport for Capture {
    fn identity(&self) -> &str {
        "i18n-local-capture"
    }
    fn send(&self, _: &str, message: &Message) -> Outcome {
        self.0.lock().unwrap().push(message.clone());
        Outcome::Accepted("local-only".into())
    }
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let catalog = Catalog {
        version: 1,
        fallback: "en".into(),
        languages: [
            (
                "en".into(),
                [
                    ("page".into(), "Welcome, {{name}}".into()),
                    ("subject".into(), "Order confirmed".into()),
                    ("mail".into(), "Thank you, {{name}}.".into()),
                ]
                .into(),
            ),
            (
                "zh".into(),
                [
                    ("page".into(), "欢迎，{{name}}".into()),
                    ("subject".into(), "订单已确认".into()),
                    ("mail".into(), "谢谢您，{{name}}。".into()),
                ]
                .into(),
            ),
        ]
        .into(),
    };
    let temp = tempfile::tempdir()?;
    let path = temp.path().join("capture.db");
    lenso_notifications::provision(&path).await?;
    let capture = Arc::new(Capture::default());
    let i = Instance::new("i18n", I18nPlugin(catalog));
    let ih = i.handle();
    let n = Instance::new(
        "notifications",
        NotificationsPlugin::new(path, capture.clone()),
    );
    let nh = n.handle();
    let app = App::new().with(i).with(n).start().await?;
    let business = async {
        let i18n = app.get(&ih)?;
        let notifications = app.get(&nh)?;
        for language in ["en", "zh-CN,en;q=0.5"] {
            let request = i18n.request(language)?;
            let vars: Variables = [("name".into(), "Demo <Guest>".into())].into();
            let page = request.html_text("page", &vars)?;
            println!(
                "<html lang=\"{}\"><body><h1>{page}</h1></body></html>",
                request.locale()
            );
            assert!(!page.contains("<Guest>"));
            let template = request.notification(
                Template {
                    name: "order".into(),
                    version: 1,
                    category: "orders".into(),
                    required: true,
                    subject: String::new(),
                    text: String::new(),
                },
                "subject",
                "mail",
            )?;
            let scope = Scope {
                tenant: "demo".into(),
                owner: "fixture".into(),
            };
            let receipt = notifications
                .create(Request {
                    scope: scope.clone(),
                    business_key: request.locale().into(),
                    to: "demo@example.invalid".into(),
                    template,
                    variables: vars.clone(),
                })
                .await?;
            notifications.deliver(scope, receipt.id).await?;
            assert_eq!(
                capture.0.lock().unwrap().last().unwrap().text,
                request.text("mail", &vars)?
            );
        }
        for message in capture.0.lock().unwrap().iter() {
            println!("MAIL: {} / {}", message.subject, message.text);
        }
        Ok::<(), Box<dyn std::error::Error>>(())
    }
    .await;
    let shutdown = app.shutdown().await;
    if let Err(error) = shutdown {
        return Err(format!("business={business:?}; shutdown={error}").into());
    }
    business
}
