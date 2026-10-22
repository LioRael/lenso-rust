use lenso_i18n::*;
use lenso_plugin_core::{App, Instance};
fn catalog(version: u32) -> Catalog {
    Catalog {
        version,
        fallback: "en".into(),
        languages: [
            (
                "en".into(),
                [
                    ("hello".into(), "Hello {{name}}".into()),
                    ("fallback".into(), "English".into()),
                ]
                .into(),
            ),
            (
                "zh".into(),
                [("hello".into(), "你好 {{name}}".into())].into(),
            ),
        ]
        .into(),
    }
}
#[tokio::test]
async fn contracts_and_lifecycle() {
    let instance = Instance::new("i18n", I18nPlugin(catalog(1)));
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let i18n = app.get(&handle).unwrap();
    for (header, expected) in [
        ("zh-CN;q=0.9,en;q=0.4", "zh"),
        ("en;q=0.2,zh;q=0.8", "zh"),
        ("zh;q=0,*;q=1", "en"),
        ("fr", "en"),
        ("", "en"),
        ("*", "en"),
        ("zh;q=0.5,en;q=0.5", "zh"),
    ] {
        assert_eq!(i18n.request(header).unwrap().locale(), expected);
    }
    for bad in [
        "en;q=NaN",
        "en;q=1.1",
        "zh;q=-1",
        "zh;q=0.1234",
        "en;q=0.5;x=1",
    ] {
        assert!(matches!(i18n.request(bad), Err(Error::Invalid)));
    }
    let request = i18n.request("zh").unwrap();
    let vars = [("name".into(), "<script>&\"'{{name}}".into())].into();
    assert_eq!(
        request.text("hello", &vars).unwrap(),
        "你好 <script>&\"'{{name}}"
    );
    assert_eq!(
        request.html_text("hello", &vars).unwrap(),
        "你好 &lt;script&gt;&amp;&quot;&#39;{{name}}"
    );
    assert_eq!(
        request.text("fallback", &Variables::new()).unwrap(),
        "English"
    );
    assert_eq!(
        request.text("missing", &Variables::new()),
        Err(Error::Missing)
    );
    assert_eq!(
        request.text("hello", &Variables::new()),
        Err(Error::Invalid)
    );
    assert_eq!(request.text("fallback", &vars), Err(Error::Invalid));
    let mut bad = catalog(2);
    bad.languages
        .get_mut("zh")
        .unwrap()
        .insert("hello".into(), "{{illegal}}".into());
    assert_eq!(i18n.replace(bad), Err(Error::Invalid));
    assert_eq!(i18n.request("en").unwrap().version(), 1);
    let mut bad = catalog(2);
    bad.languages
        .get_mut("en")
        .unwrap()
        .insert("hello".into(), "{{broken".into());
    assert_eq!(i18n.replace(bad), Err(Error::Invalid));
    i18n.replace(catalog(2)).unwrap();
    assert_eq!(request.version(), 1);
    assert_eq!(i18n.request("zh").unwrap().version(), 2);
    assert_eq!(i18n.replace(catalog(2)), Err(Error::Version));
    let mut threads = Vec::new();
    for index in 0..32 {
        let service = i18n.clone();
        threads.push(std::thread::spawn(move || {
            let lang = if index % 2 == 0 { "en" } else { "zh" };
            let r = service.request(lang).unwrap();
            std::thread::yield_now();
            assert_eq!(r.locale(), lang);
            assert_eq!(
                r.text("hello", &[("name".into(), "A".into())].into())
                    .unwrap(),
                if lang == "en" { "Hello A" } else { "你好 A" }
            );
        }));
    }
    for thread in threads {
        thread.join().unwrap();
    }
    app.shutdown().await.unwrap();
    assert_eq!(request.text("hello", &vars), Err(Error::Closed));
    assert!(matches!(i18n.request("en"), Err(Error::Closed)));
    assert_eq!(i18n.replace(catalog(3)), Err(Error::Closed));
}
#[tokio::test]
async fn rejects_bad_catalog_at_startup() {
    let mut bad = catalog(1);
    bad.fallback = "fr".into();
    assert!(App::new()
        .with(Instance::new("bad", I18nPlugin(bad)))
        .start()
        .await
        .is_err());
}
