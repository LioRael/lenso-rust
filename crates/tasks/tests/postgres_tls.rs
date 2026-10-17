#![cfg(feature = "postgres")]
#[path = "support/postgres.rs"]
mod support;
use lenso_plugin_core::{App, Instance};
use lenso_tasks::{
    provision_postgres_with_options, EnqueueOptions, PostgresConfig, PostgresOptions, RetryPolicy,
    Task, TasksPlugin,
};
use native_tls::{Certificate, TlsConnector};
use postgres::{config::SslMode, NoTls};
use postgres_native_tls::MakeTlsConnector;
use serde_json::{json, Value};
use std::{net::TcpListener, os::unix::fs::PermissionsExt, process::Command, time::Duration};

#[test]
#[ignore = "requires local initdb/pg_ctl/openssl; owns a temporary loopback-only TLS cluster"]
fn postgres_tls_contract() {
    let cluster = support::Cluster::new();
    // Exercise the existing socket-only fixture config before switching this owned
    // cluster to TLS on an ephemeral loopback port.
    cluster.config().connect(NoTls).unwrap().close().unwrap();
    let root = cluster.socket();
    let certificate = root.join("server.crt");
    let key = root.join("server.key");
    let generated = Command::new("openssl")
        .args([
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "1",
            "-subj",
            "/CN=localhost",
            "-addext",
            "subjectAltName=DNS:localhost",
            "-addext",
            "extendedKeyUsage=serverAuth",
            "-keyout",
        ])
        .arg(&key)
        .arg("-out")
        .arg(&certificate)
        .output()
        .unwrap();
    assert!(
        generated.status.success(),
        "openssl: {}",
        String::from_utf8_lossy(&generated.stderr)
    );
    std::fs::set_permissions(&key, std::fs::Permissions::from_mode(0o600)).unwrap();
    let stop = Command::new("pg_ctl")
        .arg("-D")
        .arg(root.join("data"))
        .args(["-m", "fast", "-w", "stop"])
        .output()
        .unwrap();
    assert!(stop.status.success());
    std::fs::write(root.join("data/pg_hba.conf"), "local all all trust\nhostnossl all all 127.0.0.1/32 reject\nhostssl all all 127.0.0.1/32 trust\n").unwrap();
    let reservation = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let start = Command::new("pg_ctl")
        .arg("-D")
        .arg(root.join("data"))
        .arg("-l")
        .arg(root.join("server.log"))
        .arg("-o")
        .arg(format!(
            "-k {} -h 127.0.0.1 -p {port} -c ssl=on -c ssl_cert_file={} -c ssl_key_file={}",
            root.display(),
            certificate.display(),
            key.display()
        ))
        .args(["-w", "start"])
        .output()
        .unwrap();
    assert!(
        start.status.success(),
        "pg_ctl: {}",
        String::from_utf8_lossy(&start.stderr)
    );
    let config = |host: &str| {
        let mut config = PostgresConfig::new();
        config
            .host(host)
            .hostaddr("127.0.0.1".parse().unwrap())
            .port(port)
            .user("lenso_test")
            .dbname("postgres")
            .application_name("lenso-tls-test")
            .ssl_mode(SslMode::Require)
            .connect_timeout(Duration::from_secs(3))
            .options("-c statement_timeout=5000");
        config
    };
    let connector = || {
        let mut builder = TlsConnector::builder();
        builder.disable_built_in_roots(true).add_root_certificate(
            Certificate::from_pem(&std::fs::read(&certificate).unwrap()).unwrap(),
        );
        MakeTlsConnector::new(builder.build().unwrap())
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        assert!(provision_postgres_with_options(PostgresOptions::new(config("localhost")), "tls").await.is_err());
        let untrusted = MakeTlsConnector::new(TlsConnector::builder().disable_built_in_roots(true).build().unwrap());
        assert!(provision_postgres_with_options(PostgresOptions::with_tls(config("localhost"), untrusted), "tls").await.is_err());
        assert!(provision_postgres_with_options(PostgresOptions::with_tls(config("wrong-host.invalid"), connector()), "tls").await.is_err());
        let options = PostgresOptions::with_tls(config("localhost"), connector()).max_connections(2);
        provision_postgres_with_options(options.clone(), "tls").await.unwrap();
        let instance = Instance::new("tls", TasksPlugin::postgres_with_options(options, "tls", vec![Task::new("echo", RetryPolicy::default(), |v: Value, _| async { Ok(v) })]));
        let handle = instance.handle();
        let app = App::new().with(instance).start().await.unwrap();
        let tasks = app.get(&handle).unwrap();
        let id = tasks.enqueue("echo", json!("tls"), EnqueueOptions { deduplication_key: Some("tls".into()), ..Default::default() }).await.unwrap();
        let admin_config = {
            let mut c = PostgresConfig::new();
            c.host_path(&root).port(port).user("lenso_test").dbname("postgres");
            c
        };
        let observe = admin_config.clone();
        tokio::task::spawn_blocking(move || {
            let mut admin = observe.connect(NoTls).unwrap();
            let row = admin.query_one("SELECT count(*)::bigint, bool_and(s.ssl) FROM pg_stat_activity a JOIN pg_stat_ssl s USING(pid) WHERE a.application_name='lenso-tls-test'", &[]).unwrap();
            assert_eq!(row.get::<_, i64>(0), 2);
            assert!(row.get::<_, bool>(1));
            admin.query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE application_name='lenso-tls-test'", &[]).unwrap();
        }).await.unwrap();
        assert_eq!(tasks.lookup("tls").await.unwrap().unwrap().id, id);
        assert_eq!(tasks.run_one().await.unwrap().unwrap().result, Some(json!("tls")));
        app.shutdown().await.unwrap();
        tokio::task::spawn_blocking(move || {
            let mut admin = admin_config.connect(NoTls).unwrap();
            let start = std::time::Instant::now();
            loop {
                let count: i64 = admin.query_one("SELECT count(*) FROM pg_stat_activity WHERE application_name='lenso-tls-test'", &[]).unwrap().get(0);
                if count == 0 { break; }
                assert!(start.elapsed() < Duration::from_secs(3));
                std::thread::sleep(Duration::from_millis(5));
            }
        }).await.unwrap();
    });
}
