use postgres::Config;
use std::{path::PathBuf, process::Command, time::Duration};

/// Starts its own credential-free, socket-only database. Never reads PG* or .env.
pub struct Cluster {
    directory: tempfile::TempDir,
}
impl Cluster {
    pub fn new() -> Self {
        let directory = tempfile::Builder::new()
            .prefix("lenso-pg-test-")
            .tempdir_in("/tmp")
            .unwrap();
        let data = directory.path().join("data");
        let output = Command::new("initdb")
            .args(["-U", "lenso_test", "--auth=trust", "--no-locale", "-D"])
            .arg(&data)
            .output()
            .expect("initdb on PATH");
        assert!(
            output.status.success(),
            "initdb: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let cluster = Self { directory };
        let output = Command::new("pg_ctl")
            .arg("-D")
            .arg(&data)
            .arg("-l")
            .arg(cluster.directory.path().join("server.log"))
            .arg("-o")
            .arg(format!(
                "-k {} -h '' -p 55431",
                cluster.directory.path().display()
            ))
            .arg("-w")
            .arg("start")
            .output()
            .expect("pg_ctl on PATH");
        assert!(
            output.status.success(),
            "pg_ctl: {} {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        cluster
    }
    pub fn socket(&self) -> PathBuf {
        self.directory.path().to_owned()
    }
    pub fn config(&self) -> Config {
        config(self.socket())
    }
}
pub fn config(socket: PathBuf) -> Config {
    let mut config = Config::new();
    config
        .host_path(socket)
        .port(55431)
        .user("lenso_test")
        .dbname("postgres")
        .connect_timeout(Duration::from_secs(3))
        .options("-c statement_timeout=5000");
    config
}
impl Drop for Cluster {
    fn drop(&mut self) {
        let _ = Command::new("pg_ctl")
            .arg("-D")
            .arg(self.directory.path().join("data"))
            .args(["-m", "immediate", "-w", "stop"])
            .output();
    }
}
