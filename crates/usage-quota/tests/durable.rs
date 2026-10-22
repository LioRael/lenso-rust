use lenso_usage_quota::{Balance, Command as C, Error, Outcome, UsageQuota, UsageQuotaPlugin};
use std::sync::{Arc, Barrier};
use tempfile::TempDir;

fn fixture(limit: i64) -> (TempDir, UsageQuota) {
    let dir = tempfile::tempdir().unwrap();
    let store = UsageQuota::open(dir.path().join("usage.db")).unwrap();
    store
        .execute(
            "create",
            1,
            C::Create {
                scope: "api".into(),
                limit,
            },
        )
        .unwrap();
    (dir, store)
}
fn reserve(id: &str, amount: i64, expires_at: i64) -> C {
    C::Reserve {
        scope: "api".into(),
        reservation: id.into(),
        amount,
        expires_at,
    }
}
fn call(request: &str, units: i64) -> C {
    C::ApiCall {
        scope: "api".into(),
        request: request.into(),
        units,
    }
}

#[test]
fn partial_consumption_release_and_durable_replay() {
    let (dir, store) = fixture(10);
    let original = store.execute("reserve", 2, reserve("r", 8, 100)).unwrap();
    store
        .execute(
            "consume",
            3,
            C::Consume {
                reservation: "r".into(),
                amount: 3,
            },
        )
        .unwrap();
    assert_eq!(
        store
            .execute(
                "release",
                4,
                C::Release {
                    reservation: "r".into()
                }
            )
            .unwrap()
            .units,
        5
    );
    assert_eq!(
        store.balance("api").unwrap(),
        Balance {
            limit: 10,
            used: 3,
            reserved: 0
        }
    );
    store.close().unwrap();
    let reopened = UsageQuota::open(dir.path().join("usage.db")).unwrap();
    assert_eq!(
        reopened
            .execute("reserve", 200, reserve("r", 8, 100))
            .unwrap(),
        original
    );
    assert_eq!(reopened.records().unwrap().len(), 4);
    assert!(matches!(
        reopened.execute("reserve", 5, reserve("r", 7, 100)),
        Err(Error::Conflict)
    ));
    assert!(matches!(
        reopened.execute(
            "late",
            5,
            C::Consume {
                reservation: "r".into(),
                amount: 1
            }
        ),
        Err(Error::Terminal)
    ));
    assert_eq!(
        reopened.outcome("reserve").unwrap(),
        Outcome::Committed(original)
    );
    assert_eq!(reopened.outcome("missing").unwrap(), Outcome::NotObserved);
}

#[test]
fn restart_expiration_is_inclusive_and_idempotent() {
    let (dir, store) = fixture(10);
    store.execute("r", 2, reserve("r", 9, 5)).unwrap();
    drop(store);
    let store = UsageQuota::open(dir.path().join("usage.db")).unwrap();
    assert!(matches!(
        store.execute(
            "consume",
            5,
            C::Consume {
                reservation: "r".into(),
                amount: 1
            }
        ),
        Err(Error::Terminal)
    ));
    assert!(matches!(
        store.execute(
            "release",
            5,
            C::Release {
                reservation: "r".into()
            }
        ),
        Err(Error::Terminal)
    ));
    assert_eq!(store.balance("api").unwrap().reserved, 9);
    assert_eq!(
        store
            .execute(
                "sweep",
                5,
                C::Expire {
                    scope: "api".into()
                }
            )
            .unwrap()
            .units,
        9
    );
    assert_eq!(
        store
            .execute(
                "sweep",
                6,
                C::Expire {
                    scope: "api".into()
                }
            )
            .unwrap()
            .units,
        9
    );
    assert_eq!(
        store
            .execute(
                "sweep2",
                6,
                C::Expire {
                    scope: "api".into()
                }
            )
            .unwrap()
            .units,
        0
    );
    assert_eq!(store.balance("api").unwrap().reserved, 0);
    assert!(matches!(
        store.execute("reuse", 6, reserve("r", 1, 10)),
        Err(Error::Conflict)
    ));
    assert_eq!(store.balance("api").unwrap().reserved, 0);
}

#[test]
fn api_fixture_consumes_once_and_rolls_back_on_conflict_or_failure() {
    let (dir, store) = fixture(10);
    let original = store.execute("op", 2, call("request", 4)).unwrap();
    assert_eq!(
        store.execute("op", 3, call("request", 4)).unwrap(),
        original
    );
    assert!(matches!(
        store.execute("different", 3, call("request", 4)),
        Err(Error::Conflict)
    ));
    assert!(matches!(
        store.execute("too-much", 3, call("second", 7)),
        Err(Error::Exhausted)
    ));
    let connection = rusqlite::Connection::open(dir.path().join("usage.db")).unwrap();
    connection.execute_batch("CREATE TRIGGER fixture_failure BEFORE INSERT ON uq_api_calls BEGIN SELECT RAISE(ABORT, 'fixture failure'); END;").unwrap();
    assert!(matches!(
        store.execute("failed", 3, call("third", 2)),
        Err(Error::Database(_))
    ));
    assert_eq!(store.balance("api").unwrap().used, 4);
    assert_eq!(store.api_calls("api").unwrap(), 1);
    assert_eq!(store.records().unwrap().len(), 2);
    assert_eq!(store.outcome("failed").unwrap(), Outcome::NotObserved);
}

#[test]
fn independent_connections_cannot_overspend() {
    let (dir, store) = fixture(7);
    let connections: Vec<_> = (0..16)
        .map(|_| UsageQuota::open(dir.path().join("usage.db")).unwrap())
        .collect();
    let barrier = Arc::new(Barrier::new(connections.len()));
    let threads: Vec<_> = connections
        .into_iter()
        .enumerate()
        .map(|(n, connection)| {
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                connection.execute(&format!("op-{n}"), 2, call(&format!("req-{n}"), 1))
            })
        })
        .collect();
    let results: Vec<_> = threads.into_iter().map(|t| t.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 7);
    assert!(results
        .iter()
        .all(|r| r.is_ok() || matches!(r, Err(Error::Exhausted))));
    assert_eq!(store.balance("api").unwrap().used, 7);
    assert_eq!(store.api_calls("api").unwrap(), 7);
}

#[test]
fn racing_duplicate_and_terminal_operations_are_serialized() {
    let (dir, store) = fixture(10);
    let connections: Vec<_> = (0..8)
        .map(|_| UsageQuota::open(dir.path().join("usage.db")).unwrap())
        .collect();
    let threads: Vec<_> = connections
        .into_iter()
        .map(|connection| {
            std::thread::spawn(move || {
                connection
                    .execute("reserve", 2, reserve("r", 10, 20))
                    .unwrap()
            })
        })
        .collect();
    for thread in threads {
        assert_eq!(thread.join().unwrap().units, 10);
    }
    assert_eq!(store.balance("api").unwrap().reserved, 10);
    let a = UsageQuota::open(dir.path().join("usage.db")).unwrap();
    let b = UsageQuota::open(dir.path().join("usage.db")).unwrap();
    let consume = std::thread::spawn(move || {
        a.execute(
            "consume",
            3,
            C::Consume {
                reservation: "r".into(),
                amount: 10,
            },
        )
    });
    let release = std::thread::spawn(move || {
        b.execute(
            "release",
            3,
            C::Release {
                reservation: "r".into(),
            },
        )
    });
    let results = [consume.join().unwrap(), release.join().unwrap()];
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(store.balance("api").unwrap().reserved, 0);
}

#[test]
fn validation_clock_and_integer_boundaries() {
    let (_dir, store) = fixture(i64::MAX);
    assert!(matches!(
        store.execute("negative", 2, reserve("r", -1, 4)),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        store.execute("expired", 2, reserve("r", 1, 2)),
        Err(Error::Invalid(_))
    ));
    store.execute("max", 2, reserve("r", i64::MAX, 10)).unwrap();
    assert!(matches!(
        store.execute("overflow", 2, call("x", 1)),
        Err(Error::Exhausted)
    ));
    assert!(matches!(
        store.execute(
            "past",
            1,
            C::Expire {
                scope: "api".into()
            }
        ),
        Err(Error::Invalid(_))
    ));
    store
        .execute(
            "consume",
            3,
            C::Consume {
                reservation: "r".into(),
                amount: i64::MAX,
            },
        )
        .unwrap();
    assert_eq!(store.balance("api").unwrap().used, i64::MAX);
}

#[tokio::test]
async fn plugin_closes_retained_service() {
    use lenso_plugin_core::{App, Instance};
    let dir = tempfile::tempdir().unwrap();
    let instance = Instance::new("usage", UsageQuotaPlugin::new(dir.path().join("usage.db")));
    let handle = instance.handle();
    let app = App::new().with(instance).start().await.unwrap();
    let service = app.get(&handle).unwrap();
    service
        .execute(
            "create",
            1,
            C::Create {
                scope: "api".into(),
                limit: 2,
            },
        )
        .unwrap();
    app.shutdown().await.unwrap();
    assert!(matches!(
        service.execute("request", 2, call("x", 1)),
        Err(Error::Closed)
    ));
    assert!(matches!(service.outcome("create"), Err(Error::Closed)));
}

#[test]
fn process_exit_recovery() {
    const CHILD_PATH: &str = "LENSO_USAGE_QUOTA_RECOVERY_TEST_DB";
    if let Some(path) = std::env::var_os(CHILD_PATH) {
        let service = UsageQuota::open(path).unwrap();
        service.execute("reserve", 2, reserve("r", 6, 10)).unwrap();
        service
            .execute(
                "consume",
                3,
                C::Consume {
                    reservation: "r".into(),
                    amount: 2,
                },
            )
            .unwrap();
        // Exit without destructors, explicit close, or WAL checkpoint.
        std::process::exit(0);
    }
    let (dir, service) = fixture(10);
    service.close().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "process_exit_recovery", "--nocapture"])
        .env(CHILD_PATH, dir.path().join("usage.db"))
        .status()
        .unwrap();
    assert!(status.success());
    let recovered = UsageQuota::open(dir.path().join("usage.db")).unwrap();
    assert_eq!(
        recovered.balance("api").unwrap(),
        Balance {
            limit: 10,
            used: 2,
            reserved: 4
        }
    );
    assert!(matches!(
        recovered.outcome("consume").unwrap(),
        Outcome::Committed(_)
    ));
    recovered
        .execute(
            "expire",
            10,
            C::Expire {
                scope: "api".into(),
            },
        )
        .unwrap();
    assert_eq!(
        recovered.balance("api").unwrap(),
        Balance {
            limit: 10,
            used: 2,
            reserved: 0
        }
    );
}

#[test]
fn expiration_is_scoped_even_at_integer_limit() {
    let (_dir, service) = fixture(i64::MAX);
    service
        .execute(
            "create-other",
            1,
            C::Create {
                scope: "other".into(),
                limit: 1,
            },
        )
        .unwrap();
    service
        .execute("r1", 2, reserve("r1", i64::MAX, 3))
        .unwrap();
    service
        .execute(
            "r2",
            2,
            C::Reserve {
                scope: "other".into(),
                reservation: "r2".into(),
                amount: 1,
                expires_at: 3,
            },
        )
        .unwrap();
    assert_eq!(
        service
            .execute(
                "expire",
                3,
                C::Expire {
                    scope: "api".into()
                }
            )
            .unwrap()
            .units,
        i64::MAX
    );
    assert_eq!(service.balance("api").unwrap().reserved, 0);
    assert_eq!(service.balance("other").unwrap().reserved, 1);
}
