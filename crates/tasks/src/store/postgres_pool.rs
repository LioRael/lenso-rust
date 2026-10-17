use super::postgres::PostgresConnection;
use super::StoreConnection;
use crate::TaskError;
use postgres::{
    tls::{MakeTlsConnect, TlsConnect},
    Client, Config, NoTls, Socket,
};
use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

type Connect = dyn Fn(&Config) -> Result<Client, postgres::Error> + Send + Sync;

/// PostgreSQL transport and bounded, eagerly opened connection pool.
///
/// Existing constructors use `NoTls`. Use `with_tls` with a driver-compatible
/// connector and `Config::ssl_mode(Require)` to require encryption. Certificate
/// and hostname verification are the connector's responsibility.
#[derive(Clone)]
pub struct PostgresOptions {
    config: Config,
    connect: Arc<Connect>,
    max_connections: usize,
    checkout_timeout: Duration,
}
impl PostgresOptions {
    pub fn new(config: Config) -> Self {
        Self::with_tls(config, NoTls)
    }

    pub fn with_tls<T>(mut config: Config, tls: T) -> Self
    where
        T: MakeTlsConnect<Socket> + Clone + Send + Sync + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        if config.get_connect_timeout().is_none() {
            config.connect_timeout(Duration::from_secs(5));
        }
        Self {
            config,
            connect: Arc::new(move |config| config.connect(tls.clone())),
            max_connections: 4,
            checkout_timeout: Duration::from_secs(5),
        }
    }

    /// Number of connections opened at startup, from 1 through 64 (default 4).
    pub fn max_connections(mut self, count: usize) -> Self {
        self.max_connections = count;
        self
    }

    /// Maximum time waiting for a pool slot, excluding connection and SQL time.
    /// Must be positive and at most one hour; default five seconds.
    pub fn checkout_timeout(mut self, timeout: Duration) -> Self {
        self.checkout_timeout = timeout;
        self
    }

    pub(crate) fn validate(&self) -> Result<(), TaskError> {
        if !(1..=64).contains(&self.max_connections)
            || self.checkout_timeout.is_zero()
            || self.checkout_timeout > Duration::from_secs(3600)
            || self
                .config
                .get_connect_timeout()
                .is_some_and(Duration::is_zero)
        {
            return Err(TaskError::InvalidOptions);
        }
        Ok(())
    }

    pub(crate) fn connect(&self) -> Result<Client, TaskError> {
        Ok((self.connect)(&self.config)?)
    }
}

struct State {
    // Each entry is an available slot. None needs a replacement connection.
    idle: Vec<Option<PostgresConnection>>,
    closed: bool,
}
pub(super) struct PostgresPool {
    options: PostgresOptions,
    queue: String,
    identity: String,
    state: Mutex<State>,
    available: Condvar,
}
impl PostgresPool {
    pub fn open(options: PostgresOptions, queue: String) -> Result<Self, TaskError> {
        options.validate()?;
        let mut idle = Vec::with_capacity(options.max_connections);
        let mut identity = None;
        for _ in 0..options.max_connections {
            let (connection, found) = PostgresConnection::open(&options, queue.clone())?;
            if identity.as_ref().is_some_and(|expected| expected != &found) {
                return Err(TaskError::QueueIdentityChanged);
            }
            identity = Some(found);
            idle.push(Some(connection));
        }
        Ok(Self {
            options,
            queue,
            identity: identity.expect("validated nonempty pool"),
            state: Mutex::new(State {
                idle,
                closed: false,
            }),
            available: Condvar::new(),
        })
    }

    pub fn identity(&self) -> &str {
        &self.identity
    }

    fn checkout(&self) -> Result<Lease<'_>, TaskError> {
        let deadline = Instant::now() + self.options.checkout_timeout;
        let mut state = self.state.lock().map_err(|_| TaskError::Runtime)?;
        loop {
            if state.closed {
                return Err(TaskError::Closed);
            }
            if let Some(connection) = state.idle.pop() {
                return Ok(Lease {
                    pool: self,
                    connection,
                });
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(TaskError::PostgresPoolTimeout);
            }
            state = self
                .available
                .wait_timeout(state, remaining)
                .map_err(|_| TaskError::Runtime)?
                .0;
        }
    }

    pub fn with<T>(
        &self,
        operation: impl FnOnce(&mut dyn StoreConnection) -> Result<T, TaskError>,
    ) -> Result<T, TaskError> {
        let mut lease = self.checkout()?;
        // Only a health probe may be retried. No caller SQL has run yet.
        if lease.connection.as_mut().is_some_and(|c| !c.healthy()) {
            lease.connection = None;
        }
        if lease.connection.is_none() {
            let (connection, identity) =
                PostgresConnection::open(&self.options, self.queue.clone())?;
            if identity != self.identity {
                return Err(TaskError::QueueIdentityChanged);
            }
            lease.connection = Some(connection);
        }
        let result = operation(lease.connection.as_mut().expect("connected slot"));
        // Never replay operations, even if COMMIT's outcome is unknown. Retire
        // errored sessions; a later invocation can restore this slot independently.
        if matches!(&result, Err(TaskError::Postgres(_))) {
            lease.connection = None;
        }
        result
    }

    pub fn close(&self) -> Result<(), TaskError> {
        let connections = {
            let mut state = self.state.lock().map_err(|_| TaskError::Runtime)?;
            state.closed = true;
            self.available.notify_all();
            std::mem::take(&mut state.idle)
        };
        let mut error = None;
        for connection in connections.into_iter().flatten() {
            if let Err(failed) = Box::new(connection).close() {
                error.get_or_insert(failed);
            }
        }
        error.map_or(Ok(()), Err)
    }
}

struct Lease<'a> {
    pool: &'a PostgresPool,
    connection: Option<PostgresConnection>,
}
impl Drop for Lease<'_> {
    fn drop(&mut self) {
        // A panic must not lose capacity or reuse a partially completed session.
        if std::thread::panicking() {
            self.connection = None;
        }
        if let Ok(mut state) = self.pool.state.lock() {
            if !state.closed {
                state.idle.push(self.connection.take());
                self.pool.available.notify_one();
            }
        }
    }
}

#[cfg(test)]
#[path = "postgres_pool_tests.rs"]
mod tests;
