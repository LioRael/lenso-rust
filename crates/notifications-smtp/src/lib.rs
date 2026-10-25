//! SMTP transport for the existing persistent Notifications service.
//!
//! One connection per attempt, no pool or retries. SMTP has no idempotency-key
//! guarantee: a Message-ID is correlation only. Lost DATA acknowledgements are
//! Unknown; Notifications must retain those receipts rather than replay them.
use lenso_notifications::{Error, Message, Outcome, Transport};
use lettre::{
    message::Mailbox,
    transport::smtp::{
        authentication::{Credentials, Mechanism},
        client::{AsyncSmtpConnection, TlsParameters},
        extension::ClientId,
    },
    Message as Email,
};
use std::{net::SocketAddr, time::Duration};

/// Explicit credentials; never loaded from environment or exposed through Debug.
pub use lettre::transport::smtp::authentication::Credentials as SmtpCredentials;

pub struct Smtp {
    runtime: Option<tokio::runtime::Runtime>,
    address: SocketAddr,
    tls: Option<TlsParameters>,
    credentials: Option<Credentials>,
    from: Mailbox,
    timeout: Duration,
    identity: String,
}

/// Detailed classification, before mapping into Notifications' coarser outcomes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    Accepted(String),
    TemporaryRejection,
    PermanentRejection,
    InvalidMessage,
    Unknown,
}
impl Smtp {
    /// Implicit TLS with public roots and hostname validation. The trusted host
    /// supplies a resolved socket address, avoiding DNS during delivery.
    /// Timeout bounds the entire SMTP session, including TLS, AUTH and QUIT.
    /// Call the synchronous Transport on a blocking worker (as Notifications does).
    ///
    /// # Errors
    /// Returns `Invalid` for invalid endpoint, sender, account, TLS or timeout
    /// configuration, or `Runtime` if the owned Tokio runtime cannot be created.
    pub fn new(
        account: &str,
        hostname: &str,
        address: SocketAddr,
        from: &str,
        credentials: Option<SmtpCredentials>,
        timeout: Duration,
    ) -> Result<Self, Error> {
        field(hostname)?;
        let tls = TlsParameters::new(hostname.to_owned()).map_err(|_| Error::Invalid)?;
        Self::build(
            account,
            hostname,
            address,
            from,
            Some(tls),
            credentials,
            timeout,
        )
    }

    /// Explicit plaintext fixture. The address is hard-coded to IPv4 loopback;
    /// credentials and arbitrary remote destinations are unavailable here.
    ///
    /// # Errors
    /// Returns `Invalid` for an invalid port/timeout or `Runtime` if runtime creation fails.
    pub fn local_fixture(port: u16, timeout: Duration) -> Result<Self, Error> {
        Self::build(
            "fixture",
            "localhost",
            ([127, 0, 0, 1], port).into(),
            "orders@example.invalid",
            None,
            None,
            timeout,
        )
    }

    fn build(
        account: &str,
        hostname: &str,
        address: SocketAddr,
        from: &str,
        tls: Option<TlsParameters>,
        credentials: Option<Credentials>,
        timeout: Duration,
    ) -> Result<Self, Error> {
        field(account)?;
        if address.port() == 0 || timeout.is_zero() || timeout > Duration::from_secs(30) {
            return Err(Error::Invalid);
        }
        let sender = mailbox(from)?;
        // Physical address may rotate within the same trusted endpoint/account.
        let identity = serde_json::to_string(&(
            "smtp",
            account,
            hostname,
            address.port(),
            from,
            tls.is_some(),
        ))
        .map_err(|_| Error::Invalid)?;
        field(&identity)?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .map_err(|_| Error::Runtime)?;
        Ok(Self {
            runtime: Some(runtime),
            address,
            tls,
            credentials,
            from: sender,
            timeout,
            identity,
        })
    }

    /// Blocking entry point. A returned Accepted ID is local correlation, not
    /// a server queue ID or evidence of inbox delivery. No internal retry occurs.
    pub fn deliver(&self, key: &str, message: &Message) -> Delivery {
        let email = match self.email(key, message) {
            Ok(email) => email,
            Err(_) => return Delivery::InvalidMessage,
        };
        // Notifications calls this on its admitted blocking worker. Cancellation
        // of that caller does not abandon this session or its durable attempt.
        self.runtime
            .as_ref()
            .expect("runtime lives until drop")
            .block_on(async {
                let mut settled = None;
                let session = async {
                    let hello = ClientId::Domain("localhost".into());
                    let mut connection = match AsyncSmtpConnection::connect_tokio1(
                        self.address,
                        Some(self.timeout),
                        &hello,
                        self.tls.clone(),
                        None,
                    )
                    .await
                    {
                        Ok(connection) => connection,
                        Err(error) => return classify(error),
                    };
                    if let Some(credentials) = &self.credentials {
                        if let Err(error) = connection
                            .auth(&[Mechanism::Plain, Mechanism::Login], credentials)
                            .await
                        {
                            return classify(error);
                        }
                    }
                    let outcome = match connection.send(email.envelope(), &email.formatted()).await
                    {
                        Ok(response) if response.code().to_string() == "250" => {
                            Delivery::Accepted(format!("{key}@lenso.invalid"))
                        }
                        Ok(_) => Delivery::Unknown,
                        Err(error) => classify(error),
                    };
                    // A failed or stalled QUIT cannot revoke DATA acceptance.
                    settled = Some(outcome.clone());
                    let _ = connection.quit().await;
                    outcome
                };
                match tokio::time::timeout(self.timeout, session).await {
                    Ok(outcome) => outcome,
                    Err(_) => settled.unwrap_or(Delivery::Unknown),
                }
                // Timeout drops the session future and its socket, with no retry.
            })
    }

    fn email(&self, key: &str, message: &Message) -> Result<Email, Error> {
        if key.is_empty()
            || key.len() > 128
            || !key
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(Error::Invalid);
        }
        field(&message.subject)?;
        if message.text.len() > 65536 || message.text.contains('\0') {
            return Err(Error::Invalid);
        }
        Email::builder()
            .from(self.from.clone())
            .to(mailbox(&message.to)?)
            .subject(&message.subject)
            .message_id(Some(format!("<{key}@lenso.invalid>")))
            .body(message.text.clone())
            .map_err(|_| Error::Invalid)
    }
}
impl Drop for Smtp {
    fn drop(&mut self) {
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
impl Transport for Smtp {
    fn identity(&self) -> &str {
        &self.identity
    }
    fn send(&self, key: &str, message: &Message) -> Outcome {
        match self.deliver(key, message) {
            Delivery::Accepted(id) => Outcome::Accepted(id),
            Delivery::TemporaryRejection
            | Delivery::PermanentRejection
            | Delivery::InvalidMessage => Outcome::Rejected,
            Delivery::Unknown => Outcome::Unknown,
        }
    }
}
fn classify(error: lettre::transport::smtp::Error) -> Delivery {
    if error.is_transient() {
        Delivery::TemporaryRejection
    } else if error.is_permanent() {
        Delivery::PermanentRejection
    } else {
        Delivery::Unknown
    }
}
fn field(value: &str) -> Result<(), Error> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        Err(Error::Invalid)
    } else {
        Ok(())
    }
}
fn mailbox(value: &str) -> Result<Mailbox, Error> {
    field(value)?;
    // Accept one bare ASCII addr-spec, not display-name/header/list syntax.
    if !value.is_ascii()
        || value
            .bytes()
            .any(|b| b.is_ascii_whitespace() || b"<>,;\"()".contains(&b))
    {
        return Err(Error::Invalid);
    }
    let address = value
        .parse::<lettre::Address>()
        .map_err(|_| Error::Invalid)?;
    Ok(Mailbox::new(None, address))
}
