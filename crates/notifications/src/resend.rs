use crate::{Error, Message, Outcome, Transport};
use std::{
    io::Read,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
/// Resend POST /emails adapter. Credentials are explicit; no environment lookup.
/// No redirects or application-level retries. Credentials never appear in Debug.
pub struct Resend {
    agent: ureq::Agent,
    endpoint: String,
    token: String,
    from: String,
    identity: String,
}
impl Resend {
    /// Host supplies a trusted pre-resolved IP at startup; TLS validates api.resend.com.
    /// This avoids unbounded platform DNS during delivery.
    pub fn new(
        account: &str,
        token: String,
        from: String,
        resolved_ip: IpAddr,
    ) -> Result<Self, Error> {
        Self::build(
            "https://api.resend.com/emails".into(),
            account,
            token,
            from,
            Duration::from_secs(10),
            SocketAddr::new(resolved_ip, 443),
        )
    }
    /// Local fixture only: fixed loopback IP and dummy credentials.
    pub fn local_fixture(port: u16, timeout: Duration) -> Result<Self, Error> {
        Self::build(
            format!("http://127.0.0.1:{port}/emails"),
            "fixture",
            "fixture-token".into(),
            "orders@example.invalid".into(),
            timeout,
            SocketAddr::from(([127, 0, 0, 1], port)),
        )
    }
    fn build(
        endpoint: String,
        account: &str,
        token: String,
        from: String,
        timeout: Duration,
        address: SocketAddr,
    ) -> Result<Self, Error> {
        crate::name(account)?;
        crate::name(&from)?;
        if token.is_empty()
            || token.len() > 4096
            || token.chars().any(char::is_control)
            || timeout.is_zero()
            || timeout > Duration::from_secs(30)
        {
            return Err(Error::Invalid);
        }
        let identity =
            serde_json::to_string(&(account, &endpoint, &from)).map_err(|_| Error::Invalid)?;
        crate::name(&identity)?;
        let agent = ureq::AgentBuilder::new()
            .timeout(timeout)
            .timeout_connect(timeout)
            .redirects(0)
            .try_proxy_from_env(false)
            .max_idle_connections(0)
            .resolver(move |_: &str| Ok(vec![address]))
            .build();
        Ok(Self {
            agent,
            endpoint,
            token,
            from,
            identity,
        })
    }
}
impl Transport for Resend {
    fn identity(&self) -> &str {
        &self.identity
    }
    fn send(&self, key: &str, message: &Message) -> Outcome {
        let result = self.agent.post(&self.endpoint)
            .set("Authorization",&format!("Bearer {}",self.token))
            .set("Idempotency-Key",key)
            .send_json(serde_json::json!({"from": self.from,"to":[message.to],"subject":message.subject,"text":message.text}));
        let response = match result {
            Ok(r) => r,
            Err(ureq::Error::Status(400 | 401 | 403 | 404 | 422, _)) => return Outcome::Rejected,
            Err(_) => return Outcome::Unknown,
        };
        if !(200..300).contains(&response.status()) {
            return Outcome::Unknown;
        }
        let mut bytes = Vec::new();
        if response
            .into_reader()
            .take(8193)
            .read_to_end(&mut bytes)
            .is_err()
            || bytes.len() > 8192
        {
            return Outcome::Unknown;
        }
        match serde_json::from_slice::<serde_json::Value>(&bytes)
            .ok()
            .and_then(|v| v.get("id").and_then(|id| id.as_str()).map(str::to_owned))
        {
            Some(id) if !id.is_empty() && id.len() <= 256 => Outcome::Accepted(id),
            _ => Outcome::Unknown,
        }
    }
}
