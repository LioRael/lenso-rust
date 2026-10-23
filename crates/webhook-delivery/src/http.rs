use crate::Error;
use hmac::{Hmac, KeyInit, Mac};
use sha2::Sha256;
use std::{
    net::{IpAddr, SocketAddr},
    time::Duration,
};

/// Fixed destination and pinned address, constructed before plugin startup.
/// No environment credentials/proxies, redirects, DNS lookup or implicit retries.
pub struct Endpoint {
    agent: ureq::Agent,
    pub(crate) identity: String,
    url: String,
    key_id: String,
    key: Vec<u8>,
}
impl Endpoint {
    /// Caller resolves DNS at configuration time. Every connection uses this checked
    /// address, while TLS still verifies the URL hostname. Rebuild to refresh DNS.
    pub fn https(
        url: &str,
        address: IpAddr,
        key_id: &str,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<Self, Error> {
        let parsed = url::Url::parse(url).map_err(|_| Error::Invalid)?;
        if parsed.scheme() != "https" || !public_address(address) {
            return Err(Error::Invalid);
        }
        if let Some(host) = parsed.host_str() {
            if let Ok(ip) = host.trim_matches(['[', ']']).parse::<IpAddr>() {
                if ip != address || !public_address(ip) {
                    return Err(Error::Invalid);
                }
            }
            if host.eq_ignore_ascii_case("localhost") || host.ends_with(".localhost") {
                return Err(Error::Invalid);
            }
        }
        let port = parsed.port_or_known_default().ok_or(Error::Invalid)?;
        Self::build(parsed, SocketAddr::new(address, port), key_id, key, timeout)
    }
    /// Explicit local HTTP fixture adapter. Always connects to IPv4 loopback.
    /// Never select this constructor from untrusted input or production config.
    pub fn local_fixture(port: u16, key: Vec<u8>, timeout: Duration) -> Result<Self, Error> {
        Self::build(
            url::Url::parse(&format!("http://127.0.0.1:{port}/events"))
                .map_err(|_| Error::Invalid)?,
            SocketAddr::from(([127, 0, 0, 1], port)),
            "fixture",
            key,
            timeout,
        )
    }
    fn build(
        url: url::Url,
        address: SocketAddr,
        key_id: &str,
        key: Vec<u8>,
        timeout: Duration,
    ) -> Result<Self, Error> {
        crate::identifier(key_id)?;
        if !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || url.query().is_some()
            || url.host_str().is_none()
            || key.len() < 32
            || key.len() > 4096
            || timeout.is_zero()
            || timeout > Duration::from_secs(30)
            || address.port() == 0
        {
            return Err(Error::Invalid);
        }
        let identity = format!("{url}|{key_id}");
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
            identity,
            url: url.into(),
            key_id: key_id.into(),
            key,
        })
    }
    pub(crate) fn send(&self, event: &str, payload: &[u8]) -> Option<u16> {
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.key).expect("validated HMAC key");
        mac.update(b"v1\n");
        mac.update(event.as_bytes());
        mac.update(b"\n");
        mac.update(payload);
        let signature: String = mac
            .finalize()
            .into_bytes()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect();
        let response = self
            .agent
            .post(&self.url)
            .set("Content-Type", "application/json")
            .set("Webhook-Event-Id", event)
            .set("Webhook-Key-Id", &self.key_id)
            .set("Authorization", &format!("LensoWebhook v1={signature}"))
            .send_bytes(payload);
        match response {
            Ok(r) | Err(ureq::Error::Status(_, r)) => Some(r.status()),
            Err(_) => None,
        }
    }
}

// Conservative globally routable allowlist: reject reserved/documentation ranges,
// IPv4-mapped IPv6, transition mechanisms and special-purpose IPv6 allocations.
fn public_address(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v) => {
            let [a, b, c, _] = v.octets();
            !(a == 0
                || a == 10
                || a == 127
                || a >= 224
                || (a == 100 && (64..=127).contains(&b))
                || (a == 169 && b == 254)
                || (a == 172 && (16..=31).contains(&b))
                || (a == 192 && (b == 168 || b == 0 || (b == 88 && c == 99)))
                || (a == 198 && (b == 18 || b == 19 || (b == 51 && c == 100)))
                || (a == 203 && b == 0 && c == 113))
        }
        IpAddr::V6(v) => {
            let s = v.segments();
            s[0] & 0xe000 == 0x2000
                && s[0] != 0x2002
                && !(s[0] == 0x2001 && (s[1] < 0x200 || s[1] == 0xdb8))
                && !(s[0] == 0x3fff && s[1] < 0x1000)
        }
    }
}
