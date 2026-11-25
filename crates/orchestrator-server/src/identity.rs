//! Who calls the engine port, as Traefik tells it: the module's route on
//! the `internal` entry point requires a client certificate of the
//! installation's CA and passes its subject in
//! `X-Forwarded-Tls-Client-Cert-Info` (`passTLSClientCert`), URL-encoded,
//! `Subject="O=qd,OU=<role>,CN=<name>";SerialNumber="<decimal>"`, the
//! verified certificate first (decision 0011, verification V8). Traefik
//! replaces a header of that name a client sends.
//!
//! The header proves who calls only because the server is reachable through
//! Traefik alone: it publishes no port, and its network is the module's.

use async_trait::async_trait;
use axum::http::HeaderMap;

use crate::{Authenticate, Machine};

/// The header Traefik writes the client certificate's subject in.
pub const CLIENT_CERT_INFO: &str = "x-forwarded-tls-client-cert-info";

/// The organisation every machine certificate of an installation names.
pub(crate) const ORGANISATION: &str = "qd";

/// Reads the calling machine from Traefik's header.
#[derive(Debug, Clone, Copy, Default)]
pub struct TraefikMachine;

#[async_trait]
impl Authenticate for TraefikMachine {
    async fn machine(&self, headers: &HeaderMap) -> Option<Machine> {
        named_machine(headers).map(|m| Machine(m.name))
    }
}

/// A machine certificate's subject, as Traefik passes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedMachine {
    pub role: Option<String>,
    pub name: String,
}

/// The machine named by the first (verified) certificate in `headers`;
/// `None` without exactly one such header, or with anything else than an
/// installation's machine subject.
#[must_use]
pub fn named_machine(headers: &HeaderMap) -> Option<NamedMachine> {
    let mut values = headers.get_all(CLIENT_CERT_INFO).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return None;
    };
    let decoded = percent_decode(value.to_str().ok()?.trim())?;
    let rest = decoded.strip_prefix("Subject=\"")?;
    let (subject, _) = rest.split_once('"')?;
    let (mut org, mut role, mut name) = (None, None, None);
    for part in subject.split(',') {
        let (key, value) = part.split_once('=')?;
        let slot = match key {
            "O" => &mut org,
            "OU" => &mut role,
            "CN" => &mut name,
            _ => return None,
        };
        if slot.is_some() || value.is_empty() || !value.bytes().all(is_plain) {
            return None;
        }
        *slot = Some(value.to_string());
    }
    (org.as_deref() == Some(ORGANISATION)).then_some(())?;
    Some(NamedMachine { role, name: name? })
}

fn is_plain(b: u8) -> bool {
    b.is_ascii_alphanumeric() || matches!(b, b'-' | b'.' | b'_')
}

fn percent_decode(raw: &str) -> Option<String> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    /// What V8 saw Traefik 3.6.23 write for a worker's certificate.
    const SEEN: &str = "Subject%3D%22O%3Dqd%2COU%3Dworker%2CCN%3Dcluster-1%22%3BSerialNumber%3D%22671339952135252498188578807256094516038729581486%22%2CSubject%3D%22O%3Dqd%2CCN%3Dca%22%3BSerialNumber%3D%22354064348813046406487911612557262388711970302739%22";

    fn headers(values: &[&str]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for v in values {
            h.append(CLIENT_CERT_INFO, HeaderValue::from_str(v).unwrap());
        }
        h
    }

    #[test]
    fn reads_the_verified_certificate_traefik_passes() {
        assert_eq!(
            named_machine(&headers(&[SEEN])),
            Some(NamedMachine {
                role: Some("worker".into()),
                name: "cluster-1".into()
            })
        );
    }

    #[tokio::test]
    async fn refuses_what_is_not_one_machine_of_the_installation() {
        let other_org = "Subject%3D%22O%3Dacme%2CCN%3Dx%22%3BSerialNumber%3D%221%22";
        let no_name = "Subject%3D%22O%3Dqd%2COU%3Dworker%22%3BSerialNumber%3D%221%22";
        let odd = "Subject%3D%22O%3Dqd%2CCN%3Da%2Fb%22%3BSerialNumber%3D%221%22";
        let twice = "Subject%3D%22O%3Dqd%2CCN%3Da%2CCN%3Db%22";
        for case in [other_org, no_name, odd, twice, "garbage", "%zz"] {
            assert_eq!(named_machine(&headers(&[case])), None, "{case}");
        }
        assert_eq!(named_machine(&headers(&[SEEN, SEEN])), None, "two headers");
        assert_eq!(TraefikMachine.machine(&HeaderMap::new()).await, None);
        let m = TraefikMachine.machine(&headers(&[SEEN])).await;
        assert_eq!(m, Some(Machine("cluster-1".into())));
    }
}
