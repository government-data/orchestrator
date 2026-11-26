use super::*;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};
use serde_json::json;

fn pair() -> Ed25519KeyPair {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap()
}

fn sign(pair: &Ed25519KeyPair, header: &Value, claims: &Value) -> String {
    let input = format!(
        "{}.{}",
        URL_SAFE_NO_PAD.encode(header.to_string()),
        URL_SAFE_NO_PAD.encode(claims.to_string())
    );
    let sig = URL_SAFE_NO_PAD.encode(pair.sign(input.as_bytes()).as_ref());
    format!("{input}.{sig}")
}

fn at(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn expected() -> Expected {
    Expected {
        issuer: "https://core.test".into(),
        audience: "orchestrator".into(),
    }
}

fn claims(client: &str) -> Value {
    json!({"iss": "https://core.test", "aud": ["orchestrator"], "client_id": client,
           "exp": at("2026-11-25T11:00:00Z").timestamp()})
}

#[test]
fn a_machine_token_names_its_module() {
    let p = pair();
    let keys = vec![("k1".to_string(), p.public_key().as_ref().to_vec())];
    let header = json!({"alg": "EdDSA", "typ": "at+jwt", "kid": "k1"});
    let now = at("2026-11-25T10:00:00Z");
    let token = sign(&p, &header, &claims("engine-a-machine"));
    assert_eq!(
        module_of(&token, &keys, &expected(), now).as_deref(),
        Some("engine-a")
    );
    let jwks = json!({"keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "k1",
        "x": URL_SAFE_NO_PAD.encode(p.public_key().as_ref())}, {"kty": "EC"}]});
    assert_eq!(ed25519_keys(&jwks), keys);
}

#[test]
fn what_a_machine_token_is_not() {
    let p = pair();
    let other = pair();
    let keys = vec![("k1".to_string(), p.public_key().as_ref().to_vec())];
    let header = json!({"alg": "EdDSA", "typ": "at+jwt", "kid": "k1"});
    let now = at("2026-11-25T10:00:00Z");
    let check = |token: &str| module_of(token, &keys, &expected(), now);
    assert_eq!(
        check(&sign(&other, &header, &claims("engine-a-machine"))),
        None,
        "other key"
    );
    assert_eq!(
        check(&sign(&p, &header, &claims("engine-a"))),
        None,
        "a people client"
    );
    let mut c = claims("engine-a-machine");
    c["aud"] = json!("another-service");
    assert_eq!(check(&sign(&p, &header, &c)), None, "another audience");
    let mut c = claims("engine-a-machine");
    c["iss"] = json!("https://elsewhere");
    assert_eq!(check(&sign(&p, &header, &c)), None, "another issuer");
    let late = module_of(
        &sign(&p, &header, &claims("engine-a-machine")),
        &keys,
        &expected(),
        at("2026-11-25T12:00:00Z"),
    );
    assert_eq!(late, None, "expired");
    let id = json!({"alg": "EdDSA", "typ": "JWT", "kid": "k1"});
    assert_eq!(
        check(&sign(&p, &id, &claims("engine-a-machine"))),
        None,
        "an id token"
    );
    assert_eq!(check("a.b"), None);
}
