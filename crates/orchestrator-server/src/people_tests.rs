use std::sync::atomic::{AtomicUsize, Ordering};

use super::*;
use crate::{FixedClock, KeyList};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine as _;
use ring::rand::SystemRandom;
use ring::signature::{Ed25519KeyPair, KeyPair};

fn pair() -> Ed25519KeyPair {
    let pkcs8 = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new()).unwrap();
    Ed25519KeyPair::from_pkcs8(pkcs8.as_ref()).unwrap()
}

fn sign(pair: &Ed25519KeyPair, claims: &Value) -> String {
    let header = json!({"alg": "EdDSA", "typ": "at+jwt", "kid": "k1"});
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

/// A person's access token as the core's provider issues it.
fn person_claims() -> Value {
    json!({"iss": "https://core.test", "sub": "account-1", "aud": "orchestrator",
           "client_id": "orchestrator", "scope": "openid", "role": "user",
           "exp": at("2026-11-27T11:00:00Z").timestamp()})
}

struct StaticKeys(KeyList);

#[async_trait]
impl Keys for StaticKeys {
    async fn keys(&self) -> KeyList {
        self.0.clone()
    }
}

#[test]
fn a_persons_token_names_the_person_and_nothing_else_does() {
    let p = pair();
    let keys = vec![("k1".to_string(), p.public_key().as_ref().to_vec())];
    let now = at("2026-11-27T10:00:00Z");
    let check = |c: &Value| person_of(&sign(&p, c), &keys, &expected(), now);
    assert_eq!(
        check(&person_claims()).map(|p| p.id).as_deref(),
        Some("account-1")
    );

    let mut c = person_claims();
    c["client_id"] = json!("engine-a-machine");
    c["sub"] = json!("engine-a-machine");
    assert_eq!(check(&c), None, "a module's machine token");
    let mut c = person_claims();
    c["act"] = json!({"sub": "engine-a"});
    assert_eq!(check(&c), None, "an exchanged token");
    let mut c = person_claims();
    c["aud"] = json!("bi-superset");
    c["client_id"] = json!("bi-superset");
    assert_eq!(check(&c), None, "another module's people client");
    let mut c = person_claims();
    c["sub"] = json!("");
    assert_eq!(check(&c), None, "no subject");
    let late = person_of(
        &sign(&p, &person_claims()),
        &keys,
        &expected(),
        at("2026-11-27T12:00:00Z"),
    );
    assert_eq!(late, None, "expired");
    let other = pair();
    let forged = person_of(&sign(&other, &person_claims()), &keys, &expected(), now);
    assert_eq!(forged, None, "another key");
}

/// The core's stand-in: what it answers and how often it was asked.
#[derive(Default)]
struct FakeCore {
    answer: std::sync::Mutex<Option<Vec<String>>>,
    asked: AtomicUsize,
}

#[async_trait]
impl EffectiveActions for Arc<FakeCore> {
    async fn held(&self, _person: &Person) -> Option<Vec<String>> {
        self.asked.fetch_add(1, Ordering::SeqCst);
        self.answer.lock().unwrap().clone()
    }
}

fn ana() -> Person {
    Person { id: "ana".into() }
}

#[tokio::test]
async fn the_cores_answer_is_kept_a_minute_and_dropped_on_a_change() {
    let core = Arc::new(FakeCore::default());
    *core.answer.lock().unwrap() = Some(vec![IMPORT_FLOWS.into()]);
    let clock = FixedClock::at(at("2026-11-27T10:00:00Z"));
    let cached = CachedActions::new(core.clone(), clock.clone());
    assert_eq!(cached.held(&ana()).await, Some(vec![IMPORT_FLOWS.into()]));
    *core.answer.lock().unwrap() = Some(Vec::new());
    clock.advance(Duration::seconds(59));
    assert_eq!(cached.held(&ana()).await, Some(vec![IMPORT_FLOWS.into()]));
    assert_eq!(core.asked.load(Ordering::SeqCst), 1);
    clock.advance(Duration::seconds(1));
    assert_eq!(cached.held(&ana()).await, Some(Vec::new()), "60 s at most");
    *core.answer.lock().unwrap() = Some(vec![IMPORT_FLOWS.into()]);
    cached.forget().await;
    assert_eq!(cached.held(&ana()).await, Some(vec![IMPORT_FLOWS.into()]));
    assert_eq!(core.asked.load(Ordering::SeqCst), 3);
    *core.answer.lock().unwrap() = None;
    cached.forget().await;
    assert_eq!(cached.held(&ana()).await, None);
    assert_eq!(cached.held(&ana()).await, None, "no answer is not kept");
    assert_eq!(core.asked.load(Ordering::SeqCst), 5);
}

fn people(core: &Arc<FakeCore>, keys: KeyList) -> People {
    let clock = FixedClock::at(at("2026-11-27T10:00:00Z"));
    let tokens = OidcPeople::new(StaticKeys(keys), expected(), clock);
    People::new(Arc::new(tokens), Arc::new(core.clone()))
}

fn bearer(token: &str) -> HeaderMap {
    let mut h = HeaderMap::new();
    h.insert("authorization", format!("Bearer {token}").parse().unwrap());
    h
}

#[tokio::test]
async fn an_unauthorised_call_is_refused() {
    let p = pair();
    let keys = vec![("k1".to_string(), p.public_key().as_ref().to_vec())];
    let core = Arc::new(FakeCore::default());
    let people = people(&core, keys);
    let token = sign(&p, &person_claims());

    assert_eq!(
        people.allow(&HeaderMap::new(), IMPORT_FLOWS).await,
        Err(Refused::NotSignedIn)
    );
    assert_eq!(
        people.allow(&bearer("a.b.c"), IMPORT_FLOWS).await,
        Err(Refused::NotSignedIn)
    );
    assert_eq!(
        core.asked.load(Ordering::SeqCst),
        0,
        "no person, no question"
    );

    *core.answer.lock().unwrap() = Some(vec!["orchestrator:Other".into()]);
    assert_eq!(
        people.allow(&bearer(&token), IMPORT_FLOWS).await,
        Err(Refused::NotAllowed)
    );
    *core.answer.lock().unwrap() = None;
    assert_eq!(
        people.allow(&bearer(&token), IMPORT_FLOWS).await,
        Err(Refused::Unavailable)
    );
    *core.answer.lock().unwrap() = Some(vec![IMPORT_FLOWS.into()]);
    let person = people.allow(&bearer(&token), IMPORT_FLOWS).await.unwrap();
    assert_eq!(person.id, "account-1");
    assert!(
        !format!("{person:?}").contains("account-1"),
        "never in Debug"
    );

    let codes: Vec<u16> = [
        Refused::NotSignedIn,
        Refused::NotAllowed,
        Refused::Unavailable,
    ]
    .into_iter()
    .map(|r| r.into_response().status().as_u16())
    .collect();
    assert_eq!(codes, [401, 403, 503]);
}
