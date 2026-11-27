//! The contract suite against the fake orchestrator an engine module's CI
//! uses, and the fake's own lease and hold rules.

use engine_port::{Claim, Finish, Renew, RenewAction};
use engine_runner::contract::{run_suite, FakeEngine, FakeOrchestrator, Seed};
use engine_runner::{CallError, Engine, Orchestrator};
use orchestration_model::FinalState;
use serde_json::json;

#[tokio::test]
async fn the_suite_passes_against_the_fake() {
    let fake = FakeOrchestrator::default();
    run_suite(fake.clone(), &fake).await;
    assert_eq!(fake.registered().len(), 1, "the descriptor was registered");
}

#[tokio::test]
async fn another_lease_changes_nothing() {
    let fake = FakeOrchestrator::default();
    let d = FakeEngine::default().descriptor();
    let id = fake.queue(json!({"behave": "succeed"})).await;
    let claim = Claim { runner: "r".into() };
    let claimed = fake.claim(&d, &claim).await.unwrap().expect("handed out");
    assert_eq!(claimed.run, id);
    let stale = Finish {
        lease: "lease-other".into(),
        state: FinalState::Succeeded,
        counts: Default::default(),
        error_class: None,
        interruption: None,
    };
    assert_eq!(fake.finish(&id, &stale).await, Err(CallError::Lease));
    assert_eq!(fake.state(&id).await, "running");
    assert!(fake.claim(&d, &claim).await.unwrap().is_none(), "one claim");
}

#[tokio::test]
async fn a_hold_stops_claims_and_renewals_say_held() {
    let fake = FakeOrchestrator::default();
    let d = FakeEngine::default().descriptor();
    let claim = Claim { runner: "r".into() };
    let first = fake.queue(json!({"behave": "wait"})).await;
    let claimed = fake.claim(&d, &claim).await.unwrap().unwrap();
    fake.hold(true);
    let _second = fake.queue(json!({"behave": "succeed"})).await;
    assert!(fake.claim(&d, &claim).await.unwrap().is_none());
    let renew = Renew {
        lease: claimed.lease,
        progress: None,
        engine_ref: Some("q-1".into()),
    };
    let renewed = fake.renew(&first, &renew).await.unwrap();
    assert_eq!(renewed.action, RenewAction::Held);
    assert_eq!(fake.run(&first).unwrap().engine_ref.as_deref(), Some("q-1"));
    fake.hold(false);
    assert!(fake.claim(&d, &claim).await.unwrap().is_some());
}

#[tokio::test]
async fn a_hold_stops_deploy_claims_too() {
    let fake = FakeOrchestrator::default();
    let d = FakeEngine::default().descriptor();
    let claim = Claim { runner: "r".into() };
    let id = fake.queue_import("/srv/found/a");
    let claimed = fake.claim_deploy(&d, &claim).await.unwrap().unwrap();
    assert_eq!(claimed.deploy, id);
    fake.hold(true);
    assert!(fake.claim_deploy(&d, &claim).await.unwrap().is_none());
    fake.hold(false);
    let again = fake.claim_deploy(&d, &claim).await.unwrap().unwrap();
    assert_eq!(again.deploy, id, "taken up again once released");
}
