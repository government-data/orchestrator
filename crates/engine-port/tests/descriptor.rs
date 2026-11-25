//! A runner's descriptor: it round-trips, and the orchestrator refuses a
//! port it does not serve or a descriptor that is malformed.

use engine_port::*;
use orchestration_model::{EngineName, Line};
use serde_json::json;

fn descriptor() -> Descriptor {
    Descriptor {
        engine: EngineName::new("engine-a").unwrap(),
        line: Line::new("-").unwrap(),
        engine_version: "1.2.3".into(),
        port: PORT,
        kinds: vec![RunKind {
            name: "load".into(),
            parameters: json!({"type": "object"}),
        }],
        concurrency: 2,
    }
}

#[test]
fn a_descriptor_and_its_answer_round_trip() {
    let wire = serde_json::to_string(&descriptor()).unwrap();
    assert_eq!(
        serde_json::from_str::<Descriptor>(&wire).unwrap(),
        descriptor()
    );
    let answer = Registered::for_port(PORT);
    assert_eq!(
        serde_json::to_value(&answer).unwrap(),
        json!({"port": 1, "lease_secs": 120, "renew_every_secs": 30})
    );
}

#[test]
fn a_port_not_served_is_refused() {
    assert!(check_port(PORT).is_ok());
    let mut d = descriptor();
    d.port = PORT + 1;
    let refused = d.validate().unwrap_err();
    assert_eq!(
        refused,
        Refused::Port {
            asked: PORT + 1,
            served: SERVED_PORTS.to_vec()
        }
    );
    assert!(refused.to_string().contains("not served"));
    assert!(check_port(0).is_err());
}

#[test]
fn a_malformed_descriptor_is_refused() {
    assert!(descriptor().validate().is_ok());
    type Change = Box<dyn Fn(&mut Descriptor)>;
    let cases: Vec<(&str, Change)> = vec![
        ("no version", Box::new(|d| d.engine_version = String::new())),
        (
            "padded version",
            Box::new(|d| d.engine_version = " 1.2 ".into()),
        ),
        ("no kinds", Box::new(|d| d.kinds.clear())),
        ("bad kind", Box::new(|d| d.kinds[0].name = "Load".into())),
        ("twice", Box::new(|d| d.kinds.push(d.kinds[0].clone()))),
        ("schema", Box::new(|d| d.kinds[0].parameters = json!(true))),
        ("idle", Box::new(|d| d.concurrency = 0)),
        ("greedy", Box::new(|d| d.concurrency = MAX_CONCURRENCY + 1)),
    ];
    for (name, change) in cases {
        let mut d = descriptor();
        change(&mut d);
        assert!(matches!(d.validate(), Err(Refused::Invalid(_))), "{name}");
    }
    assert!(descriptor().kind("load").is_some());
    assert!(descriptor().kind("other").is_none());
}

#[test]
fn unknown_fields_and_engine_names_are_refused_on_the_wire() {
    let mut wire = serde_json::to_value(descriptor()).unwrap();
    wire["extra"] = json!(1);
    assert!(serde_json::from_value::<Descriptor>(wire).is_err());
    let mut wire = serde_json::to_value(descriptor()).unwrap();
    wire["engine"] = json!("Not An Engine");
    assert!(serde_json::from_value::<Descriptor>(wire).is_err());
}
