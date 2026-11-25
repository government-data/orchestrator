//! The rules document for grants on stdin (`{"core": […], "modules": […]}`),
//! on stdout: what the verification scripts give Trino.

use std::io::Read;

use trino_rules::{document, Grant};

#[derive(serde::Deserialize)]
struct Input {
    core: Vec<Grant>,
    #[serde(default)]
    modules: Vec<Grant>,
}

fn main() {
    let mut text = String::new();
    std::io::stdin()
        .read_to_string(&mut text)
        .expect("grants on stdin");
    let input: Input = serde_json::from_str(&text).expect("{\"core\": [...], \"modules\": [...]}");
    let (doc, invalid) = document(&input.core, &input.modules);
    for i in &invalid {
        eprintln!("left out: {}: {}", i.subject, i.why);
    }
    println!("{doc:#}");
}
