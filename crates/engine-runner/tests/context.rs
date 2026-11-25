//! What an engine reports through its run context.

use engine_port::{LogStream, MAX_LOG_LINE_BYTES};
use engine_runner::{Outcome, RunContext};
use orchestration_model::FinalState;

#[test]
fn outcomes_say_how_a_run_ended() {
    assert_eq!(Outcome::succeeded().state, FinalState::Succeeded);
    let failed = Outcome::failed("syntax");
    assert_eq!(
        (failed.state, failed.error_class.as_deref()),
        (FinalState::Failed, Some("syntax"))
    );
    assert_eq!(Outcome::cancelled().state, FinalState::Cancelled);
}

#[test]
fn a_context_takes_lines_progress_and_a_reference() {
    let ctx = RunContext::default();
    ctx.log(LogStream::Stdout, "one");
    ctx.log(LogStream::Stderr, &"é".repeat(MAX_LOG_LINE_BYTES));
    ctx.progress(Some(0.5), Some("half"));
    ctx.engine_ref("q-1");
    let debug = format!("{ctx:?}");
    assert!(debug.contains("q-1") && debug.contains("half"), "{debug}");
    assert!(debug.contains("seq: 2"), "numbered from 1: {debug}");
}
