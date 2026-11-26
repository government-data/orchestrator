//! Which machines the core lists as live (decision 0011, re-attach): a
//! cluster whose machine the core no longer lists is gone, and its flows
//! may be re-attached to a new cluster's inventory. Its HTTP adapter, on the
//! core's `GET /v1/machines` with the module's machine client token, comes
//! with [`crate::ModuleList`]'s.

use async_trait::async_trait;

/// The core's list of live machines.
#[async_trait]
pub trait MachineList: Send + Sync + 'static {
    /// The live machines' names (their certificates' CN); `None` when the
    /// core did not answer.
    async fn live(&self) -> Option<Vec<String>>;
}
