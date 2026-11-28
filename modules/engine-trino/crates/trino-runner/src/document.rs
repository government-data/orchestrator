//! An entity lookup's typed document (core decision 0090, rulings 81, 85
//! and 102): the one `text` placeholder of an `entity_key` bundle, which no
//! answer fills. The runner asks the run's document route once per
//! attempt (the core gives it once, then answers it is gone) and writes it
//! into the lookup's values read as one quoted SQL literal, which Trino
//! hashes with the findings key. It is held only while the run runs:
//! never in a log, the progress detail, the report, the metrics, an error
//! or a `Debug`.

use std::collections::BTreeMap;

use engine_runner::Outcome;

use crate::bundle::{Bundle, PlaceholderType};
use crate::engine::{transient, TrinoEngine};
use crate::ports::{Core, Trino};

/// The only kind whose bundle may carry a `text` placeholder.
pub(crate) const ENTITY_KEY: &str = "entity_key";

/// The longest document the core takes, in characters.
const MAX_CHARS: usize = 64;

/// A person's typed document, as the core's document route gives it.
#[derive(Clone, PartialEq, Eq)]
pub struct Document(String);

impl Document {
    #[must_use]
    pub fn new(text: String) -> Self {
        Self(text)
    }

    /// The document as one SQL string literal, its quotes doubled; `None`
    /// unless it is 1 to 64 characters and none a control character, as
    /// the core takes it.
    fn literal(&self) -> Option<String> {
        let n = self.0.chars().count();
        let ok = (1..=MAX_CHARS).contains(&n) && !self.0.chars().any(char::is_control);
        ok.then(|| format!("'{}'", self.0.replace('\'', "''")))
    }
}

impl std::fmt::Debug for Document {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Document(..)")
    }
}

/// The `text` placeholders' literals by name, for this attempt only.
#[derive(Clone, Default, PartialEq, Eq)]
pub(crate) struct Texts(BTreeMap<String, String>);

impl Texts {
    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.0.get(name).map(String::as_str)
    }
}

impl std::fmt::Debug for Texts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_set().entries(self.0.keys()).finish()
    }
}

impl<C: Core, T: Trino> TrinoEngine<C, T> {
    /// The literals of `bundle`'s `text` placeholders: the run's document,
    /// asked once, for each. Nothing is asked for a bundle without one.
    pub(crate) async fn texts(&self, job: &str, bundle: &Bundle) -> Result<Texts, Outcome> {
        let names: Vec<&str> = bundle
            .placeholders
            .iter()
            .filter(|p| p.kind == PlaceholderType::Text)
            .map(|p| p.name.as_str())
            .collect();
        if names.is_empty() {
            return Ok(Texts::default());
        }
        let document = match self.core.document(job).await {
            Ok(Some(d)) => d,
            // Given once already: a repeated attempt cannot look it up.
            Ok(None) => return Err(Outcome::failed("document_gone")),
            Err(_) => return Err(transient("core_unavailable")),
        };
        let literal = document
            .literal()
            .ok_or_else(|| Outcome::failed("document_invalid"))?;
        Ok(Texts(
            names
                .into_iter()
                .map(|n| (n.to_string(), literal.clone()))
                .collect(),
        ))
    }
}
