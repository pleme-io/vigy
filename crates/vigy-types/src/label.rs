//! Operator-attached metadata. Kubernetes-style label conventions
//! (DNS subdomain prefix + suffix) so existing tooling habits transfer.

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use utoipa::ToSchema;

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(transparent)]
pub struct Labels(BTreeMap<String, String>);

impl Labels {
    pub fn new() -> Self {
        Self(BTreeMap::new())
    }

    pub fn insert(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Result<&mut Self> {
        let key = key.into();
        validate_key(&key)?;
        self.0.insert(key, value.into());
        Ok(self)
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(|s| s.as_str())
    }

    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.0.iter()
    }

    pub fn matches_selector(&self, selector: &str) -> Result<bool> {
        // Minimal selector: comma-separated `k=v` (AND).
        // Future: `k in (v1,v2)`, `k!=v`, presence-only `k`.
        for piece in selector.split(',').map(|s| s.trim()).filter(|s| !s.is_empty()) {
            let Some((k, v)) = piece.split_once('=') else {
                return Err(Error::InvalidLabelSelector {
                    selector: selector.to_string(),
                    reason: "expected key=value",
                });
            };
            if self.0.get(k).map(|x| x.as_str()) != Some(v) {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl From<BTreeMap<String, String>> for Labels {
    fn from(m: BTreeMap<String, String>) -> Self {
        Self(m)
    }
}

fn validate_key(k: &str) -> Result<()> {
    if k.is_empty() || k.len() > 253 {
        return Err(Error::InvalidLabelKey {
            key: k.to_string(),
            reason: "must be 1..=253 chars",
        });
    }
    if !k
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_' || c == '.' || c == '/')
    {
        return Err(Error::InvalidLabelKey {
            key: k.to_string(),
            reason: "only [A-Za-z0-9-_./] allowed",
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_validates_key() {
        let mut l = Labels::new();
        assert!(l.insert("host", "mado").is_ok());
        assert!(l.insert("scope.io/v1", "tear-sync").is_ok());
        assert!(l.insert("", "x").is_err());
        assert!(l.insert("bad key", "x").is_err());
    }

    #[test]
    fn selector_matches_and_clauses() {
        let mut l = Labels::new();
        l.insert("host", "mado").unwrap();
        l.insert("scope", "tear-sync").unwrap();
        assert!(l.matches_selector("host=mado").unwrap());
        assert!(l.matches_selector("host=mado,scope=tear-sync").unwrap());
        assert!(!l.matches_selector("host=tear").unwrap());
        assert!(!l.matches_selector("missing=x").unwrap());
    }

    #[test]
    fn selector_rejects_malformed() {
        let l = Labels::new();
        assert!(l.matches_selector("no-equals").is_err());
    }
}
