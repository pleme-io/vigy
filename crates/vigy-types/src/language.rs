use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "kebab-case")]
pub enum Language {
    #[default]
    TataraLisp,
    Blue,
}

impl Language {
    pub const ALL: [Language; 2] = [Language::TataraLisp, Language::Blue];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Language::TataraLisp => "tatara-lisp",
            Language::Blue => "blue",
        }
    }
}

impl std::fmt::Display for Language {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Language {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        Language::ALL
            .into_iter()
            .find(|l| l.as_str() == s)
            .ok_or_else(|| Error::UnknownLanguage {
                language: s.to_string(),
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_language_round_trips_through_its_name_and_serde() {
        for l in Language::ALL {
            assert_eq!(l.as_str().parse::<Language>().unwrap(), l);
            let json = serde_json::to_string(&l).unwrap();
            assert_eq!(json, format!("\"{}\"", l.as_str()));
            assert_eq!(serde_json::from_str::<Language>(&json).unwrap(), l);
        }
    }

    #[test]
    fn an_unknown_language_is_refused_by_name() {
        let err = "lua".parse::<Language>().unwrap_err().to_string();
        assert!(err.contains("lua"), "{err}");
    }

    #[test]
    fn the_default_is_tatara_lisp() {
        assert_eq!(Language::default(), Language::TataraLisp);
    }
}
