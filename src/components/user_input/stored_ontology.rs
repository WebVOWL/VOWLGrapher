use leptos::prelude::*;
use leptos::server_fn::ServerFnError;
use leptos::server_fn::codec::Rkyv;
use std::fmt::Display;
use std::path::Path;
#[cfg(feature = "server")]
use vowlr_database::prelude::VOWLRStore;
use vowlr_util::prelude::VOWLRError;

#[derive(
    Debug,
    Copy,
    Clone,
    PartialEq,
    Eq,
    strum::EnumIter,
    rkyv::Archive,
    rkyv::Deserialize,
    rkyv::Serialize,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum StoredOntology {
    /// Friend of a Friend (FOAF) vocabulary.
    ///
    /// - Classes: 22
    /// - Size: 13 kB
    FriendOfAFriend,
    /// Clinical Trials Ontology (CTO).
    ///
    /// - Classes: 273
    /// - Size: 589 kB
    ClinicalTrialsOntology,
    /// Dummy data to benchmark render performance across visualization tools.
    ///
    /// - Classes: 2.5k
    /// - Size: 160 kB
    RenderingBenchmark,
    /// The Environment Ontology (ENVO).
    ///
    /// - Classes: 6.9k
    /// - Size: 10 MB
    EnvironmentOntology,
}

impl StoredOntology {
    pub const fn path(&self) -> &'static str {
        match self {
            Self::FriendOfAFriend => "src/assets/data/foaf.ttl",
            Self::ClinicalTrialsOntology => "src/assets/data/ClinicalTrialOntology-merged.owl",
            Self::RenderingBenchmark => "src/assets/data/vowlr-benchmark-2500.ofn",
            Self::EnvironmentOntology => "src/assets/data/envo.owl",
        }
    }
}

impl Display for StoredOntology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FriendOfAFriend => {
                write!(f, "Friend of a Friend (FOAF) vocabulary (22 classes)")
            }
            Self::ClinicalTrialsOntology => {
                write!(f, "Clinical Trials Ontology (CTO) (273 classes)")
            }
            Self::RenderingBenchmark => {
                write!(f, "Rendering Benchmark (2.5k classes)")
            }
            Self::EnvironmentOntology => {
                write!(f, "The Environment Ontology (6.9k classes)")
            }
        }
    }
}

impl TryFrom<&str> for StoredOntology {
    type Error = ServerFnError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value {
            "Friend of a Friend (FOAF) vocabulary (22 classes)" => Ok(Self::FriendOfAFriend),
            "Clinical Trials Ontology (CTO) (273 classes)" => Ok(Self::ClinicalTrialsOntology),
            "Rendering Benchmark (2.5k classes)" => Ok(Self::RenderingBenchmark),
            "The Environment Ontology (6.9k classes)" => Ok(Self::EnvironmentOntology),

            _ => Err(ServerFnError::ServerError(format!(
                "Unknown ontology: {value}"
            ))),
        }
    }
}

impl TryFrom<String> for StoredOntology {
    type Error = ServerFnError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.as_str().try_into()
    }
}

#[server(input = Rkyv, output = Rkyv)]
pub async fn load_stored_ontology(ontology: StoredOntology) -> Result<(), VOWLRError> {
    let path = Path::new(ontology.path());
    let store = VOWLRStore::default();

    store.insert_file(path, false).await?;
    Ok(())
}
