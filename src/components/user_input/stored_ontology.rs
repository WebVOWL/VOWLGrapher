use leptos::prelude::*;
use leptos::server_fn::ServerFnError;
use leptos::server_fn::codec::Rkyv;
use std::fmt::Display;
use std::path::Path;
#[cfg(feature = "server")]
use vowlgrapher_database::prelude::VOWLGrapherStore;
use vowlgrapher_util::prelude::VOWLGrapherError;
#[cfg(feature = "ssr")]
use vowlgrapher_util::prelude::manage_user_id;

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
    /// - Classes: 13
    /// - Size: 23 kB
    FriendOfAFriend,
    /// Ontology Visualization Benchmark (OntoViBe).
    ///
    /// - Classes 43
    /// - Size: 13 kB
    OntoViBe,
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
            Self::FriendOfAFriend => "target/site/data/foaf.ttl",
            Self::OntoViBe => "target/site/data/ontovibe.ttl",
            Self::ClinicalTrialsOntology => "target/site/data/ClinicalTrialOntology-merged.owl",
            Self::RenderingBenchmark => "target/site/data/vowlgrapher-benchmark-2500.ofn",
            Self::EnvironmentOntology => "target/site/data/envo.owl",
        }
    }
}

impl Display for StoredOntology {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FriendOfAFriend => {
                write!(f, "Friend of a Friend (FOAF) vocabulary (13 classes)")
            }
            Self::OntoViBe => {
                write!(
                    f,
                    "Ontology Visualization Benchmark (OntoViBe) (43 classes)"
                )
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
            "Friend of a Friend (FOAF) vocabulary (13 classes)" => Ok(Self::FriendOfAFriend),
            "Ontology Visualization Benchmark (OntoViBe) (43 classes)" => Ok(Self::OntoViBe),
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
pub async fn load_stored_ontology(
    ontology: StoredOntology,
) -> Result<Option<VOWLGrapherError>, VOWLGrapherError> {
    let path = Path::new(ontology.path());
    let store = VOWLGrapherStore::new_for_user(manage_user_id().await?);

    let warnings = store.insert_file(path, false).await?;
    Ok(warnings)
}
