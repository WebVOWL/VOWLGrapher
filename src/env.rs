//! This module defines all environment variables available in ``VOWLGrapher``
//!
//! They are supplied at runtime.

use std::env::var;
use std::fmt::Debug;
use std::str::FromStr;
#[cfg(feature = "server")]
use std::sync::LazyLock;

use bytesize::ByteSize;
use leptos::prelude::*;
use leptos::server_fn::codec::Rkyv;
use log::warn;

/// Server-side access to environment variables.
#[cfg(feature = "server")]
pub static VOWLGRAPHER_ENVIRONMENT: LazyLock<VOWLGrapherEnviron> =
    LazyLock::new(VOWLGrapherEnviron::new);

/// Client-side access to environment variables.
#[server(input = Rkyv, output = Rkyv)]
pub async fn environ() -> Result<VOWLGrapherEnviron, ServerFnError> {
    Ok(*VOWLGRAPHER_ENVIRONMENT)
}

/// Wrapper type to remotely derive impls of rkyv and serde.
#[derive(
    rkyv::Archive, rkyv::Deserialize, rkyv::Serialize, serde::Deserialize, serde::Serialize,
)]
#[rkyv(remote = ByteSize)]
#[rkyv(archived = ArchivedByteSize)]
#[serde(remote = "ByteSize")]
struct SerilizableByteSize(pub u64);

impl From<SerilizableByteSize> for ByteSize {
    fn from(value: SerilizableByteSize) -> Self {
        Self(value.0)
    }
}

/// Encapsulates all environment variables available in ``VOWLGrapher``.
///
/// The environment variables are gathered at runtime.
///
/// If a variable is not found in the environment, a default value is used instead.
#[repr(C)]
#[derive(
    rkyv::Archive,
    rkyv::Deserialize,
    rkyv::Serialize,
    serde::Deserialize,
    serde::Serialize,
    Debug,
    Copy,
    Clone,
)]
pub struct VOWLGrapherEnviron {
    /// The maximum allowed size, in bytes, of any input into ``VOWLGrapher``.
    #[rkyv(with = SerilizableByteSize)]
    #[serde(with = "SerilizableByteSize")]
    pub max_input_size_bytes: ByteSize,
}

impl VOWLGrapherEnviron {
    pub fn new() -> Self {
        // Default is 50 MB
        let max_input_size_bytes =
            Self::parse_environment("VOWLGRAPHER_MAX_INPUT_SIZE_BYTES", ByteSize::mb(50));

        Self {
            max_input_size_bytes,
        }
    }

    /// Returns the value of key from the environment, if found, otherwise returns the provided default.
    fn parse_environment<T, K>(key: &K, default: T) -> T
    where
        K: ToString + ?Sized,
        T: FromStr + ToString,
        <T as FromStr>::Err: Debug,
    {
        var(key.to_string())
            .unwrap_or_else(|_| {
                warn!(
                    "Did not find variable {} in environment. Using default '{}'",
                    key.to_string(),
                    default.to_string()
                );
                default.to_string()
            })
            .parse::<T>()
            .unwrap_or_else(|e| {
                warn!(
                    "Failed to parse value for environment variable {}: {:#?}. Using default '{}'",
                    key.to_string(),
                    e,
                    default.to_string()
                );
                default
            })
    }
}

impl Default for VOWLGrapherEnviron {
    fn default() -> Self {
        Self::new()
    }
}
