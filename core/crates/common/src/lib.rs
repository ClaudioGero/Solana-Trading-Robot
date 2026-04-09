use anyhow::{Context, Result};
use serde::de::DeserializeOwned;
use std::{fs, path::Path};

pub mod format;
pub mod jupiter;
pub mod token_info;

pub fn read_json_file<T: DeserializeOwned>(path: impl AsRef<Path>) -> Result<T> {
    let path = path.as_ref();
    let bytes =
        fs::read(path).with_context(|| format!("failed reading file: {}", path.display()))?;
    let v = serde_json::from_slice::<T>(&bytes)
        .with_context(|| format!("failed parsing json: {}", path.display()))?;
    Ok(v)
}
