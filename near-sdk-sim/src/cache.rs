use crate::types::{CompiledContract, CompiledContractCache};
use near_primitives::borsh::{BorshDeserialize, BorshSerialize};
use near_primitives::hash::CryptoHash;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// This provides a disc cache for compiled contracts.
/// The cached contracts are located `CARGO_MANIFEST_DIR/target/contract_cache`.
#[derive(Clone, Default)]
pub struct ContractCache {
    data: Arc<Mutex<HashMap<Vec<u8>, CompiledContract>>>,
}

pub(crate) fn key_to_b58(key: &[u8]) -> String {
    near_sdk::bs58::encode(key).into_string()
}

impl ContractCache {
    pub fn new() -> Self {
        ContractCache::default()
    }

    fn path() -> PathBuf {
        let s = std::env::var("CARGO_MANIFEST_DIR").unwrap().to_string();
        Path::new(&s).join("target").join("contract_cache")
    }

    fn open_file(&self, key: &[u8]) -> std::io::Result<File> {
        let path = self.get_path(key);
        // Ensure that the parent path exists
        let prefix = path.parent().unwrap();
        std::fs::create_dir_all(prefix).unwrap();
        // Ensure we can read, write, and create file if it doesn't exist
        OpenOptions::new().read(true).write(true).create(true).open(path)
    }

    fn get_path(&self, key: &[u8]) -> PathBuf {
        ContractCache::path().join(key_to_b58(key))
    }

    fn file_exists(&self, key: &[u8]) -> bool {
        self.get_path(key).exists()
    }

    pub fn insert(&self, key: &[u8], value: &CompiledContract) -> Option<CompiledContract> {
        self.data.lock().unwrap().insert(key.to_vec(), value.clone())
    }

    pub fn get(&self, key: &[u8]) -> Option<CompiledContract> {
        self.data.lock().unwrap().get(key).cloned()
    }

    #[allow(dead_code)]
    pub(crate) fn to_box(&self) -> Box<ContractCache> {
        Box::new(self.clone())
    }
}

impl CompiledContractCache for ContractCache {
    fn put(&self, key: &CryptoHash, value: CompiledContract) -> Result<(), std::io::Error> {
        let key: &[u8] = key.as_ref();
        self.insert(key, &value);
        let mut file = self.open_file(key).expect("File failed to open");
        let metadata = file.metadata()?;
        let serialized = value.try_to_vec()?;
        if metadata.len() != serialized.len() as u64 {
            file.write_all(&serialized)?;
        }
        Ok(())
    }

    fn get(&self, key: &CryptoHash) -> Result<Option<CompiledContract>, std::io::Error> {
        let key: &[u8] = key.as_ref();
        if (*self.data).lock().unwrap().contains_key(key) {
            return Ok(self.get(key));
        } else if self.file_exists(key) {
            let mut file = self.open_file(key)?;
            let mut contents = vec![];
            file.read_to_end(&mut contents)?;
            let value = CompiledContract::try_from_slice(&contents)?;
            self.insert(key, &value);
            return Ok(Some(value));
        }
        Ok(None)
    }
}

pub fn create_cache() -> ContractCache {
    ContractCache::new()
}

pub fn cache_to_box(cache: &ContractCache) -> Box<ContractCache> {
    cache.to_box()
}
