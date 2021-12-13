use std::collections::HashMap;
use std::sync::Arc;

use crate::cache::{cache_to_arc, create_cache, ContractCache};
use crate::ViewResult;
use chrono::{TimeZone, Utc};
use near_crypto::{InMemorySigner, KeyType, PublicKey, Signer};
use near_pool::{types::PoolIterator, TransactionPool};
use near_primitives::account::{AccessKey, Account};
use near_primitives::errors::RuntimeError;
use near_primitives::hash::CryptoHash;
use near_primitives::profile::ProfileData;
use near_primitives::receipt::Receipt;
use near_primitives::runtime::config::RuntimeConfig;
use near_primitives::runtime::config_store::RuntimeConfigStore;
use near_primitives::runtime::migration_data::{MigrationData, MigrationFlags};
use near_primitives::state_record::{self, StateRecord};
use near_primitives::test_utils::account_new;
use near_primitives::test_utils::MockEpochInfoProvider;
use near_primitives::transaction::{
    ExecutionMetadata, ExecutionOutcome, ExecutionStatus, SignedTransaction,
};
use near_primitives::types::{
    AccountInfo, Balance, BlockHeight, EpochHeight, EpochId, EpochInfoProvider, Gas,
    StateChangeCause,
};
use near_primitives::version::PROTOCOL_VERSION;
use near_primitives::views::ViewApplyState;
use near_runtime::{state_viewer::TrieViewer, ApplyState, Runtime};
use near_sdk::{AccountId, Duration};
use near_store::{
    get_access_key, get_account, set_account, test_utils::create_test_store, ShardTries, Store,
};

const DEFAULT_EPOCH_LENGTH: u64 = 3;
const DEFAULT_BLOCK_PROD_TIME: Duration = 1_000_000_000;

pub fn init_runtime(
    genesis_config: Option<GenesisConfig>,
) -> (RuntimeStandalone, InMemorySigner, AccountId) {
    let mut genesis = genesis_config.unwrap_or_default();
    genesis.runtime_config.wasm_config.limit_config.max_total_prepaid_gas = genesis.gas_limit;
    let root_account_id: AccountId = AccountId::new_unchecked("root".to_string());
    let signer = genesis.init_root_signer(root_account_id.as_str());
    let runtime = RuntimeStandalone::new_with_store(genesis);
    (runtime, signer, root_account_id)
}

#[derive(Debug)]
pub struct GenesisConfig {
    pub genesis_time: u64,
    pub gas_price: Balance,
    pub gas_limit: Gas,
    pub genesis_height: u64,
    pub epoch_length: u64,
    pub block_prod_time: Duration,
    pub runtime_config: RuntimeConfig,
    pub state_records: Vec<StateRecord>,
    pub validators: Vec<AccountInfo>,
}

impl Default for GenesisConfig {
    fn default() -> Self {
        let runtime_config = RuntimeConfigStore::new(None)
            .get_config(PROTOCOL_VERSION)
            .as_ref()
            .clone();
        Self {
            genesis_time: 0,
            gas_price: 100_000_000,
            gas_limit: runtime_config.wasm_config.limit_config.max_total_prepaid_gas,
            genesis_height: 1,
            epoch_length: DEFAULT_EPOCH_LENGTH,
            block_prod_time: DEFAULT_BLOCK_PROD_TIME,
            runtime_config,
            state_records: vec![],
            validators: vec![],
        }
    }
}

impl GenesisConfig {
    pub fn init_root_signer(&mut self, account_id: &str) -> InMemorySigner {
        let account_id: near_primitives::types::AccountId = account_id.parse().unwrap();
        let signer = InMemorySigner::from_seed(account_id.clone(), KeyType::ED25519, "test");
        let root_account = account_new(10u128.pow(33), CryptoHash::default());

        self.state_records
            .push(StateRecord::Account { account_id: account_id.clone(), account: root_account });
        self.state_records.push(StateRecord::AccessKey {
            account_id: account_id.clone(),
            public_key: signer.public_key(),
            access_key: AccessKey::full_access(),
        });
        signer
    }

    pub fn genesis(&self) -> near_chain_configs::Genesis {
        let mut genesis_config: near_chain_configs::GenesisConfig =
            near_chain_configs::GenesisConfig::default();
        genesis_config.genesis_time = Utc.timestamp_nanos(self.genesis_time as i64);
        genesis_config.gas_limit = self.gas_limit;
        genesis_config.genesis_height = self.genesis_height;
        genesis_config.epoch_length = self.epoch_length;
        genesis_config.num_blocks_per_year =
            (365 * 24 * 3600 * 1_000_000_000) / self.block_prod_time;
        genesis_config.num_block_producer_seats = self.validators.len() as u64;
        genesis_config.num_block_producer_seats_per_shard =
            vec![genesis_config.num_block_producer_seats];
        genesis_config.validators = self.validators.clone();

        near_chain_configs::Genesis {
            config: genesis_config,
            records: near_chain_configs::GenesisRecords(self.state_records.clone()),
            records_file: Default::default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Block {
    prev_block: Option<Arc<Block>>,
    state_root: CryptoHash,
    gas_burnt: Gas,
    pub epoch_height: EpochHeight,
    pub block_height: BlockHeight,
    pub block_timestamp: u64,
    pub gas_price: Balance,
    pub gas_limit: Gas,
}

impl Drop for Block {
    fn drop(&mut self) {
        // Blocks form a liked list, so the generated recursive drop overflows
        // the stack. Let's use an explicit loop to avoid that.
        let mut curr = self.prev_block.take();
        while let Some(mut next) = curr.and_then(|it| Arc::try_unwrap(it).ok()) {
            curr = next.prev_block.take();
        }
    }
}

impl Block {
    pub fn genesis(genesis_config: &GenesisConfig) -> Self {
        Self {
            prev_block: None,
            state_root: CryptoHash::default(),
            block_height: genesis_config.genesis_height,
            epoch_height: 0,
            block_timestamp: genesis_config.genesis_time,
            gas_price: genesis_config.gas_price,
            gas_limit: genesis_config.gas_limit,
            gas_burnt: 0,
        }
    }

    fn produce(
        &self,
        new_state_root: CryptoHash,
        epoch_length: u64,
        block_prod_time: Duration,
    ) -> Block {
        Self {
            gas_price: self.gas_price,
            gas_limit: self.gas_limit,
            block_timestamp: self.block_timestamp + block_prod_time,
            prev_block: Some(Arc::new(self.clone())),
            state_root: new_state_root,
            block_height: self.block_height + 1,
            epoch_height: (self.block_height + 1) / epoch_length,
            gas_burnt: 0,
        }
    }
}

pub struct RuntimeStandalone {
    pub genesis: GenesisConfig,
    tx_pool: TransactionPool,
    transactions: HashMap<CryptoHash, SignedTransaction>,
    outcomes: HashMap<CryptoHash, ExecutionOutcome>,
    profile: HashMap<CryptoHash, ProfileData>,
    pub cur_block: Block,
    runtime: Runtime,
    tries: ShardTries,
    pending_receipts: Vec<Receipt>,
    epoch_info_provider: Box<dyn EpochInfoProvider>,
    pub last_outcomes: Vec<CryptoHash>,
    cache: ContractCache,
}

impl RuntimeStandalone {
    pub fn new(genesis: GenesisConfig, store: Arc<Store>) -> Self {
        let mut genesis_block = Block::genesis(&genesis);
        let runtime = Runtime::new();
        let tries = ShardTries::new(store, 0, 1);
        let state_root = runtime.apply_genesis_state(
            tries.clone(),
            0,
            &[],
            &genesis.genesis(),
            &genesis.runtime_config,
            genesis
                .state_records
                .iter()
                .map(|s| state_record::state_record_to_account_id(s))
                .cloned()
                .collect(),
        );
        genesis_block.state_root = state_root;
        let validators = genesis.validators.clone();
        Self {
            genesis,
            tries,
            runtime,
            transactions: HashMap::new(),
            outcomes: HashMap::new(),
            profile: HashMap::new(),
            cur_block: genesis_block,
            tx_pool: TransactionPool::new(Default::default()),
            pending_receipts: vec![],
            epoch_info_provider: Box::new(MockEpochInfoProvider::new(
                validators.into_iter().map(|info| (info.account_id, info.amount)),
            )),
            cache: create_cache(),
            last_outcomes: vec![],
        }
    }

    pub fn new_with_store(genesis: GenesisConfig) -> Self {
        RuntimeStandalone::new(genesis, create_test_store())
    }

    /// Processes blocks until the final value is produced
    pub fn resolve_tx(
        &mut self,
        mut tx: SignedTransaction,
    ) -> Result<(CryptoHash, ExecutionOutcome), RuntimeError> {
        tx.init();
        let mut outcome_hash = tx.get_hash();
        self.transactions.insert(outcome_hash, tx.clone());
        self.tx_pool.insert_transaction(tx);
        self.last_outcomes = vec![];
        loop {
            self.produce_block()?;
            if let Some(outcome) = self.outcomes.get(&outcome_hash) {
                match outcome.status {
                    ExecutionStatus::Unknown => unreachable!(), // ExecutionStatus::Unknown is not relevant for a standalone runtime
                    ExecutionStatus::SuccessReceiptId(ref id) => outcome_hash = *id,
                    ExecutionStatus::SuccessValue(_) | ExecutionStatus::Failure(_) => {
                        return Ok((outcome_hash, outcome.clone()))
                    }
                };
            } else if self.pending_receipts.is_empty() {
                unreachable!("Lost an outcome for the receipt hash {}", outcome_hash);
            }
        }
    }

    /// Just puts tx into the transaction pool
    pub fn send_tx(&mut self, tx: SignedTransaction) -> CryptoHash {
        let tx_hash = tx.get_hash();
        self.transactions.insert(tx_hash, tx.clone());
        self.tx_pool.insert_transaction(tx);
        tx_hash
    }

    pub fn outcome(&self, hash: &CryptoHash) -> Option<ExecutionOutcome> {
        self.outcomes.get(hash).cloned()
    }

    pub fn profile_of_outcome(&self, hash: &CryptoHash) -> Option<ProfileData> {
        match self.profile.get(hash) {
            Some(p) => Some(p.clone()),
            _ => None,
        }
    }

    /// Processes all transactions and pending receipts until there is no pending_receipts left
    pub fn process_all(&mut self) -> Result<(), RuntimeError> {
        loop {
            self.produce_block()?;
            if self.pending_receipts.is_empty() {
                return Ok(());
            }
        }
    }

    /// Processes one block. Populates outcomes and producining new pending_receipts.
    pub fn produce_block(&mut self) -> Result<(), RuntimeError> {
        let apply_state = ApplyState {
            block_index: self.cur_block.block_height,
            prev_block_hash: Default::default(),
            epoch_height: self.cur_block.epoch_height,
            gas_price: self.cur_block.gas_price,
            block_timestamp: self.cur_block.block_timestamp,
            gas_limit: None,
            // not used
            random_seed: Default::default(),
            epoch_id: EpochId::default(),
            current_protocol_version: PROTOCOL_VERSION,
            config: Arc::new(self.genesis.runtime_config.clone()),
            #[cfg(feature = "no_contract_cache")]
            cache: None,
            #[cfg(not(feature = "no_contract_cache"))]
            cache: Some(cache_to_arc(&self.cache)),
            block_hash: Default::default(),
            is_new_chunk: true,
            migration_data: Arc::new(MigrationData::default()),
            migration_flags: MigrationFlags::default(),
        };

        let shard_uid = as_shard_uid(0);
        let apply_result = self.runtime.apply(
            self.tries.get_trie_for_shard(shard_uid),
            self.cur_block.state_root,
            &None,
            &apply_state,
            &self.pending_receipts,
            &Self::prepare_transactions(&mut self.tx_pool),
            self.epoch_info_provider.as_ref(),
            None,
        )?;
        self.pending_receipts = apply_result.outgoing_receipts;
        apply_result.outcomes.iter().for_each(|outcome| {
            self.last_outcomes.push(outcome.id);
            self.outcomes.insert(outcome.id, outcome.outcome.clone());
            // purposely not using `if let` to take advantage of exhaustiveness check
            match &outcome.outcome.metadata {
                ExecutionMetadata::V2(profile) => {
                    self.profile.insert(outcome.id, profile.clone());
                }
                ExecutionMetadata::V1 => (),
            };
        });
        let (update, _) = self
            .tries
            .apply_all(&apply_result.trie_changes, shard_uid)
            .expect("Unexpected Storage error");
        update.commit().expect("Unexpected io error");
        self.cur_block = self.cur_block.produce(
            apply_result.state_root,
            self.genesis.epoch_length,
            self.genesis.block_prod_time,
        );

        Ok(())
    }

    /// Produce num_of_blocks blocks.
    /// # Examples
    ///
    /// ```
    /// use near_sdk_sim::runtime::init_runtime;
    /// let (mut runtime, _, _) = init_runtime(None);
    /// runtime.produce_blocks(5);
    /// // note: genesis height is 1
    /// assert_eq!(runtime.current_block().block_height, 6);
    /// assert_eq!(runtime.current_block().epoch_height, 2);
    ///```

    pub fn produce_blocks(&mut self, num_of_blocks: u64) -> Result<(), RuntimeError> {
        for _ in 0..num_of_blocks {
            self.produce_block()?;
        }
        Ok(())
    }

    /// Force alter account and change state_root.
    pub fn force_account_update(&mut self, account_id: AccountId, account: &Account) {
        let account_id = crate::to_near_account_id(account_id);
        let shard_uid = as_shard_uid(0);
        let mut trie_update = self.tries.new_trie_update(shard_uid, self.cur_block.state_root);
        set_account(&mut trie_update, account_id, account);
        trie_update.commit(StateChangeCause::ValidatorAccountsUpdate);
        let (trie_changes, _) = trie_update.finalize().expect("Unexpected Storage error");
        let (store_update, new_root) = self.tries.apply_all(&trie_changes, shard_uid).unwrap();
        store_update.commit().expect("No io errors expected");
        self.cur_block.state_root = new_root;
    }

    pub fn view_account(&self, account_id: &str) -> Option<Account> {
        let account_id = crate::to_near_account_id(account_id);
        let shard_uid = as_shard_uid(0);
        let trie_update = self.tries.new_trie_update(shard_uid, self.cur_block.state_root);
        get_account(&trie_update, &account_id).expect("Unexpected Storage error")
    }

    pub fn view_access_key(&self, account_id: &str, public_key: &PublicKey) -> Option<AccessKey> {
        let account_id = crate::to_near_account_id(account_id);
        let shard_uid = as_shard_uid(0);
        let trie_update = self.tries.new_trie_update(shard_uid, self.cur_block.state_root);
        get_access_key(&trie_update, &account_id, public_key).expect("Unexpected Storage error")
    }

    /// Returns a ViewResult containing the value or error and any logs
    pub fn view_method_call(&self, account_id: &str, method_name: &str, args: &[u8]) -> ViewResult {
        let account_id = crate::to_near_account_id(account_id);
        let shard_uid = as_shard_uid(0);
        let trie_update = self.tries.new_trie_update(shard_uid, self.cur_block.state_root);
        let viewer = TrieViewer::default();
        let mut logs = vec![];
        let view_state = ViewApplyState {
            block_height: self.cur_block.block_height,
            prev_block_hash: self.cur_block.prev_block.as_ref().unwrap().state_root,
            epoch_id: EpochId::default(),
            epoch_height: self.cur_block.epoch_height,
            block_timestamp: self.cur_block.block_timestamp,
            current_protocol_version: PROTOCOL_VERSION,
            cache: Some(cache_to_arc(&self.cache)),
            block_hash: self.cur_block.state_root,
        };
        let result = viewer.call_function(
            trie_update,
            view_state,
            &account_id,
            method_name,
            args,
            &mut logs,
            self.epoch_info_provider.as_ref(),
        );
        ViewResult::new(result, logs)
    }

    /// Returns a reference to the current block.
    ///
    /// # Examples
    /// ```
    /// use near_sdk_sim::runtime::init_runtime;
    /// let (mut runtime, _, _) = init_runtime(None);
    /// runtime.produce_block().unwrap();
    /// runtime.current_block();
    /// assert_eq!(runtime.current_block().block_height, 2);
    /// runtime.produce_blocks(4).unwrap();
    /// assert_eq!(runtime.current_block().block_height, 6);
    /// ```
    pub fn current_block(&self) -> &Block {
        &self.cur_block
    }

    pub fn pending_receipts(&self) -> &[Receipt] {
        &self.pending_receipts
    }

    fn prepare_transactions(tx_pool: &mut TransactionPool) -> Vec<SignedTransaction> {
        let mut res = vec![];
        let mut pool_iter = tx_pool.pool_iterator();
        while let Some(iter) = pool_iter.next() {
            if let Some(tx) = iter.next() {
                res.push(tx);
            }
        }
        res
    }
}

fn as_shard_uid(id: u32) -> near_primitives::shard_layout::ShardUId {
    near_primitives::shard_layout::ShardUId { version: 0, shard_id: id }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::to_yocto;

    struct Foo {}

    impl Foo {
        fn _private(&self) {
            print!("yay!")
        }
    }

    #[test]
    fn single_test() {
        let foo = Foo {};
        foo._private();
    }

    #[test]
    fn single_block() {
        let (mut runtime, signer, _) = init_runtime(None);
        let hash = runtime.send_tx(SignedTransaction::create_account(
            1,
            signer.account_id.clone(),
            "alice".parse().unwrap(),
            100,
            signer.public_key(),
            &signer,
            CryptoHash::default(),
        ));
        runtime.produce_block().unwrap();
        assert!(matches!(
            runtime.outcome(&hash),
            Some(ExecutionOutcome { status: ExecutionStatus::SuccessReceiptId(_), .. })
        ));
    }

    #[test]
    fn process_all() {
        let (mut runtime, signer, _) = init_runtime(None);
        const ACCOUNT: &str = "alice.root";
        assert_eq!(runtime.view_account(ACCOUNT), None);
        let outcome = runtime.resolve_tx(SignedTransaction::create_account(
            1,
            signer.account_id.clone(),
            ACCOUNT.parse().unwrap(),
            165437999999999999999000,
            signer.public_key(),
            &signer,
            CryptoHash::default(),
        ));
        assert!(matches!(
            outcome,
            Ok((_, ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. }))
        ));
        assert_eq!(
            runtime.view_account(ACCOUNT),
            Some(Account::new(165437999999999999999000, 0, CryptoHash::default(), 182,))
        );
    }

    #[test]
    fn test_cross_contract_call() {
        let (mut runtime, signer, _) = init_runtime(None);

        assert!(matches!(
            runtime.resolve_tx(SignedTransaction::create_contract(
                1,
                signer.account_id.clone(),
                "status.root".parse().unwrap(),
                include_bytes!("../../examples/status-message/res/status_message.wasm")
                    .as_ref()
                    .into(),
                to_yocto("35"),
                signer.public_key(),
                &signer,
                CryptoHash::default(),
            )),
            Ok((_, ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. }))
        ));
        let res = runtime.resolve_tx(SignedTransaction::create_contract(
            2,
            signer.account_id.clone(),
            "caller.root".parse().unwrap(),
            include_bytes!(
                "../../examples/cross-contract-high-level/res/cross_contract_high_level.wasm"
            )
            .as_ref()
            .into(),
            to_yocto("35"),
            signer.public_key(),
            &signer,
            CryptoHash::default(),
        ));
        assert!(matches!(
            res,
            Ok((_, ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. }))
        ));
        let res = runtime.resolve_tx(SignedTransaction::call(
            3,
            signer.account_id.clone(),
            "caller.root".parse().unwrap(),
            &signer,
            0,
            "simple_call".into(),
            "{\"account_id\": \"status.root\", \"message\": \"caller status is ok!\"}"
                .as_bytes()
                .to_vec(),
            300_000_000_000_000,
            CryptoHash::default(),
        ));
        let (_, res) = res.unwrap();
        runtime.process_all().unwrap();

        assert!(matches!(res, ExecutionOutcome { status: ExecutionStatus::SuccessValue(_), .. }));
        let res = runtime.view_method_call(
            &"status.root",
            "get_status",
            "{\"account_id\": \"root\"}".as_bytes(),
        );

        let caller_status = String::from_utf8(res.unwrap()).unwrap();
        assert_eq!("\"caller status is ok!\"", caller_status);
    }

    #[test]
    fn test_force_update_account() {
        let (mut runtime, _, _) = init_runtime(None);
        let mut bob_account = runtime.view_account(&"root").unwrap();
        bob_account = set_locked(bob_account, 10000);
        runtime.force_account_update("root".parse().unwrap(), &bob_account);
        assert_eq!(runtime.view_account(&"root").unwrap().locked(), 10000);
    }

    #[test]
    fn can_produce_many_blocks_without_stack_overflow() {
        let (mut runtime, _signer, _) = init_runtime(None);
        runtime.produce_blocks(20_000).unwrap();
    }

    fn set_locked(account: Account, locked: Balance) -> Account {
        Account::new(account.amount(), locked, account.code_hash(), account.storage_usage())
    }
}
