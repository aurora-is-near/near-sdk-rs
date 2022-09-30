use near_primitives_core::runtime::fees::RuntimeFeesConfig;
use near_vm_logic::{VMConfig, ViewConfig};

use crate::environment::mocked_blockchain::MockedBlockchain;
use crate::test_utils::test_env::*;
use crate::AccountId;
use crate::{
    Balance, BlockHeight, EpochHeight, Gas, PromiseResult, PublicKey, StorageUsage, VMContext,
};

/// Returns a pre-defined account_id from a list of 6.
pub fn accounts(id: usize) -> AccountId {
    AccountId::new_unchecked(
        ["alice", "bob", "charlie", "danny", "eugene", "fargo"][id].to_string(),
    )
}

/// Simple VMContext builder that allows to quickly create custom context in tests.
#[derive(Clone)]
pub struct VMContextBuilder {
    pub context: VMContext,
}

fn convert_account_id(a: AccountId) -> near_primitives_core::account::id::AccountId {
    a.as_ref().parse().unwrap()
}

#[allow(dead_code)]
impl VMContextBuilder {
    pub fn new() -> Self {
        Self {
            context: VMContext {
                current_account_id: convert_account_id(alice()),
                signer_account_id: convert_account_id(bob()),
                signer_account_pk: vec![0u8; 32],
                predecessor_account_id: convert_account_id(bob()),
                input: vec![],
                block_index: 0,
                block_timestamp: 0,
                epoch_height: 0,
                account_balance: 10u128.pow(26),
                account_locked_balance: 0,
                storage_usage: 1024 * 300,
                attached_deposit: 0,
                prepaid_gas: 300 * 10u64.pow(12),
                random_seed: vec![0u8; 32],
                view_config: None,
                output_data_receivers: vec![],
            },
        }
    }

    pub fn current_account_id(&mut self, account_id: AccountId) -> &mut Self {
        self.context.current_account_id = convert_account_id(account_id);
        self
    }

    pub fn signer_account_id(&mut self, account_id: AccountId) -> &mut Self {
        self.context.signer_account_id = convert_account_id(account_id);
        self
    }

    pub fn signer_account_pk(&mut self, pk: PublicKey) -> &mut Self {
        self.context.signer_account_pk = pk;
        self
    }

    pub fn predecessor_account_id(&mut self, account_id: AccountId) -> &mut Self {
        self.context.predecessor_account_id = convert_account_id(account_id);
        self
    }

    pub fn block_index(&mut self, block_index: BlockHeight) -> &mut Self {
        self.context.block_index = block_index;
        self
    }

    pub fn block_timestamp(&mut self, block_timestamp: u64) -> &mut Self {
        self.context.block_timestamp = block_timestamp;
        self
    }

    pub fn epoch_height(&mut self, epoch_height: EpochHeight) -> &mut Self {
        self.context.epoch_height = epoch_height;
        self
    }

    pub fn account_balance(&mut self, amount: Balance) -> &mut Self {
        self.context.account_balance = amount;
        self
    }

    pub fn account_locked_balance(&mut self, amount: Balance) -> &mut Self {
        self.context.account_locked_balance = amount;
        self
    }

    pub fn storage_usage(&mut self, usage: StorageUsage) -> &mut Self {
        self.context.storage_usage = usage;
        self
    }

    pub fn attached_deposit(&mut self, amount: Balance) -> &mut Self {
        self.context.attached_deposit = amount;
        self
    }

    pub fn prepaid_gas(&mut self, gas: Gas) -> &mut Self {
        self.context.prepaid_gas = gas;
        self
    }

    pub fn random_seed(&mut self, seed: Vec<u8>) -> &mut Self {
        self.context.random_seed = seed;
        self
    }

    pub fn is_view(&mut self, is_view: bool) -> &mut Self {
        if is_view {
            self.context.view_config = Some(ViewConfig { max_gas_burnt: 200_000_000_000_000 });
        } else {
            self.context.view_config = None;
        }
        self
    }

    pub fn build(&self) -> VMContext {
        self.context.clone()
    }
}

// TODO: This probably shouldn't be necessary with the `testing_env` macro.
/// Initializes the [`BlockchainInterface`] with a single promise result during execution.
///
/// [`BlockchainInterface`]: (crate::BlockchainInterface)
pub fn testing_env_with_promise_results(context: VMContext, promise_result: PromiseResult) {
    let storage = crate::env::take_blockchain_interface()
        .unwrap()
        .as_mut_mocked_blockchain()
        .unwrap()
        .take_storage();

    crate::env::set_blockchain_interface(Box::new(MockedBlockchain::new(
        context,
        VMConfig::test(),
        RuntimeFeesConfig::test(),
        vec![promise_result],
        storage,
        Default::default(),
        None,
    )));
}
