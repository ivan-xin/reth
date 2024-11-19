/// This is a placeholder comment for the selected code.
/// The selected code indicates that the user is interested in using the Narwhal consensus protocol,
/// but does not provide any actual code. To document the Narwhal consensus implementation,
/// you would need to provide the relevant code and context.
/// 

use alloy_eips::{eip1898::BlockHashOrNumber, eip7685::Requests};
use alloy_primitives::{BlockHash, BlockNumber, Bloom, B256, U256};
use reth_beacon_consensus::BeaconEngineMessage;
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_consensus::{Consensus, ConsensusError, PostExecutionInput};
use reth_engine_primitives::EngineTypes;
use reth_execution_errors::{
    BlockExecutionError, BlockValidationError, InternalBlockExecutionError,
};
use reth_execution_types::ExecutionOutcome;
use reth_primitives::{
    proofs, Block, BlockBody, BlockWithSenders, Header, SealedBlock, SealedHeader,
    TransactionSigned, Withdrawals,
};
use reth_provider::{BlockReaderIdExt, StateProviderFactory, StateRootProvider};
use reth_revm::database::StateProviderDatabase;
use reth_transaction_pool::TransactionPool;
use reth_trie::HashedPostState;
use revm_primitives::calc_excess_blob_gas;
use std::{
    collections::HashMap,
    fmt::Debug,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc::UnboundedSender, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::trace;


mod mode;
mod task;
mod client;

pub use mode::{FixedBlockTimeMiner, NarwhalMode, ReadyTransactionMiner};
use reth_evm::execute::{BlockExecutorProvider, Executor};

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct NarwhalConsensus<ChainSpec> {
    /// Configuration
    chain_spec: Arc<ChainSpec>,
}

impl<ChainSpec> NarwhalConsensus<ChainSpec> {
    /// Create a new instance of [`AutoSealConsensus`]
    pub const fn new(chain_spec: Arc<ChainSpec>) -> Self {
        Self { chain_spec }
    }
}


impl<ChainSpec: Send + Sync + Debug> Consensus for NarwhalConsensus<ChainSpec> {
    fn validate_header(&self, _header: &SealedHeader) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_against_parent(
        &self,
        _header: &SealedHeader,
        _parent: &SealedHeader,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_header_with_total_difficulty(
        &self,
        _header: &Header,
        _total_difficulty: U256,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_pre_execution(&self, _block: &SealedBlock) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn validate_block_post_execution(
        &self,
        _block: &BlockWithSenders,
        _input: PostExecutionInput<'_>,
    ) -> Result<(), ConsensusError> {
        Ok(())
    }
}


#[derive(Debug)]
pub struct NarwhalSealBuilder<Client, Pool, Engine: EngineTypes, EvmConfig, ChainSpec> {
    client: Client,
    consensus: NarwhalConsensus<ChainSpec>,
    pool: Pool,
    mode: NarwhalMode,
    storage: Storage,
    to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
    evm_config: EvmConfig,
}


impl<Client, Pool, Engine, EvmConfig, ChainSpec>
NarwhalSealBuilder<Client, Pool, Engine, EvmConfig, ChainSpec>
where
    Client: BlockReaderIdExt,
    Pool: TransactionPool,
    Engine: EngineTypes,
    ChainSpec: EthChainSpec,
{

}
