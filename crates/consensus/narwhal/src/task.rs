use crate::{mode::NarwhalMode, Storage};
use alloy_rpc_types_engine::ForkchoiceState;
use futures_util::{future::BoxFuture, FutureExt};
use reth_beacon_consensus::{BeaconEngineMessage, ForkchoiceStatus};
use reth_chainspec::{EthChainSpec, EthereumHardforks};
use reth_engine_primitives::{EngineApiMessageVersion, EngineTypes};
use reth_evm::execute::BlockExecutorProvider;
use reth_provider::{CanonChainTracker, StateProviderFactory};
use reth_stages_api::PipelineEvent;
use reth_tokio_util::EventStream;
use reth_transaction_pool::{TransactionPool, ValidPoolTransaction};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
use tokio::sync::{mpsc::UnboundedSender, oneshot};

/// A Future that listens for new ready transactions and puts new blocks into storage
pub struct NarwhalMiningTask<Client, Pool: TransactionPool, Executor, Engine: EngineTypes, ChainSpec> {
    /// The configured chain spec
    chain_spec: Arc<ChainSpec>,
    /// The client used to interact with the state
    client: Client,
    /// The active miner
    miner: NarwhalMode,
    /// Single active future that inserts a new block into `storage`
    insert_task: Option<BoxFuture<'static, Option<EventStream<PipelineEvent>>>>,
    /// Shared storage to insert new blocks
    storage: Storage,
    /// Pool where transactions are stored
    pool: Pool,
    /// backlog of sets of transactions ready to be mined
    queued: VecDeque<Vec<Arc<ValidPoolTransaction<<Pool as TransactionPool>::Transaction>>>>,
    // TODO: ideally this would just be a sender of hashes
    to_engine: UnboundedSender<BeaconEngineMessage<Engine>>,
    /// The pipeline events to listen on
    pipe_line_events: Option<EventStream<PipelineEvent>>,
    /// The type used for block execution
    block_executor: Executor,
}