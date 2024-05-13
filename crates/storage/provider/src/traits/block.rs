use crate::{
    BlockIdReader, BlockNumReader, BundleStateWithReceipts, Chain, HeaderProvider, ReceiptProvider,
    ReceiptProviderIdExt, RequestsProvider, TransactionsProvider, WithdrawalsProvider,
};
use auto_impl::auto_impl;
use reth_db::models::StoredBlockBodyIndices;
use reth_interfaces::provider::ProviderResult;
use reth_primitives::{
    Block, BlockHashOrNumber, BlockId, BlockNumber, BlockNumberOrTag, BlockWithSenders, Header,
    PruneModes, Receipt, SealedBlock, SealedBlockWithSenders, SealedHeader, B256,
};
use reth_trie::{updates::TrieUpdates, HashedPostState};
use std::ops::RangeInclusive;

/// Enum to control transaction hash inclusion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransactionVariant {
    /// Indicates that transactions should be processed without including their hashes.
    NoHash,
    /// Indicates that transactions should be processed along with their hashes.
    #[default]
    WithHash,
}

/// A helper enum that represents the origin of the requested block.
///
/// This helper type's sole purpose is to give the caller more control over from where blocks can be
/// fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum BlockSource {
    /// Check all available sources.
    ///
    /// Note: it's expected that looking up pending blocks is faster than looking up blocks in the
    /// database so this prioritizes Pending > Database.
    #[default]
    Any,
    /// The block was fetched from the pending block source, the blockchain tree that buffers
    /// blocks that are not yet finalized.
    Pending,
    /// The block was fetched from the database.
    Database,
}

impl BlockSource {
    /// Returns `true` if the block source is `Pending` or `Any`.
    pub fn is_pending(&self) -> bool {
        matches!(self, BlockSource::Pending | BlockSource::Any)
    }

    /// Returns `true` if the block source is `Database` or `Any`.
    pub fn is_database(&self) -> bool {
        matches!(self, BlockSource::Database | BlockSource::Any)
    }
}

/// Api trait for fetching `Block` related data.
///
/// If not requested otherwise, implementers of this trait should prioritize fetching blocks from
/// the database.
#[auto_impl::auto_impl(&, Arc)]
pub trait BlockReader:
    BlockNumReader
    + HeaderProvider
    + TransactionsProvider
    + ReceiptProvider
    + WithdrawalsProvider
    + RequestsProvider
    + Send
    + Sync
{
    /// Tries to find in the given block source.
    ///
    /// Note: this only operates on the hash because the number might be ambiguous.
    ///
    /// Returns `None` if block is not found.
    fn find_block_by_hash(&self, hash: B256, source: BlockSource) -> ProviderResult<Option<Block>>;

    /// Returns the block with given id from the database.
    ///
    /// Returns `None` if block is not found.
    fn block(&self, id: BlockHashOrNumber) -> ProviderResult<Option<Block>>;

    /// Returns the pending block if available
    ///
    /// Note: This returns a [SealedBlock] because it's expected that this is sealed by the provider
    /// and the caller does not know the hash.
    fn pending_block(&self) -> ProviderResult<Option<SealedBlock>>;

    /// Returns the pending block if available
    ///
    /// Note: This returns a [SealedBlockWithSenders] because it's expected that this is sealed by
    /// the provider and the caller does not know the hash.
    fn pending_block_with_senders(&self) -> ProviderResult<Option<SealedBlockWithSenders>>;

    /// Returns the pending block and receipts if available.
    fn pending_block_and_receipts(&self) -> ProviderResult<Option<(SealedBlock, Vec<Receipt>)>>;

    /// Returns the ommers/uncle headers of the given block from the database.
    ///
    /// Returns `None` if block is not found.
    fn ommers(&self, id: BlockHashOrNumber) -> ProviderResult<Option<Vec<Header>>>;

    /// Returns the block with matching hash from the database.
    ///
    /// Returns `None` if block is not found.
    fn block_by_hash(&self, hash: B256) -> ProviderResult<Option<Block>> {
        self.block(hash.into())
    }

    /// Returns the block with matching number from database.
    ///
    /// Returns `None` if block is not found.
    fn block_by_number(&self, num: u64) -> ProviderResult<Option<Block>> {
        self.block(num.into())
    }

    /// Returns the block body indices with matching number from database.
    ///
    /// Returns `None` if block is not found.
    fn block_body_indices(&self, num: u64) -> ProviderResult<Option<StoredBlockBodyIndices>>;

    /// Returns the block with senders with matching number or hash from database.
    ///
    /// Returns the block's transactions in the requested variant.
    ///
    /// Returns `None` if block is not found.
    fn block_with_senders(
        &self,
        id: BlockHashOrNumber,
        transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<BlockWithSenders>>;

    /// Returns all blocks in the given inclusive range.
    ///
    /// Note: returns only available blocks
    fn block_range(&self, range: RangeInclusive<BlockNumber>) -> ProviderResult<Vec<Block>>;

    /// retrieves a range of blocks from the database, along with the senders of each
    /// transaction in the blocks.
    ///
    /// The `transaction_kind` parameter determines whether to return its hash
    fn block_with_senders_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Vec<BlockWithSenders>>;
}

/// Trait extension for `BlockReader`, for types that implement `BlockId` conversion.
///
/// The `BlockReader` trait should be implemented on types that can retrieve a block from either
/// a block number or hash. However, it might be desirable to fetch a block from a `BlockId` type,
/// which can be a number, hash, or tag such as `BlockNumberOrTag::Safe`.
///
/// Resolving tags requires keeping track of block hashes or block numbers associated with the tag,
/// so this trait can only be implemented for types that implement `BlockIdReader`. The
/// `BlockIdReader` methods should be used to resolve `BlockId`s to block numbers or hashes, and
/// retrieving the block should be done using the type's `BlockReader` methods.
#[auto_impl::auto_impl(&, Arc)]
pub trait BlockReaderIdExt: BlockReader + BlockIdReader + ReceiptProviderIdExt {
    /// Returns the block with matching tag from the database
    ///
    /// Returns `None` if block is not found.
    fn block_by_number_or_tag(&self, id: BlockNumberOrTag) -> ProviderResult<Option<Block>> {
        self.convert_block_number(id)?.map_or_else(|| Ok(None), |num| self.block(num.into()))
    }

    /// Returns the pending block header if available
    ///
    /// Note: This returns a [SealedHeader] because it's expected that this is sealed by the
    /// provider and the caller does not know the hash.
    fn pending_header(&self) -> ProviderResult<Option<SealedHeader>> {
        self.sealed_header_by_id(BlockNumberOrTag::Pending.into())
    }

    /// Returns the latest block header if available
    ///
    /// Note: This returns a [SealedHeader] because it's expected that this is sealed by the
    /// provider and the caller does not know the hash.
    fn latest_header(&self) -> ProviderResult<Option<SealedHeader>> {
        self.sealed_header_by_id(BlockNumberOrTag::Latest.into())
    }

    /// Returns the safe block header if available
    ///
    /// Note: This returns a [SealedHeader] because it's expected that this is sealed by the
    /// provider and the caller does not know the hash.
    fn safe_header(&self) -> ProviderResult<Option<SealedHeader>> {
        self.sealed_header_by_id(BlockNumberOrTag::Safe.into())
    }

    /// Returns the finalized block header if available
    ///
    /// Note: This returns a [SealedHeader] because it's expected that this is sealed by the
    /// provider and the caller does not know the hash.
    fn finalized_header(&self) -> ProviderResult<Option<SealedHeader>> {
        self.sealed_header_by_id(BlockNumberOrTag::Finalized.into())
    }

    /// Returns the block with the matching [BlockId] from the database.
    ///
    /// Returns `None` if block is not found.
    fn block_by_id(&self, id: BlockId) -> ProviderResult<Option<Block>>;

    /// Returns the block with senders with matching [BlockId].
    ///
    /// Returns the block's transactions in the requested variant.
    ///
    /// Returns `None` if block is not found.
    fn block_with_senders_by_id(
        &self,
        id: BlockId,
        transaction_kind: TransactionVariant,
    ) -> ProviderResult<Option<BlockWithSenders>> {
        match id {
            BlockId::Hash(hash) => {
                self.block_with_senders(hash.block_hash.into(), transaction_kind)
            }
            BlockId::Number(num) => self.convert_block_number(num)?.map_or_else(
                || Ok(None),
                |num| self.block_with_senders(num.into(), transaction_kind),
            ),
        }
    }

    /// Returns the header with matching tag from the database
    ///
    /// Returns `None` if header is not found.
    fn header_by_number_or_tag(&self, id: BlockNumberOrTag) -> ProviderResult<Option<Header>> {
        self.convert_block_number(id)?
            .map_or_else(|| Ok(None), |num| self.header_by_hash_or_number(num.into()))
    }

    /// Returns the header with matching tag from the database
    ///
    /// Returns `None` if header is not found.
    fn sealed_header_by_number_or_tag(
        &self,
        id: BlockNumberOrTag,
    ) -> ProviderResult<Option<SealedHeader>> {
        self.convert_block_number(id)?
            .map_or_else(|| Ok(None), |num| self.header_by_hash_or_number(num.into()))?
            .map_or_else(|| Ok(None), |h| Ok(Some(h.seal_slow())))
    }

    /// Returns the sealed header with the matching `BlockId` from the database.
    ///
    /// Returns `None` if header is not found.
    fn sealed_header_by_id(&self, id: BlockId) -> ProviderResult<Option<SealedHeader>>;

    /// Returns the header with the matching `BlockId` from the database.
    ///
    /// Returns `None` if header is not found.
    fn header_by_id(&self, id: BlockId) -> ProviderResult<Option<Header>>;

    /// Returns the ommers with the matching tag from the database.
    fn ommers_by_number_or_tag(&self, id: BlockNumberOrTag) -> ProviderResult<Option<Vec<Header>>> {
        self.convert_block_number(id)?.map_or_else(|| Ok(None), |num| self.ommers(num.into()))
    }

    /// Returns the ommers with the matching `BlockId` from the database.
    ///
    /// Returns `None` if block is not found.
    fn ommers_by_id(&self, id: BlockId) -> ProviderResult<Option<Vec<Header>>>;
}

/// BlockExecution Writer
#[auto_impl(&, Arc, Box)]
pub trait BlockExecutionWriter: BlockWriter + BlockReader + Send + Sync {
    /// Get range of blocks and its execution result
    fn get_block_and_execution_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Chain> {
        self.get_or_take_block_and_execution_range::<false>(range)
    }

    /// Take range of blocks and its execution result
    fn take_block_and_execution_range(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Chain> {
        self.get_or_take_block_and_execution_range::<true>(range)
    }

    /// Return range of blocks and its execution result
    fn get_or_take_block_and_execution_range<const TAKE: bool>(
        &self,
        range: RangeInclusive<BlockNumber>,
    ) -> ProviderResult<Chain>;
}

/// Block Writer
#[auto_impl(&, Arc, Box)]
pub trait BlockWriter: Send + Sync {
    /// Insert full block and make it canonical. Parent tx num and transition id is taken from
    /// parent block in database.
    ///
    /// Return [StoredBlockBodyIndices] that contains indices of the first and last transactions and
    /// transition in the block.
    fn insert_block(
        &self,
        block: SealedBlockWithSenders,
        prune_modes: Option<&PruneModes>,
    ) -> ProviderResult<StoredBlockBodyIndices>;

    /// Appends a batch of sealed blocks to the blockchain, including sender information, and
    /// updates the post-state.
    ///
    /// Inserts the blocks into the database and updates the state with
    /// provided `BundleState`.
    ///
    /// # Parameters
    ///
    /// - `blocks`: Vector of `SealedBlockWithSenders` instances to append.
    /// - `state`: Post-state information to update after appending.
    /// - `prune_modes`: Optional pruning configuration.
    ///
    /// # Returns
    ///
    /// Returns `Ok(())` on success, or an error if any operation fails.
    fn append_blocks_with_state(
        &self,
        blocks: Vec<SealedBlockWithSenders>,
        state: BundleStateWithReceipts,
        hashed_state: HashedPostState,
        trie_updates: TrieUpdates,
        prune_modes: Option<&PruneModes>,
    ) -> ProviderResult<()>;
}
