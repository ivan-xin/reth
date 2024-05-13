//! Consensus protocol functions

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/paradigmxyz/reth/main/assets/reth-docs.png",
    html_favicon_url = "https://avatars0.githubusercontent.com/u/97369466?s=256",
    issue_tracker_base_url = "https://github.com/paradigmxyz/reth/issues/"
)]
#![cfg_attr(not(test), warn(unused_crate_dependencies))]
#![cfg_attr(docsrs, feature(doc_cfg, doc_auto_cfg))]

use reth_primitives::{
    BlockHash, BlockNumber, GotExpected, GotExpectedBoxed, Header, HeaderValidationError,
    InvalidTransactionError, SealedBlock, SealedHeader, B256, U256,
};
use std::fmt::Debug;

#[cfg(any(test, feature = "test-utils"))]
/// test helpers for mocking consensus
pub mod test_utils;

/// Consensus is a protocol that chooses canonical chain.
#[auto_impl::auto_impl(&, Arc)]
pub trait Consensus: Debug + Send + Sync {
    /// Validate if header is correct and follows consensus specification.
    ///
    /// This is called on standalone header to check if all hashes are correct.
    fn validate_header(&self, header: &SealedHeader) -> Result<(), ConsensusError>;

    /// Validate that the header information regarding parent are correct.
    /// This checks the block number, timestamp, basefee and gas limit increment.
    ///
    /// This is called before properties that are not in the header itself (like total difficulty)
    /// have been computed.
    ///
    /// **This should not be called for the genesis block**.
    ///
    /// Note: Validating header against its parent does not include other Consensus validations.
    fn validate_header_against_parent(
        &self,
        header: &SealedHeader,
        parent: &SealedHeader,
    ) -> Result<(), ConsensusError>;

    /// Validates the given headers
    ///
    /// This ensures that the first header is valid on its own and all subsequent headers are valid
    /// on its own and valid against its parent.
    ///
    /// Note: this expects that the headers are in natural order (ascending block number)
    fn validate_header_range(&self, headers: &[SealedHeader]) -> Result<(), HeaderConsensusError> {
        if let Some((initial_header, remaining_headers)) = headers.split_first() {
            self.validate_header(initial_header)
                .map_err(|e| HeaderConsensusError(e, initial_header.clone()))?;
            let mut parent = initial_header;
            for child in remaining_headers {
                self.validate_header(child).map_err(|e| HeaderConsensusError(e, child.clone()))?;
                self.validate_header_against_parent(child, parent)
                    .map_err(|e| HeaderConsensusError(e, child.clone()))?;
                parent = child;
            }
        }
        Ok(())
    }

    /// Validate if the header is correct and follows the consensus specification, including
    /// computed properties (like total difficulty).
    ///
    /// Some consensus engines may want to do additional checks here.
    ///
    /// Note: validating headers with TD does not include other Consensus validation.
    fn validate_header_with_total_difficulty(
        &self,
        header: &Header,
        total_difficulty: U256,
    ) -> Result<(), ConsensusError>;

    /// Validate a block disregarding world state, i.e. things that can be checked before sender
    /// recovery and execution.
    ///
    /// See the Yellow Paper sections 4.3.2 "Holistic Validity", 4.3.4 "Block Header Validity", and
    /// 11.1 "Ommer Validation".
    ///
    /// **This should not be called for the genesis block**.
    ///
    /// Note: validating blocks does not include other validations of the Consensus
    fn validate_block(&self, block: &SealedBlock) -> Result<(), ConsensusError>;
}

/// Consensus Errors
#[derive(thiserror::Error, Debug, PartialEq, Eq, Clone)]
pub enum ConsensusError {
    /// Error when the gas used in the header exceeds the gas limit.
    #[error("block used gas ({gas_used}) is greater than gas limit ({gas_limit})")]
    HeaderGasUsedExceedsGasLimit {
        /// The gas used in the block header.
        gas_used: u64,
        /// The gas limit in the block header.
        gas_limit: u64,
    },

    /// Error when the hash of block ommer is different from the expected hash.
    #[error("mismatched block ommer hash: {0}")]
    BodyOmmersHashDiff(GotExpectedBoxed<B256>),

    /// Error when the state root in the block is different from the expected state root.
    #[error("mismatched block state root: {0}")]
    BodyStateRootDiff(GotExpectedBoxed<B256>),

    /// Error when the transaction root in the block is different from the expected transaction
    /// root.
    #[error("mismatched block transaction root: {0}")]
    BodyTransactionRootDiff(GotExpectedBoxed<B256>),

    /// Error when the withdrawals root in the block is different from the expected withdrawals
    /// root.
    #[error("mismatched block withdrawals root: {0}")]
    BodyWithdrawalsRootDiff(GotExpectedBoxed<B256>),

    /// Error when the requests root in the block is different from the expected requests
    /// root.
    #[error("mismatched block requests root: {0}")]
    BodyRequestsRootDiff(GotExpectedBoxed<B256>),

    /// Error when a block with a specific hash and number is already known.
    #[error("block with [hash={hash}, number={number}] is already known")]
    BlockKnown {
        /// The hash of the known block.
        hash: BlockHash,
        /// The block number of the known block.
        number: BlockNumber,
    },

    /// Error when the parent hash of a block is not known.
    #[error("block parent [hash={hash}] is not known")]
    ParentUnknown {
        /// The hash of the unknown parent block.
        hash: BlockHash,
    },

    /// Error when the block number does not match the parent block number.
    #[error(
        "block number {block_number} does not match parent block number {parent_block_number}"
    )]
    ParentBlockNumberMismatch {
        /// The parent block number.
        parent_block_number: BlockNumber,
        /// The block number.
        block_number: BlockNumber,
    },

    /// Error when the block timestamp is in the future compared to our clock time.
    #[error("block timestamp {timestamp} is in the future compared to our clock time {present_timestamp}")]
    TimestampIsInFuture {
        /// The block's timestamp.
        timestamp: u64,
        /// The current timestamp.
        present_timestamp: u64,
    },

    /// Error when the base fee is missing.
    #[error("base fee missing")]
    BaseFeeMissing,

    /// Error when there is a transaction signer recovery error.
    #[error("transaction signer recovery error")]
    TransactionSignerRecoveryError,

    /// Error when the extra data length exceeds the maximum allowed.
    #[error("extra data {len} exceeds max length")]
    ExtraDataExceedsMax {
        /// The length of the extra data.
        len: usize,
    },

    /// Error when the difficulty after a merge is not zero.
    #[error("difficulty after merge is not zero")]
    TheMergeDifficultyIsNotZero,

    /// Error when the nonce after a merge is not zero.
    #[error("nonce after merge is not zero")]
    TheMergeNonceIsNotZero,

    /// Error when the ommer root after a merge is not empty.
    #[error("ommer root after merge is not empty")]
    TheMergeOmmerRootIsNotEmpty,

    /// Error when the withdrawals root is missing.
    #[error("missing withdrawals root")]
    WithdrawalsRootMissing,

    /// Error when the requests root is missing.
    #[error("missing requests root")]
    RequestsRootMissing,

    /// Error when an unexpected withdrawals root is encountered.
    #[error("unexpected withdrawals root")]
    WithdrawalsRootUnexpected,

    /// Error when an unexpected requests root is encountered.
    #[error("unexpected requests root")]
    RequestsRootUnexpected,

    /// Error when withdrawals are missing.
    #[error("missing withdrawals")]
    BodyWithdrawalsMissing,

    /// Error when requests are missing.
    #[error("missing requests")]
    BodyRequestsMissing,

    /// Error when blob gas used is missing.
    #[error("missing blob gas used")]
    BlobGasUsedMissing,

    /// Error when unexpected blob gas used is encountered.
    #[error("unexpected blob gas used")]
    BlobGasUsedUnexpected,

    /// Error when excess blob gas is missing.
    #[error("missing excess blob gas")]
    ExcessBlobGasMissing,

    /// Error when unexpected excess blob gas is encountered.
    #[error("unexpected excess blob gas")]
    ExcessBlobGasUnexpected,

    /// Error when the parent beacon block root is missing.
    #[error("missing parent beacon block root")]
    ParentBeaconBlockRootMissing,

    /// Error when an unexpected parent beacon block root is encountered.
    #[error("unexpected parent beacon block root")]
    ParentBeaconBlockRootUnexpected,

    /// Error when blob gas used exceeds the maximum allowed.
    #[error("blob gas used {blob_gas_used} exceeds maximum allowance {max_blob_gas_per_block}")]
    BlobGasUsedExceedsMaxBlobGasPerBlock {
        /// The actual blob gas used.
        blob_gas_used: u64,
        /// The maximum allowed blob gas per block.
        max_blob_gas_per_block: u64,
    },

    /// Error when blob gas used is not a multiple of blob gas per blob.
    #[error(
        "blob gas used {blob_gas_used} is not a multiple of blob gas per blob {blob_gas_per_blob}"
    )]
    BlobGasUsedNotMultipleOfBlobGasPerBlob {
        /// The actual blob gas used.
        blob_gas_used: u64,
        /// The blob gas per blob.
        blob_gas_per_blob: u64,
    },

    /// Error when excess blob gas is not a multiple of blob gas per blob.
    #[error(
    "excess blob gas {excess_blob_gas} is not a multiple of blob gas per blob {blob_gas_per_blob}"
    )]
    ExcessBlobGasNotMultipleOfBlobGasPerBlob {
        /// The actual excess blob gas.
        excess_blob_gas: u64,
        /// The blob gas per blob.
        blob_gas_per_blob: u64,
    },

    /// Error when the blob gas used in the header does not match the expected blob gas used.
    #[error("blob gas used mismatch: {0}")]
    BlobGasUsedDiff(GotExpected<u64>),

    /// Error for a transaction that violates consensus.
    #[error(transparent)]
    InvalidTransaction(#[from] InvalidTransactionError),

    /// Error type transparently wrapping HeaderValidationError.
    #[error(transparent)]
    HeaderValidationError(#[from] HeaderValidationError),
}

impl ConsensusError {
    /// Returns `true` if the error is a state root error.
    pub fn is_state_root_error(&self) -> bool {
        matches!(self, ConsensusError::BodyStateRootDiff(_))
    }
}

/// `HeaderConsensusError` combines a `ConsensusError` with the `SealedHeader` it relates to.
#[derive(thiserror::Error, Debug)]
#[error("Consensus error: {0}, Invalid header: {1:?}")]
pub struct HeaderConsensusError(ConsensusError, SealedHeader);
