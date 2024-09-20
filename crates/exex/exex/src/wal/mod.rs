#![allow(dead_code)]

mod cache;
mod storage;

use std::{ops::ControlFlow, path::Path};

use cache::BlockCache;
use eyre::OptionExt;
use reth_exex_types::ExExNotification;
use reth_primitives::BlockNumHash;
use reth_tracing::tracing::{debug, instrument};
use storage::Storage;

/// The maximum number of blocks to cache.
///
/// [`cache::CachedBlock`] has a size of `u64 + u64 + B256` which is 384 bits. 384 bits * 1 million
/// = 48 megabytes.
const MAX_CACHED_BLOCKS: usize = 1_000_000;

/// WAL is a write-ahead log (WAL) that stores the notifications sent to a particular ExEx.
///
/// WAL is backed by a binary file represented by [`Storage`] and a block cache represented by
/// [`BlockCache`].
///
/// The expected mode of operation is as follows:
/// 1. On every new canonical chain notification, call [`Wal::commit`].
/// 2. When ExEx is on a wrong fork, rollback the WAL using [`Wal::rollback`]. The caller is
///    expected to create reverts from the removed notifications and backfill the blocks between the
///    returned block and the given rollback block. After that, commit new notifications as usual
///    with [`Wal::commit`].
/// 3. When the chain is finalized, call [`Wal::finalize`] to prevent the infinite growth of the
///    WAL.
#[derive(Debug)]
pub(crate) struct Wal {
    /// The underlying WAL storage backed by a file.
    storage: Storage,
    /// WAL block cache. See [`cache::BlockCache`] docs for more details.
    block_cache: BlockCache,
}

impl Wal {
    /// Creates a new instance of [`Wal`].
    pub(crate) fn new(directory: impl AsRef<Path>) -> eyre::Result<Self> {
        let mut wal = Self { storage: Storage::new(directory)?, block_cache: BlockCache::new() };
        wal.fill_block_cache()?;

        Ok(wal)
    }

    /// Clears the block cache and fills it with the notifications from the [`Storage`], up to the
    /// given offset in bytes, not inclusive.
    #[instrument(target = "exex::wal", skip(self))]
    fn fill_block_cache(&mut self) -> eyre::Result<()> {
        for entry in self.storage.iter_notifications(..) {
            let (file_id, notification) = entry?;

            let committed_chain = notification.committed_chain();
            let reverted_chain = notification.reverted_chain();

            debug!(
                target: "exex::wal",
                ?file_id,
                reverted_block_range = ?reverted_chain.as_ref().map(|chain| chain.range()),
                committed_block_range = ?committed_chain.as_ref().map(|chain| chain.range()),
                "Inserting block cache entries"
            );

            self.block_cache.insert_notification_blocks_with_file_id(file_id, &notification);
        }

        Ok(())
    }

    /// Commits the notification to WAL.
    #[instrument(target = "exex::wal", skip_all)]
    pub(crate) fn commit(&mut self, notification: &ExExNotification) -> eyre::Result<()> {
        let reverted_chain = notification.reverted_chain();
        let committed_chain = notification.committed_chain();

        debug!(
            reverted_block_range = ?reverted_chain.as_ref().map(|chain| chain.range()),
            committed_block_range = ?committed_chain.as_ref().map(|chain| chain.range()),
            "Writing notification to WAL"
        );
        let file_id = self.storage.write_notification(notification)?;

        debug!(
            ?file_id,
            reverted_block_range = ?reverted_chain.as_ref().map(|chain| chain.range()),
            committed_block_range = ?committed_chain.as_ref().map(|chain| chain.range()),
            "Inserting notification blocks into the block cache"
        );
        self.block_cache.insert_notification_blocks_with_file_id(file_id, notification);

        Ok(())
    }

    /// Rollbacks the WAL to the given block, inclusive.
    ///
    /// 1. Walks the WAL from the end and searches for the first notification where committed chain
    ///    contains a block with the same number and hash as `to_block`.
    /// 2. If the notification is found, truncates the WAL to the offset of the notification. It
    ///    means that if the notification contains both given block and blocks before it, the whole
    ///    notification will be truncated.
    ///
    /// # Returns
    ///
    /// 1. The block number and hash of the lowest removed block.
    /// 2. The notifications that were removed.
    #[instrument(target = "exex::wal", skip(self))]
    pub(crate) fn rollback(
        &mut self,
        to_block: BlockNumHash,
    ) -> eyre::Result<Option<(BlockNumHash, Vec<ExExNotification>)>> {
        let mut truncate_to = None;
        let mut lowest_removed_block = None;
        while let Some((file_id, block)) = self.block_cache.pop_back() {
            if block.action.is_commit() && block.block.number == to_block.number {
                debug!(?truncate_to, ?lowest_removed_block, "Found the requested block");

                if block.block.hash != to_block.hash {
                    eyre::bail!("block hash mismatch in WAL")
                }

                truncate_to = Some(file_id);

                let notification = self.storage.read_notification(file_id)?;
                lowest_removed_block = notification
                    .committed_chain()
                    .as_ref()
                    .map(|chain| chain.first())
                    .map(|block| (block.number, block.hash()).into());
                break
            }

            truncate_to = Some(file_id);
        }

        let result = if let Some(truncate_to) = truncate_to {
            let removed_notifications = self.storage.truncate_to_file_id(truncate_to)?;
            debug!(?truncate_to, "Truncated the storage");
            Some((lowest_removed_block.expect("qed"), removed_notifications))
        } else {
            debug!("No blocks were truncated");
            None
        };

        Ok(result)
    }

    /// Finalizes the WAL to the given block, inclusive.
    ///
    /// 1. Finds an offset of the notification with first unfinalized block (first notification
    ///    containing a committed block higher than `to_block`). If the notificatin includes both
    ///    finalized and non-finalized blocks, the offset will include this notification (i.e.
    ///    consider this notification unfinalized and don't remove it).
    /// 2. Truncates the storage from the offset of the notification, not inclusive.
    #[instrument(target = "exex::wal", skip(self))]
    pub(crate) fn finalize(&mut self, to_block: BlockNumHash) -> eyre::Result<()> {
        // First, walk cache to find the offset of the notification with the finalized block.
        let mut unfinalized_from_offset = None;
        while let Some((_, block)) = self.block_cache.pop_front() {
            if block.action.is_commit() &&
                block.block.number == to_block.number &&
                block.block.hash == to_block.hash
            {
                unfinalized_from_offset = self
                    .block_cache
                    .front()
                    .map_or_else(|| self.storage.max_id(), |(file_id, _)| Some(file_id));

                debug!(?unfinalized_from_offset, "Found the finalized block in the block cache");
                break
            }
        }

        // If the finalized block is still not found, we can't do anything and just return.
        let Some(unfinalized_from_offset) = unfinalized_from_offset else {
            debug!("Could not find the finalized block in WAL");
            return Ok(())
        };

        // Truncate the storage to the unfinalized block.
        let notifications_truncated =
            self.storage.truncate_from_file_id(unfinalized_from_offset)?;

        debug!(?notifications_truncated, "WAL was finalized");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::ControlFlow, sync::Arc};

    use eyre::OptionExt;
    use reth_exex_types::ExExNotification;
    use reth_provider::Chain;
    use reth_testing_utils::generators::{
        self, random_block, random_block_range, BlockParams, BlockRangeParams,
    };

    use crate::wal::{
        cache::{CachedBlock, CachedBlockAction},
        Wal,
    };

    fn read_notifications(wal: &Wal) -> eyre::Result<Vec<ExExNotification>> {
        wal.storage.iter_notifications(..).map(|entry| Ok(entry?.1)).collect::<eyre::Result<_>>()
    }

    #[test]
    fn test_wal() -> eyre::Result<()> {
        reth_tracing::init_test_tracing();

        let mut rng = generators::rng();

        // Create an instance of the WAL in a temporary directory
        let temp_dir = tempfile::tempdir()?;
        let mut wal = Wal::new(&temp_dir)?;
        assert!(wal.block_cache.is_empty());

        // Create 4 canonical blocks and one reorged block with number 2
        let blocks = random_block_range(&mut rng, 0..=3, BlockRangeParams::default())
            .into_iter()
            .map(|block| block.seal_with_senders().ok_or_eyre("failed to recover senders"))
            .collect::<eyre::Result<Vec<_>>>()?;
        let block_2_reorged = random_block(
            &mut rng,
            2,
            BlockParams { parent: Some(blocks[1].hash()), ..Default::default() },
        )
        .seal_with_senders()
        .ok_or_eyre("failed to recover senders")?;

        // Create notifications for the above blocks.
        // 1. Committed notification for blocks with number 0 and 1
        // 2. Reverted notification for block with number 1
        // 3. Committed notification for block with number 1 and 2
        // 4. Reorged notification for block with number 2 that was reverted, and blocks with number
        //    2 and 3 that were committed
        let committed_notification_1 = ExExNotification::ChainCommitted {
            new: Arc::new(Chain::new(
                vec![blocks[0].clone(), blocks[1].clone()],
                Default::default(),
                None,
            )),
        };
        let reverted_notification = ExExNotification::ChainReverted {
            old: Arc::new(Chain::new(vec![blocks[1].clone()], Default::default(), None)),
        };
        let committed_notification_2 = ExExNotification::ChainCommitted {
            new: Arc::new(Chain::new(
                vec![blocks[1].clone(), blocks[2].clone()],
                Default::default(),
                None,
            )),
        };
        let reorged_notification = ExExNotification::ChainReorged {
            old: Arc::new(Chain::new(vec![blocks[2].clone()], Default::default(), None)),
            new: Arc::new(Chain::new(
                vec![block_2_reorged.clone(), blocks[3].clone()],
                Default::default(),
                None,
            )),
        };

        // Commit notifications, verify that the block cache is updated and the notifications are
        // written to WAL.

        // First notification (commit block 0, 1)
        let file_id = 0;
        let committed_notification_1_cache = vec![
            (
                file_id,
                CachedBlock {
                    action: CachedBlockAction::Commit,
                    block: (blocks[0].number, blocks[0].hash()).into(),
                },
            ),
            (
                file_id,
                CachedBlock {
                    action: CachedBlockAction::Commit,
                    block: (blocks[1].number, blocks[1].hash()).into(),
                },
            ),
        ];
        wal.commit(&committed_notification_1)?;
        assert_eq!(wal.block_cache.iter().collect::<Vec<_>>(), committed_notification_1_cache);
        assert_eq!(read_notifications(&wal)?, vec![committed_notification_1.clone()]);

        // Second notification (revert block 1)
        wal.commit(&reverted_notification)?;
        let file_id = wal.storage.max_id().unwrap();
        let reverted_notification_cache = vec![(
            file_id,
            CachedBlock {
                action: CachedBlockAction::Revert,
                block: (blocks[1].number, blocks[1].hash()).into(),
            },
        )];
        assert_eq!(
            wal.block_cache.iter().collect::<Vec<_>>(),
            [committed_notification_1_cache.clone(), reverted_notification_cache.clone()].concat()
        );
        assert_eq!(
            read_notifications(&wal)?,
            vec![committed_notification_1.clone(), reverted_notification.clone()]
        );

        // Now, rollback to block 1 and verify that both the block cache and the storage are
        // empty. We expect the rollback to delete the first notification (commit block 0, 1),
        // because we can't delete blocks partly from the notification, and also the second
        // notification (revert block 1). Additionally, check that the block that the rolled
        // back to is the block with number 0.
        let rollback_result = wal.rollback((blocks[1].number, blocks[1].hash()).into())?;
        assert_eq!(wal.block_cache.iter().collect::<Vec<_>>(), vec![]);
        assert_eq!(wal.storage.iter_notifications(..).collect::<eyre::Result<Vec<_>>>()?, vec![]);
        assert_eq!(
            rollback_result,
            Some((
                (blocks[0].number, blocks[0].hash()).into(),
                vec![committed_notification_1.clone(), reverted_notification.clone()]
            ))
        );

        // Commit notifications 1 and 2 again
        wal.commit(&committed_notification_1)?;
        assert_eq!(
            wal.block_cache.iter().collect::<Vec<_>>(),
            [committed_notification_1_cache.clone()].concat()
        );
        assert_eq!(read_notifications(&wal)?, vec![committed_notification_1.clone()]);
        wal.commit(&reverted_notification)?;
        assert_eq!(
            wal.block_cache.iter().collect::<Vec<_>>(),
            [committed_notification_1_cache.clone(), reverted_notification_cache.clone()].concat()
        );
        assert_eq!(
            read_notifications(&wal)?,
            vec![committed_notification_1.clone(), reverted_notification.clone()]
        );

        // Third notification (commit block 1, 2)
        wal.commit(&committed_notification_2)?;
        let file_id = wal.storage.max_id().unwrap();
        let committed_notification_2_cache = vec![
            (
                file_id,
                CachedBlock {
                    action: CachedBlockAction::Commit,
                    block: (blocks[1].number, blocks[1].hash()).into(),
                },
            ),
            (
                file_id,
                CachedBlock {
                    action: CachedBlockAction::Commit,
                    block: (blocks[2].number, blocks[2].hash()).into(),
                },
            ),
        ];
        assert_eq!(
            wal.block_cache.iter().collect::<Vec<_>>(),
            [
                committed_notification_1_cache.clone(),
                reverted_notification_cache.clone(),
                committed_notification_2_cache.clone()
            ]
            .concat()
        );
        assert_eq!(
            read_notifications(&wal)?,
            vec![
                committed_notification_1.clone(),
                reverted_notification.clone(),
                committed_notification_2.clone()
            ]
        );

        // Fourth notification (revert block 2, commit block 2, 3)
        wal.commit(&reorged_notification)?;
        let file_id = wal.storage.max_id().unwrap();
        let reorged_notification_cache = vec![
            (
                file_id,
                CachedBlock {
                    action: CachedBlockAction::Revert,
                    block: (blocks[2].number, blocks[2].hash()).into(),
                },
            ),
            (
                file_id,
                CachedBlock {
                    action: CachedBlockAction::Commit,
                    block: (block_2_reorged.number, block_2_reorged.hash()).into(),
                },
            ),
            (
                file_id,
                CachedBlock {
                    action: CachedBlockAction::Commit,
                    block: (blocks[3].number, blocks[3].hash()).into(),
                },
            ),
        ];
        assert_eq!(
            wal.block_cache.iter().collect::<Vec<_>>(),
            [
                committed_notification_1_cache,
                reverted_notification_cache.clone(),
                committed_notification_2_cache.clone(),
                reorged_notification_cache.clone()
            ]
            .concat()
        );
        assert_eq!(
            read_notifications(&wal)?,
            vec![
                committed_notification_1.clone(),
                reverted_notification.clone(),
                committed_notification_2.clone(),
                reorged_notification.clone()
            ]
        );

        // Now, finalize the WAL up to the block 1. Block 1 was in the third notification that also
        // had block 2 committed. In this case, we can't split the notification into two parts, so
        // we preserve the whole notification in both the block cache and the storage, and delete
        // the notifications before it.
        wal.finalize((blocks[1].number, blocks[1].hash()).into())?;
        let finalized_files_number = reverted_notification_cache[0].0;
        let finalized_notification_cache = [
            reverted_notification_cache,
            committed_notification_2_cache,
            reorged_notification_cache,
        ]
        .concat()
        .into_iter()
        .map(|(file_id, block)| (file_id - finalized_files_number, block))
        .collect::<Vec<_>>();
        assert_eq!(wal.block_cache.iter().collect::<Vec<_>>(), finalized_notification_cache);
        assert_eq!(
            read_notifications(&wal)?,
            vec![reverted_notification, committed_notification_2, reorged_notification]
        );

        Ok(())
    }
}
