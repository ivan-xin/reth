#![allow(dead_code)]

use std::{
    collections::VecDeque,
    fs::File,
    io::{BufRead, BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use reth_exex_types::ExExNotification;
use reth_primitives::BlockNumHash;
use reth_tracing::tracing::{debug, instrument};

/// The maximum number of blocks to cache in the WAL.
///
/// [`CachedBlock`] has a size of `u64 + u64 + B256` which is 384 bits. 384 bits * 1 million = 48
/// megabytes.
const MAX_CACHED_BLOCKS: usize = 1_000_000;

#[derive(Debug)]
struct CachedBlock {
    /// The file offset where the WAL entry is written.
    file_offset: u64,
    /// The block number and hash of the block.
    block: BlockNumHash,
}

#[derive(Debug)]
pub(crate) struct Wal {
    /// The path to the WAL file.
    path: PathBuf,
    /// The file handle of the WAL file.
    file: File,
    /// The block cache of the WAL. Acts as a FIFO queue with a maximum size of
    /// [`MAX_CACHED_BLOCKS`].
    ///
    /// For each notification written to the WAL, there will be an entry per block written to
    /// the cache with the same file offset as the notification in the WAL file. I.e. for each
    /// notification, there may be multiple blocks in the cache.
    ///
    /// This cache is needed only for convenience, so we can avoid walking the WAL file every time
    /// we want to find a notification corresponding to a block.
    block_cache: VecDeque<CachedBlock>,
}

impl Wal {
    /// Creates a new instance of [`Wal`].
    pub(crate) fn new(directory: PathBuf) -> eyre::Result<Self> {
        let path = directory.join("latest.wal");
        let file = File::create(&path)?;

        let mut wal = Self { path, file, block_cache: VecDeque::new() };
        wal.fill_block_cache(u64::MAX)?;

        Ok(wal)
    }

    /// Fills the block cache with the notifications from the WAL file, up to the given offset in
    /// bytes, not inclusive.
    #[instrument]
    fn fill_block_cache(&mut self, to_offset: u64) -> eyre::Result<()> {
        self.block_cache = VecDeque::new();

        let mut file_offset = 0;
        for line in BufReader::new(&self.file).split(b'\n') {
            let line = line?;
            let notification: ExExNotification = bincode::deserialize(&line)?;
            let committed_chain = notification.committed_chain().unwrap_or_default();

            debug!(
                target: "exex::wal",
                ?file_offset,
                committed_block_range = ?committed_chain.range(),
                "Inserting block cache entries"
            );

            for block in committed_chain.blocks().values() {
                self.block_cache.push_back(CachedBlock {
                    file_offset,
                    block: (block.number, block.hash()).into(),
                });
                if self.block_cache.len() > MAX_CACHED_BLOCKS {
                    self.block_cache.pop_front();
                }
            }

            file_offset += line.len() as u64 + 1;
            if file_offset >= to_offset {
                debug!(
                    target: "exex::wal",
                    ?file_offset,
                    "Reached the requested offset when filling the block cache"
                );
                break
            }
        }

        Ok(())
    }

    /// Commits the notification to WAL. If the notification contains a
    /// reverted chain, the WAL is truncated.
    ///
    /// 1. If there's a reverted chain, walks the WAL file from the end and truncates all blocks
    ///    from that were reverted.
    /// 2. If there's a committed chain, encodes the whole notification and writes it to the WAL.
    #[instrument(target = "exex::wal", skip_all)]
    pub(crate) fn commit(&mut self, notification: &ExExNotification) -> eyre::Result<()> {
        if let Some(reverted_chain) = notification.reverted_chain() {
            let reverted_block_range = reverted_chain.range();

            let mut truncate_to = None;
            let mut reverted_blocks = reverted_chain.blocks().values().rev();
            loop {
                let Some(block) = self.block_cache.pop_back() else {
                    debug!(
                        ?reverted_block_range,
                        "No blocks in the block cache, filling the block cache"
                    );
                    self.fill_block_cache(truncate_to.unwrap_or(u64::MAX))?;
                    if self.block_cache.is_empty() {
                        debug!(
                            ?reverted_block_range,
                            "No blocks in the block cache, and filling the block cache didn't change anything"
                        );
                        break
                    }
                    continue
                };

                let Some(reverted_block) = reverted_blocks.next() else { break };

                if reverted_block.number != block.block.number ||
                    reverted_block.hash() != block.block.hash
                {
                    return Err(eyre::eyre!("inconsistent WAL block cache entry"))
                }

                truncate_to = Some(block.file_offset);
            }

            if let Some(truncate_to) = truncate_to {
                self.file.set_len(truncate_to)?;
                debug!(
                    ?truncate_to,
                    ?reverted_block_range,
                    "Truncated the reverted chain from the WAL file"
                );
            } else {
                self.fill_block_cache(u64::MAX)?;
                debug!(
                    ?reverted_block_range,
                    "Had a reverted chain, but no blocks were truncated. Block cache was filled."
                );
            }
        }

        if let Some(committed_chain) = notification.committed_chain() {
            debug!(
                committed_block_range = ?committed_chain.range(),
                "Writing notification with a committed chain to the WAL file"
            );

            let data = bincode::serialize(&notification)?;

            let file_offset = self.file.metadata()?.len();
            self.file.write_all(&data)?;
            self.file.write_all(b"\n")?;
            self.file.flush()?;

            for block in committed_chain.blocks().values() {
                self.block_cache.push_back(CachedBlock {
                    file_offset,
                    block: (block.number, block.hash()).into(),
                });
                if self.block_cache.len() > MAX_CACHED_BLOCKS {
                    self.block_cache.pop_front();
                }
            }
        }

        Ok(())
    }

    /// Rollbacks the WAL to the given block, inclusive.
    ///
    /// 1. Walks the WAL file from the end and searches for the first notification where committed
    ///    chain contains a block with the same number and hash as `to_block`.
    /// 2. If the notification is found, truncates the WAL file to the offset of the notification.
    ///    It means that if the notification contains both given block and blocks before it, the
    ///    whole notification will be truncated.
    ///
    /// # Returns
    ///
    /// The block number and hash of the lowest removed block. The caller is expected to backfill
    /// the blocks between the returned block and the given `to_block`, if there's any.
    #[instrument(target = "exex::wal")]
    pub(crate) fn rollback(
        &mut self,
        to_block: BlockNumHash,
    ) -> eyre::Result<Option<BlockNumHash>> {
        let mut truncate_to = None;
        let mut lowest_removed_block = None;
        loop {
            let Some(block) = self.block_cache.pop_back() else {
                debug!(
                    ?truncate_to,
                    ?lowest_removed_block,
                    "No blocks in the block cache, filling the block cache"
                );
                self.fill_block_cache(truncate_to.unwrap_or(u64::MAX))?;
                if self.block_cache.is_empty() {
                    debug!(
                        ?truncate_to,
                        ?lowest_removed_block,
                        "No blocks in the block cache, and filling the block cache didn't change anything"
                    );
                    break
                }
                continue
            };

            if block.block.number == to_block.number {
                debug!(?truncate_to, ?lowest_removed_block, "Found the requested block");

                if block.block.hash != to_block.hash {
                    return Err(eyre::eyre!("block hash mismatch in WAL"))
                }

                truncate_to = Some(block.file_offset);

                self.file.seek(SeekFrom::Start(block.file_offset))?;
                let notification: ExExNotification = bincode::deserialize_from(&self.file)?;
                lowest_removed_block = notification
                    .committed_chain()
                    .as_ref()
                    .map(|chain| chain.first())
                    .map(|block| (block.number, block.hash()).into());

                break
            }

            truncate_to = Some(block.file_offset);
        }

        if let Some(truncate_to) = truncate_to {
            self.file.set_len(truncate_to)?;
            debug!(?truncate_to, "Truncated the WAL file");
        } else {
            self.fill_block_cache(u64::MAX)?;
            debug!("No blocks were truncated. Block cache was filled.");
        }

        Ok(lowest_removed_block)
    }

    /// Finalizes the WAL to the given block, inclusive.
    ///
    /// 1. Finds an offset of the notification with first unfinalized block (first notification
    ///    containing a committed block higher than `to_block`). If the notificatin includes both
    ///    finalized and non-finalized blocks, the offset will include this notification (i.e.
    ///    consider this notification unfinalized).
    /// 2. Creates a new file and copies all notifications starting from the offset (inclusive) to
    ///    the end of the original file.
    /// 3. Renames the new file to the original file.
    #[instrument(target = "exex::wal")]
    pub(crate) fn finalize(&mut self, to_block: BlockNumHash) -> eyre::Result<()> {
        // First, walk cache to find the offset of the notification with the finalized block.
        let mut unfinalized_from_offset = None;
        while let Some(cached_block) = self.block_cache.pop_front() {
            if cached_block.block.number == to_block.number {
                if cached_block.block.hash != to_block.hash {
                    return Err(eyre::eyre!("block hash mismatch in WAL"))
                }

                unfinalized_from_offset = Some(self.block_cache.front().map_or_else(
                    || std::io::Result::Ok(self.file.metadata()?.len()),
                    |block| std::io::Result::Ok(block.file_offset),
                )?);

                debug!(?unfinalized_from_offset, "Found the finalized block in the block cache");
                break
            }
        }

        // If the finalized block is not found in cache, we need to walk the whole file.
        if unfinalized_from_offset.is_none() {
            debug!("Finalized block not found in the block cache, walking the whole file");

            let mut offset = 0;
            for data in BufReader::new(&self.file).split(b'\n') {
                let data = data?;
                let notification: ExExNotification = bincode::deserialize(&data)?;
                if let Some(committed_chain) = notification.committed_chain() {
                    let finalized_block = committed_chain.blocks().get(&to_block.number);

                    if let Some(finalized_block) = finalized_block {
                        if finalized_block.hash() != to_block.hash {
                            return Err(eyre::eyre!("block hash mismatch in WAL"))
                        }

                        if committed_chain.blocks().len() == 1 {
                            unfinalized_from_offset = Some(offset + data.len() as u64 + 1);
                        } else {
                            unfinalized_from_offset = Some(offset);
                        }

                        debug!(
                            ?unfinalized_from_offset,
                            committed_block_range = ?committed_chain.range(),
                            "Found the finalized block in the WAL file"
                        );
                        break
                    }
                }

                offset += data.len() as u64 + 1;
            }
        }

        // If the finalized block is still not found, we can't do anything and just return.
        let Some(unfinalized_from_offset) = unfinalized_from_offset else {
            debug!("Could not find the finalized block in WAL");
            return Ok(())
        };

        // Seek the original file to the first notification containing an
        // unfinalized block.
        self.file.seek(SeekFrom::Start(unfinalized_from_offset))?;

        // Open the temporary file for writing and copy the notifications with unfinalized blocks
        let tmp_file_path = self.path.with_extension("tmp");
        let mut new_file = File::create(&tmp_file_path)?;
        loop {
            let mut buffer = [0; 4096];
            let read = self.file.read(&mut buffer)?;
            new_file.write_all(&buffer[..read])?;

            if read < 1024 {
                break
            }
        }

        let old_size = self.file.metadata()?.len();
        let new_size = new_file.metadata()?.len();

        // Rename the temporary file to the WAL file and update the file handle with it
        reth_fs_util::rename(&tmp_file_path, &self.path)?;
        self.file = new_file;

        // Fill the block cache with the notifications from the new file.
        self.fill_block_cache(u64::MAX)?;

        debug!(?old_size, ?new_size, "WAL file was finalized and block cache was filled");

        Ok(())
    }
}
