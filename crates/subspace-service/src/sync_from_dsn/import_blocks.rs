// Copyright (C) 2023 Subspace Labs, Inc.
// SPDX-License-Identifier: GPL-3.0-or-later

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use crate::sync_from_dsn::segment_header_downloader::SegmentHeaderDownloader;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use sc_client_api::{AuxStore, BlockBackend, HeaderBackend};
use sc_consensus::import_queue::ImportQueueService;
use sc_consensus::IncomingBlock;
use sc_consensus_subspace::archiver::SegmentHeadersStore;
use sc_tracing::tracing::{debug, trace};
use sp_consensus::BlockOrigin;
use sp_runtime::traits::{Block as BlockT, Header, NumberFor, One};
use sp_runtime::Saturating;
use std::num::NonZeroU16;
use std::time::Duration;
use subspace_archiving::reconstructor::Reconstructor;
use subspace_core_primitives::{
    ArchivedHistorySegment, BlockNumber, Piece, RecordedHistorySegment, SegmentIndex,
};
use subspace_networking::utils::piece_provider::{PieceProvider, PieceValidator, RetryPolicy};
use tokio::sync::Semaphore;
use tracing::warn;

/// Get piece retry attempts number.
const PIECE_GETTER_RETRY_NUMBER: NonZeroU16 = NonZeroU16::new(3).expect("Not zero; qed");

/// How many blocks to queue before pausing and waiting for blocks to be imported, this is
/// essentially used to ensure we use a bounded amount of RAM during sync process.
const QUEUED_BLOCKS_LIMIT: BlockNumber = 500;
/// Time to wait for blocks to import if import is too slow
const WAIT_FOR_BLOCKS_TO_IMPORT: Duration = Duration::from_secs(1);

/// Starts the process of importing blocks.
///
/// Returns number of downloaded blocks.
pub async fn import_blocks_from_dsn<Block, AS, Client, PV, IQS>(
    segment_headers_store: &SegmentHeadersStore<AS>,
    segment_header_downloader: &SegmentHeaderDownloader<'_>,
    client: &Client,
    piece_provider: &PieceProvider<PV>,
    import_queue_service: &mut IQS,
    last_processed_segment_index: &mut SegmentIndex,
    last_processed_block_number: &mut <Block::Header as Header>::Number,
) -> Result<u64, sc_service::Error>
where
    Block: BlockT,
    AS: AuxStore + Send + Sync + 'static,
    Client: HeaderBackend<Block> + BlockBackend<Block> + Send + Sync + 'static,
    PV: PieceValidator,
    IQS: ImportQueueService<Block> + ?Sized,
{
    {
        let max_segment_index = segment_headers_store.max_segment_index().ok_or_else(|| {
            sc_service::Error::Other(
                "Archiver needs to be initialized before syncing from DSN to populate the very \
                    first segment"
                    .to_string(),
            )
        })?;
        let new_segment_headers = segment_header_downloader
            .get_segment_headers(max_segment_index)
            .await
            .map_err(|error| error.to_string())?;

        debug!("Found {} new segment headers", new_segment_headers.len());

        if !new_segment_headers.is_empty() {
            segment_headers_store.add_segment_headers(&new_segment_headers)?;
        }
    }

    let mut downloaded_blocks = 0;
    let mut reconstructor = Reconstructor::new().map_err(|error| error.to_string())?;
    // Start from the first unprocessed segment and process all segments known so far
    let segment_indices_iter = (*last_processed_segment_index + SegmentIndex::ONE)
        ..=segment_headers_store
            .max_segment_index()
            .expect("Exists, we have inserted segment headers above; qed");
    let mut segment_indices_iter = segment_indices_iter.peekable();

    while let Some(segment_index) = segment_indices_iter.next() {
        debug!(%segment_index, "Processing segment");

        let segment_header = segment_headers_store
            .get_segment_header(segment_index)
            .expect("Statically guaranteed to exist, see checks above; qed");

        trace!(
            %segment_index,
            last_archived_block_number = %segment_header.last_archived_block().number,
            last_archived_block_progress = ?segment_header.last_archived_block().archived_progress,
            "Checking segment header"
        );

        let last_archived_block =
            NumberFor::<Block>::from(segment_header.last_archived_block().number);
        let last_archived_block_partial = segment_header
            .last_archived_block()
            .archived_progress
            .partial()
            .is_some();

        let info = client.info();
        // We already have this block imported and it is finalized, so can't change
        if last_archived_block <= info.finalized_number {
            *last_processed_segment_index = segment_index;
            *last_processed_block_number = last_archived_block;
            // Reset reconstructor instance
            reconstructor = Reconstructor::new().map_err(|error| error.to_string())?;
            continue;
        }
        // Just one partial unprocessed block and this was the last segment available, so nothing to
        // import
        if last_archived_block == *last_processed_block_number + One::one()
            && last_archived_block_partial
            && segment_indices_iter.peek().is_none()
        {
            // Reset reconstructor instance
            reconstructor = Reconstructor::new().map_err(|error| error.to_string())?;
            continue;
        }

        let blocks =
            download_and_reconstruct_blocks(segment_index, piece_provider, &mut reconstructor)
                .await?;

        let mut blocks_to_import = Vec::with_capacity(QUEUED_BLOCKS_LIMIT as usize);

        let mut best_block_number = info.best_number;
        for (block_number, block_bytes) in blocks {
            let block_number = block_number.into();
            if block_number == 0u32.into() {
                let block = client
                    .block(
                        client
                            .hash(block_number)?
                            .expect("Block before best block number must always be found; qed"),
                    )?
                    .expect("Block before best block number must always be found; qed")
                    .block;

                if block.encode() != block_bytes {
                    return Err(sc_service::Error::Other(
                        "Wrong genesis block, block import failed".to_string(),
                    ));
                }
            }

            // Limit number of queued blocks for import
            // NOTE: Since best block number might be non-canonical, we might actually have more
            // than `QUEUED_BLOCKS_LIMIT` elements in the queue, but it should be rare and
            // insignificant. Feel free to address this in case you have a good strategy, but it
            // seems like complexity is not worth it.
            while block_number.saturating_sub(best_block_number) >= QUEUED_BLOCKS_LIMIT.into() {
                if !blocks_to_import.is_empty() {
                    // Import queue handles verification and importing it into the client
                    import_queue_service
                        .import_blocks(BlockOrigin::NetworkInitialSync, blocks_to_import.clone());
                    blocks_to_import.clear();
                }
                trace!(
                    %block_number,
                    %best_block_number,
                    "Number of importing blocks reached queue limit, waiting before retrying"
                );
                tokio::time::sleep(WAIT_FOR_BLOCKS_TO_IMPORT).await;
                best_block_number = client.info().best_number;
            }

            let block =
                Block::decode(&mut block_bytes.as_slice()).map_err(|error| error.to_string())?;

            *last_processed_block_number = last_archived_block;

            // No need to import blocks that are already present, if block is not present it might
            // correspond to a short fork, so we need to import it even if we already have another
            // block at this height
            if client.expect_header(block.hash()).is_ok() {
                continue;
            }

            let (header, extrinsics) = block.deconstruct();
            let hash = header.hash();

            blocks_to_import.push(IncomingBlock {
                hash,
                header: Some(header),
                body: Some(extrinsics),
                indexed_body: None,
                justifications: None,
                origin: None,
                allow_missing_state: false,
                import_existing: false,
                state: None,
                skip_execution: false,
            });

            downloaded_blocks += 1;

            if downloaded_blocks % 1000 == 0 {
                debug!("Adding block {} from DSN to the import queue", block_number);
            }
        }

        // Import queue handles verification and importing it into the client
        let last_segment = segment_indices_iter.peek().is_none();
        if last_segment {
            let last_block = blocks_to_import
                .pop()
                .expect("Not empty, checked above; qed");
            import_queue_service.import_blocks(BlockOrigin::NetworkInitialSync, blocks_to_import);
            // This will notify Substrate's sync mechanism and allow regular Substrate sync to continue gracefully
            import_queue_service.import_blocks(BlockOrigin::NetworkBroadcast, vec![last_block]);
        } else {
            import_queue_service.import_blocks(BlockOrigin::NetworkInitialSync, blocks_to_import);
        }

        *last_processed_segment_index = segment_index;
    }

    Ok(downloaded_blocks)
}

async fn download_and_reconstruct_blocks<PV>(
    segment_index: SegmentIndex,
    piece_provider: &PieceProvider<PV>,
    reconstructor: &mut Reconstructor,
) -> Result<Vec<(BlockNumber, Vec<u8>)>, sc_service::Error>
where
    PV: PieceValidator,
{
    debug!(%segment_index, "Retrieving pieces of the segment");

    let semaphore = &Semaphore::new(RecordedHistorySegment::NUM_RAW_RECORDS);

    let mut received_segment_pieces = segment_index
        .segment_piece_indexes_source_first()
        .into_iter()
        .map(|piece_index| {
            // Source pieces will acquire permit here right away
            let maybe_permit = semaphore.try_acquire().ok();

            async move {
                let permit = match maybe_permit {
                    Some(permit) => permit,
                    None => {
                        // Other pieces will acquire permit here instead
                        match semaphore.acquire().await {
                            Ok(permit) => permit,
                            Err(error) => {
                                warn!(
                                    %piece_index,
                                    %error,
                                    "Semaphore was closed, interrupting piece retrieval"
                                );
                                return None;
                            }
                        }
                    }
                };
                let maybe_piece = match piece_provider
                    .get_piece(
                        piece_index,
                        RetryPolicy::Limited(PIECE_GETTER_RETRY_NUMBER.get()),
                    )
                    .await
                {
                    Ok(maybe_piece) => maybe_piece,
                    Err(error) => {
                        trace!(
                            %error,
                            ?piece_index,
                            "Piece request failed",
                        );
                        return None;
                    }
                };

                trace!(
                    ?piece_index,
                    piece_found = maybe_piece.is_some(),
                    "Piece request succeeded",
                );

                maybe_piece.map(|received_piece| {
                    // Piece was received successfully, "remove" this slot from semaphore
                    permit.forget();
                    (piece_index, received_piece)
                })
            }
        })
        .collect::<FuturesUnordered<_>>();

    let mut segment_pieces = vec![None::<Piece>; ArchivedHistorySegment::NUM_PIECES];
    let mut pieces_received = 0;

    while let Some(maybe_result) = received_segment_pieces.next().await {
        let Some((piece_index, piece)) = maybe_result else {
            continue;
        };

        segment_pieces
            .get_mut(piece_index.position() as usize)
            .expect("Piece position is by definition within segment; qed")
            .replace(piece);

        pieces_received += 1;

        if pieces_received >= RecordedHistorySegment::NUM_RAW_RECORDS {
            trace!(%segment_index, "Received half of the segment.");
            break;
        }
    }

    let reconstructed_contents = reconstructor
        .add_segment(segment_pieces.as_ref())
        .map_err(|error| error.to_string())?;
    drop(segment_pieces);

    trace!(%segment_index, "Segment reconstructed successfully");

    Ok(reconstructed_contents.blocks)
}
