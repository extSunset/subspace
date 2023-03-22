use crate::utils::{to_number_primitive, BlockInfo, ExecutorSlotInfo};
use crate::BlockImportNotificationForExecutor;
use codec::{Decode, Encode};
use futures::channel::mpsc;
use futures::{SinkExt, Stream, StreamExt};
use sc_client_api::{BlockBackend, BlockImportNotification, BlockchainEvents};
use sp_api::{ApiError, BlockT, ProvideRuntimeApi};
use sp_blockchain::HeaderBackend;
use sp_domains::{ExecutorApi, SignedOpaqueBundle};
use sp_runtime::traits::{Header as HeaderT, NumberFor, One, Saturating};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

pub(crate) async fn handle_slot_notifications<Block, PBlock, PClient, BundlerFn>(
    primary_chain_client: &PClient,
    bundler: BundlerFn,
    mut slots: impl Stream<Item = ExecutorSlotInfo> + Unpin,
) where
    Block: BlockT,
    PBlock: BlockT,
    PClient: HeaderBackend<PBlock> + ProvideRuntimeApi<PBlock>,
    PClient::Api: ExecutorApi<PBlock, Block::Hash>,
    BundlerFn: Fn(
            (PBlock::Hash, NumberFor<PBlock>),
            ExecutorSlotInfo,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Option<
                            SignedOpaqueBundle<NumberFor<PBlock>, PBlock::Hash, Block::Hash>,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
{
    while let Some(executor_slot_info) = slots.next().await {
        if let Err(error) =
            on_new_slot::<Block, PBlock, _, _>(primary_chain_client, &bundler, executor_slot_info)
                .await
        {
            tracing::error!(?error, "Failed to submit bundle");
            break;
        }
    }
}

pub(crate) async fn handle_block_import_notifications<
    Block,
    PBlock,
    PClient,
    ProcessorFn,
    IBNS,
    EINS,
>(
    primary_chain_client: &PClient,
    best_domain_number: NumberFor<Block>,
    processor: ProcessorFn,
    mut leaves: Vec<(PBlock::Hash, NumberFor<PBlock>)>,
    block_import_notification: BlockImportNotificationForExecutor<IBNS, EINS>,
) where
    Block: BlockT,
    PBlock: BlockT,
    PClient: HeaderBackend<PBlock>
        + BlockBackend<PBlock>
        + ProvideRuntimeApi<PBlock>
        + BlockchainEvents<PBlock>,
    PClient::Api: ExecutorApi<PBlock, Block::Hash>,
    ProcessorFn: Fn(
            (PBlock::Hash, NumberFor<PBlock>),
        ) -> Pin<Box<dyn Future<Output = Result<(), sp_blockchain::Error>> + Send>>
        + Send
        + Sync,
    IBNS: Stream<Item = (NumberFor<PBlock>, mpsc::Sender<()>)> + Send + 'static,
    EINS: Stream<Item = BlockImportNotification<PBlock>> + Send + 'static,
{
    let mut active_leaves = HashMap::with_capacity(leaves.len());

    let best_domain_number = to_number_primitive(best_domain_number);

    // Notify about active leaves on startup before starting the loop
    for (hash, number) in std::mem::take(&mut leaves) {
        let _ = active_leaves.insert(hash, number);
        // Skip the blocks that have been processed by the execution chain.
        if number > best_domain_number.into() {
            if let Err(error) = processor((hash, number)).await {
                tracing::error!(?error, "Failed to process primary block on startup");
                // Bring down the service as bundles processor is an essential task.
                // TODO: more graceful shutdown.
                return;
            }
        }
    }

    let BlockImportNotificationForExecutor {
        block_import_throttling_buffer_size,
        imported_block_notification_stream,
        every_import_notification_stream,
    } = block_import_notification;
    let mut client_block_import = Box::pin(every_import_notification_stream);
    let mut block_imports = Box::pin(imported_block_notification_stream);

    // The primary chain can be ahead of the domain by up to `block_import_throttling_buffer_size/2`
    // blocks, for there are two notifications per block sent to this buffer (one will be actually
    // consumed by the domain processor, the other from `sc-consensus-subspace` is used to discontinue
    // the primary block import in case the primary chain runs much faster than the domain.).
    let (mut block_info_sender, mut block_info_receiver) =
        mpsc::channel(block_import_throttling_buffer_size as usize);

    loop {
        tokio::select! {
            maybe_client_block_import = client_block_import.next() => {
                let notification = match maybe_client_block_import {
                    Some(block_import) => block_import,
                    None => {
                        // Can be None on graceful shutdown.
                        break;
                    }
                };
                let header = match primary_chain_client.header(notification.hash) {
                    Ok(Some(header)) => header,
                    res => {
                        tracing::error!(
                            result = ?res,
                            header = ?notification.header,
                            "Imported primary block header not found",
                        );
                        return;
                    }
                };
                let block_info = BlockInfo {
                    hash: header.hash(),
                    parent_hash: *header.parent_hash(),
                    number: *header.number(),
                };
                let _ = block_info_sender.feed(Some(block_info)).await;
            }
            maybe_subspace_block_import = block_imports.next() => {
                let (_block_number, mut block_import_acknowledgement_sender) =
                    match maybe_subspace_block_import {
                        Some(block_import) => block_import,
                        None => {
                            // Can be None on graceful shutdown.
                            break;
                        }
                    };
                // Pause the primary block import when the sink is full.
                let _ = block_info_sender.feed(None).await;
                let _ = block_import_acknowledgement_sender.send(()).await;
            }
            Some(maybe_block_info) = block_info_receiver.next() => {
                if let Some(block_info) = maybe_block_info {
                    if let Err(error) = block_imported::<Block, PBlock, _>(
                        &processor,
                        &mut active_leaves,
                        block_info,
                    ).await {
                        tracing::error!(?error, "Failed to process primary block");
                        // Bring down the service as bundles processor is an essential task.
                        // TODO: more graceful shutdown.
                        break;
                    }
                }
            }
        }
    }
}

async fn on_new_slot<Block, PBlock, PClient, BundlerFn>(
    primary_chain_client: &PClient,
    bundler: &BundlerFn,
    executor_slot_info: ExecutorSlotInfo,
) -> Result<(), ApiError>
where
    Block: BlockT,
    PBlock: BlockT,
    PClient: HeaderBackend<PBlock> + ProvideRuntimeApi<PBlock>,
    PClient::Api: ExecutorApi<PBlock, Block::Hash>,
    BundlerFn: Fn(
            (PBlock::Hash, NumberFor<PBlock>),
            ExecutorSlotInfo,
        ) -> Pin<
            Box<
                dyn Future<
                        Output = Option<
                            SignedOpaqueBundle<NumberFor<PBlock>, PBlock::Hash, Block::Hash>,
                        >,
                    > + Send,
            >,
        > + Send
        + Sync,
{
    let best_hash = primary_chain_client.info().best_hash;
    let best_number = primary_chain_client.info().best_number;

    let best_hash = PBlock::Hash::decode(&mut best_hash.encode().as_slice())
        .expect("Hash type must be correct");
    let best_number = crate::utils::translate_number_type(best_number);

    let opaque_bundle = match bundler((best_hash, best_number), executor_slot_info).await {
        Some(opaque_bundle) => opaque_bundle,
        None => {
            tracing::debug!("executor returned no bundle on bundling");
            return Ok(());
        }
    };

    primary_chain_client
        .runtime_api()
        .submit_bundle_unsigned(best_hash, opaque_bundle)?;

    Ok(())
}

async fn block_imported<Block, PBlock, ProcessorFn>(
    processor: &ProcessorFn,
    active_leaves: &mut HashMap<PBlock::Hash, NumberFor<PBlock>>,
    block_info: BlockInfo<PBlock>,
) -> Result<(), ApiError>
where
    Block: BlockT,
    PBlock: BlockT,
    ProcessorFn: Fn(
            (PBlock::Hash, NumberFor<PBlock>),
        ) -> Pin<Box<dyn Future<Output = Result<(), sp_blockchain::Error>> + Send>>
        + Send
        + Sync,
{
    match active_leaves.entry(block_info.hash) {
        Entry::Vacant(entry) => entry.insert(block_info.number),
        Entry::Occupied(entry) => {
            debug_assert_eq!(*entry.get(), block_info.number);
            return Ok(());
        }
    };

    if let Some(number) = active_leaves.remove(&block_info.parent_hash) {
        debug_assert_eq!(block_info.number.saturating_sub(One::one()), number);
    }

    processor((block_info.hash, block_info.number)).await?;

    Ok(())
}
