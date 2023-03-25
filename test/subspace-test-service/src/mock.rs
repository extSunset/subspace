use crate::node_config;
use codec::{Decode, Encode};
use futures::channel::mpsc;
use futures::{select, FutureExt, StreamExt};
use sc_block_builder::BlockBuilderProvider;
use sc_client_api::{backend, BlockBackend, BlockchainEvents};
use sc_consensus::block_import::{
    BlockCheckParams, BlockImportParams, ForkChoiceStrategy, ImportResult,
};
use sc_consensus::{BlockImport, BoxBlockImport, StateAction};
use sc_consensus_subspace::notification::{
    self, SubspaceNotificationSender, SubspaceNotificationStream,
};
use sc_consensus_subspace::ImportedBlockNotification;
use sc_executor::NativeElseWasmExecutor;
use sc_service::{BasePath, InPoolTransaction, TaskManager, TransactionPool};
use sp_api::{ApiExt, HashT, HeaderT, ProvideRuntimeApi, TransactionFor};
use sp_application_crypto::UncheckedFrom;
use sp_blockchain::{HeaderBackend, HeaderMetadata};
use sp_consensus::{BlockOrigin, CacheKeyId, Error as ConsensusError, NoNetwork, SyncOracle};
use sp_consensus_slots::Slot;
use sp_consensus_subspace::digests::{CompatibleDigestItem, PreDigest};
use sp_consensus_subspace::FarmerPublicKey;
use sp_domains::{ExecutorApi, ExecutorPublicKey};
use sp_inherents::{InherentData, InherentDataProvider};
use sp_keyring::Sr25519Keyring;
use sp_runtime::generic::Digest;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT, NumberFor};
use sp_runtime::DigestItem;
use sp_timestamp::Timestamp;
use std::collections::{HashMap, VecDeque};
use std::error::Error;
use std::sync::Arc;
use std::time;
use subspace_core_primitives::{Blake2b256Hash, Solution};
use subspace_runtime_primitives::opaque::Block;
use subspace_runtime_primitives::{AccountId, Hash};
use subspace_service::FullSelectChain;
use subspace_solving::create_chunk_signature;
use subspace_test_client::{Backend, Client, FraudProofVerifier, TestExecutorDispatch};
use subspace_test_runtime::{RuntimeApi, RuntimeCall, UncheckedExtrinsic, SLOT_DURATION};
use subspace_transaction_pool::bundle_validator::BundleValidator;
use subspace_transaction_pool::FullPool;

type StorageChanges = sp_api::StorageChanges<backend::StateBackendFor<Backend, Block>, Block>;

/// A mock Subspace primary node instance used for testing.
pub struct MockPrimaryNode {
    /// `TaskManager`'s instance.
    pub task_manager: TaskManager,
    /// Client's instance.
    pub client: Arc<Client>,
    /// Backend.
    pub backend: Arc<Backend>,
    /// Code executor.
    pub executor: NativeElseWasmExecutor<TestExecutorDispatch>,
    /// Transaction pool.
    pub transaction_pool:
        Arc<FullPool<Block, Client, FraudProofVerifier, BundleValidator<Block, Client>>>,
    /// Block import pipeline
    pub block_import: BoxBlockImport<Block, TransactionFor<Client, Block>>,
    /// The SelectChain Strategy
    pub select_chain: FullSelectChain,
    /// The block import notification stream
    pub imported_block_notification_stream:
        SubspaceNotificationStream<ImportedBlockNotification<Block>>,
    /// The next slot number
    next_slot: u64,
    /// The slot notification stream
    pub new_slot_notification_stream: SubspaceNotificationStream<(Slot, Blake2b256Hash)>,
    /// The slot notification sender
    new_slot_notification_sender: SubspaceNotificationSender<(Slot, Blake2b256Hash)>,
    /// Mock subspace solution used to mock the subspace `PreDigest`
    mock_solution: Solution<FarmerPublicKey, AccountId>,
}

impl MockPrimaryNode {
    /// Run a mock primary node
    pub fn run_mock_primary_node(
        tokio_handle: tokio::runtime::Handle,
        key: Sr25519Keyring,
        base_path: BasePath,
    ) -> MockPrimaryNode {
        let config = node_config(tokio_handle, key, vec![], false, false, false, base_path);

        let executor = NativeElseWasmExecutor::<TestExecutorDispatch>::new(
            config.wasm_method,
            config.default_heap_pages,
            config.max_runtime_instances,
            config.runtime_cache_size,
        );

        let (client, backend, _, task_manager) =
            sc_service::new_full_parts::<Block, RuntimeApi, _>(&config, None, executor.clone())
                .expect("Fail to new full parts");

        let client = Arc::new(client);

        let select_chain = sc_consensus::LongestChain::new(backend.clone());

        let mut bundle_validator = BundleValidator::new(client.clone());

        let proof_verifier = subspace_fraud_proof::ProofVerifier::new(
            client.clone(),
            executor.clone(),
            task_manager.spawn_handle(),
        );
        let transaction_pool = subspace_transaction_pool::new_full(
            &config,
            &task_manager,
            client.clone(),
            proof_verifier.clone(),
            bundle_validator.clone(),
        );

        let mut imported_blocks_stream = client.import_notification_stream();
        task_manager.spawn_handle().spawn(
            "maintain-bundles-stored-in-last-k",
            None,
            Box::pin(async move {
                while let Some(incoming_block) = imported_blocks_stream.next().await {
                    if incoming_block.is_new_best {
                        bundle_validator.update_recent_stored_bundles(incoming_block.hash);
                    }
                }
            }),
        );

        // Inform the tx pool about imported and finalized blocks.
        task_manager.spawn_handle().spawn(
            "txpool-notifications",
            Some("transaction-pool"),
            sc_transaction_pool::notification_future(client.clone(), transaction_pool.clone()),
        );

        // TODO: check whether workers are needed: offchain-on-block, offchain-notifications,, on-transaction-imported
        // informant

        let fraud_proof_block_import =
            sc_consensus_fraud_proof::block_import(client.clone(), client.clone(), proof_verifier);

        let (imported_block_notification_sender, imported_block_notification_stream) =
            notification::channel("subspace_new_slot_notification_stream");

        let block_import = Box::new(MockBlockImport::<_, Client, _>::new(
            fraud_proof_block_import,
            client.clone(),
            imported_block_notification_sender,
        ));

        let (new_slot_notification_sender, new_slot_notification_stream) =
            notification::channel("subspace_new_slot_notification_stream");

        let mock_solution = {
            let mut gs = Solution::genesis_solution(
                FarmerPublicKey::unchecked_from(key.public().0),
                key.to_account_id(),
            );
            gs.chunk_signature = create_chunk_signature(&key.pair().into(), &gs.chunk.to_bytes());
            gs
        };

        MockPrimaryNode {
            task_manager,
            client,
            backend,
            executor,
            transaction_pool,
            block_import,
            select_chain,
            imported_block_notification_stream,
            next_slot: 1,
            new_slot_notification_sender,
            new_slot_notification_stream,
            mock_solution,
        }
    }

    /// Sync oracle for `MockPrimaryNode`
    pub fn sync_oracle() -> Arc<dyn SyncOracle + Send + Sync> {
        Arc::new(NoNetwork)
    }

    /// Sync block from other peer
    pub async fn sync_from_peer(&mut self, peer: &MockPrimaryNode) -> Result<(), Box<dyn Error>> {
        let (local_info, peer_info) = (self.client.info(), peer.client.info());
        // Primary chain use longest chain for chain selection thus return directly
        // if we have longer chain.
        if local_info.best_number >= peer_info.best_number {
            return Ok(());
        }
        let mut enacted = VecDeque::new();
        let mut local_block = self.client.header_metadata(local_info.best_hash)?;
        let mut peer_block = peer.client.header_metadata(peer_info.best_hash)?;
        while peer_block.number > local_block.number {
            enacted.push_front(peer_block.hash);
            peer_block = peer.client.header_metadata(peer_block.parent)?;
        }
        while peer_block.hash != local_block.hash {
            enacted.push_front(peer_block.hash);
            peer_block = peer.client.header_metadata(peer_block.parent)?;
            local_block = self.client.header_metadata(local_block.parent)?;
        }
        for h in enacted {
            let block = peer
                .client
                .block(h)?
                .expect("should able to get block body")
                .block;

            let pre_digest = block
                .header()
                .digest()
                .logs()
                .iter()
                .find_map(|s| s.as_subspace_pre_digest::<AccountId>())
                .expect("Block must always have pre-digest");

            self.import_block(block, None).await?;
            self.next_slot = <Slot as Into<u64>>::into(pre_digest.slot) + 1;
        }
        Ok(())
    }

    /// Wait until a bundle that created by `author_key` at `slot` have submitted to the
    /// transaction pool
    pub async fn wait_for_bundle<SClient, SBlock>(
        &mut self,
        author_key: Sr25519Keyring,
        system_domain_client: &Arc<SClient>,
    ) -> Result<<Block as BlockT>::Hash, Box<dyn Error>>
    where
        SBlock: BlockT,
        SClient: HeaderBackend<SBlock>,
    {
        self.wait_system_domain_catch_up(system_domain_client).await?;

        let slot = self.produce_slot().into();

        let author_key = ExecutorPublicKey::unchecked_from(author_key.public().0);
        // Check if bundle is already present at the transaction pool
        for ready_tx in self.transaction_pool.ready() {
            if is_bundle_match(ready_tx.data.encode().as_slice(), slot, &author_key)? {
                return Ok(ready_tx.hash);
            }
        }
        // Check incoming transactions
        let mut import_tx_stream = self.transaction_pool.import_notification_stream();
        while let Some(ready_tx_hash) = import_tx_stream.next().await {
            let tx = self
                .transaction_pool
                .ready_transaction(&ready_tx_hash)
                .expect("tx must exist as we just got notified; ped");
            if is_bundle_match(tx.data.encode().as_slice(), slot, &author_key)? {
                return Ok(ready_tx_hash);
            }
        }
        unreachable!()
    }

    pub async fn wait_system_domain_catch_up<SClient, SBlock>(
        &self,
        system_domain_client: &Arc<SClient>,
    ) -> Result<(), Box<dyn Error>>
    where
        SBlock: BlockT,
        SClient: HeaderBackend<SBlock>,
    {
        let head_receipt_number = self
            .client
            .runtime_api()
            .head_receipt_number(self.client.info().best_hash)?;
        for _ in 0..100 {
            if system_domain_client.info().best_number > head_receipt_number.into() {
                return Ok(());
            }
            tokio::time::sleep(time::Duration::from_millis(10)).await;
        }
        panic!("System domain fail to catch up");
    }

    async fn collect_txn_from_pool(
        &self,
        parent_number: NumberFor<Block>,
    ) -> Vec<<Block as BlockT>::Extrinsic> {
        let mut t1 = self.transaction_pool.ready_at(parent_number).fuse();
        let mut t2 = futures_timer::Delay::new(time::Duration::from_micros(100)).fuse();
        let pending_iterator = select! {
            res = t1 => res,
            _ = t2 => {
                tracing::warn!(
                    "Timeout fired waiting for transaction pool at #{}, proceeding with production.",
                    parent_number,
                );
                self.transaction_pool.ready()
            }
        };
        let pushing_duration = time::Duration::from_micros(500);
        let start = time::Instant::now();
        let mut extrinsics = Vec::new();
        for pending_tx in pending_iterator {
            if start.elapsed() >= pushing_duration {
                break;
            }
            let pending_tx_data = pending_tx.data().clone();
            extrinsics.push(pending_tx_data);
        }
        extrinsics
    }

    async fn mock_inherent_data(slot: Slot) -> Result<InherentData, Box<dyn Error>> {
        let timestamp = sp_timestamp::InherentDataProvider::new(Timestamp::new(
            <Slot as Into<u64>>::into(slot) * SLOT_DURATION,
        ));
        let subspace_inherents =
            sp_consensus_subspace::inherents::InherentDataProvider::new(slot, vec![]);

        let inherent_data = (subspace_inherents, timestamp)
            .create_inherent_data()
            .await?;

        Ok(inherent_data)
    }

    fn mock_subspace_digest(&self, slot: Slot) -> Digest {
        let pre_digest: PreDigest<FarmerPublicKey, AccountId> = PreDigest {
            slot,
            solution: self.mock_solution.clone(),
        };
        let mut digest = Digest::default();
        digest.push(DigestItem::subspace_pre_digest(&pre_digest));
        digest
    }

    /// Prepare components that used to build block
    pub async fn prepare_block(&mut self) -> BlockParams {
        let slot = self.produce_slot();
        let parent_hash = self.client.info().best_hash;
        let extrinsics = self
            .collect_txn_from_pool(self.client.info().best_number)
            .await;
        BlockParams {
            slot,
            parent_hash,
            extrinsics,
        }
    }

    /// Prepare components that used to build block with the given `extrinsics`
    pub fn prepare_block_with_extrinsics(
        &mut self,
        extrinsics: Vec<<Block as BlockT>::Extrinsic>,
    ) -> BlockParams {
        let slot = self.produce_slot();
        let parent_hash = self.client.info().best_hash;
        BlockParams {
            slot,
            parent_hash,
            extrinsics,
        }
    }

    /// Build block
    pub async fn build_block(
        &self,
        block_params: BlockParams,
    ) -> Result<(Block, StorageChanges), Box<dyn Error>> {
        let BlockParams {
            slot,
            parent_hash,
            extrinsics,
        } = block_params;

        let digest = self.mock_subspace_digest(slot);
        let inherent_data = Self::mock_inherent_data(slot).await?;

        let mut block_builder = self.client.new_block_at(parent_hash, digest, false)?;

        let inherent_txns = block_builder.create_inherents(inherent_data)?;

        for tx in inherent_txns.into_iter().chain(extrinsics) {
            sc_block_builder::BlockBuilder::push(&mut block_builder, tx)?;
        }

        let (block, storage_changes, _) = block_builder.build()?.into_inner();
        Ok((block, storage_changes))
    }

    /// Import block
    pub async fn import_block(
        &mut self,
        block: Block,
        storage_changes: Option<StorageChanges>,
    ) -> Result<(), Box<dyn Error>> {
        let (header, body) = block.deconstruct();
        let block_import_params = {
            let mut import_block = BlockImportParams::new(BlockOrigin::Own, header);
            import_block.body = Some(body);
            import_block.state_action = match storage_changes {
                Some(changes) => {
                    StateAction::ApplyChanges(sc_consensus::StorageChanges::Changes(changes))
                }
                None => StateAction::Execute,
            };
            import_block
        };

        let import_result = self
            .block_import
            .import_block(block_import_params, Default::default())
            .await?;

        match import_result {
            ImportResult::Imported(_) | ImportResult::AlreadyInChain => Ok(()),
            bad_res => Err(format!("Fail to import block due to {bad_res:?}").into()),
        }
    }

    /// Produce block with the given `BlockParams`
    pub async fn produce_block_with(
        &mut self,
        block_params: BlockParams,
    ) -> Result<(), Box<dyn Error>> {
        let block_timer = time::Instant::now();

        let (block, storage_changes) = self.build_block(block_params).await?;

        tracing::info!(
			"🎁 Prepared block for proposing at {} ({} ms) [hash: {:?}; parent_hash: {}; extrinsics ({}): [{}]]",
			block.header().number(),
			block_timer.elapsed().as_millis(),
			block.header().hash(),
			block.header().parent_hash(),
			block.extrinsics().len(),
			block.extrinsics()
				.iter()
				.map(|xt| BlakeTwo256::hash_of(xt).to_string())
				.collect::<Vec<_>>()
				.join(", ")
		);

        self.import_block(block, Some(storage_changes)).await?;

        Ok(())
    }

    /// Produce block based on the current best block and the extrinsics in pool
    pub async fn produce_block(&mut self) -> Result<(), Box<dyn Error>> {
        let block_params = self.prepare_block().await;
        self.produce_block_with(block_params).await?;
        Ok(())
    }

    /// Produce `n` number of blocks.
    pub async fn produce_n_blocks(&mut self, n: u64) -> Result<(), Box<dyn Error>> {
        for _ in 0..n {
            self.produce_block().await?;
        }
        Ok(())
    }

    /// Return the next slot number
    pub fn next_slot(&self) -> u64 {
        self.next_slot
    }

    /// Produce slot
    pub fn produce_slot(&mut self) -> Slot {
        let slot = Slot::from(self.next_slot);
        self.next_slot += 1;

        self.new_slot_notification_sender
            .notify(|| (slot, Hash::random().into()));

        slot
    }
}

/// `BlockParams` consist of components that used to construct block
pub struct BlockParams {
    /// The slot that the block producing at
    pub slot: Slot,
    /// The parent hash of the block
    pub parent_hash: <Block as BlockT>::Hash,
    /// The extrinsics of the block
    pub extrinsics: Vec<<Block as BlockT>::Extrinsic>,
}

fn is_bundle_match(
    mut tx_data: &[u8],
    slot: u64,
    author_key: &ExecutorPublicKey,
) -> Result<bool, Box<dyn Error>> {
    match UncheckedExtrinsic::decode(&mut tx_data)?.function {
        RuntimeCall::Domains(pallet_domains::Call::submit_bundle {
            signed_opaque_bundle,
        }) => {
            let slot_match = signed_opaque_bundle.bundle.header.slot_number == slot;
            let author_match = signed_opaque_bundle
                .bundle_solution
                .proof_of_election()
                .executor_public_key
                == *author_key;
            Ok(slot_match && author_match)
        }
        _ => Ok(false),
    }
}

// `MockBlockImport` is mostly port from `sc-consensus-subspace::SubspaceBlockImport` with all
// the consensus related logic removed.
struct MockBlockImport<Inner, Client, Block: BlockT> {
    inner: Inner,
    client: Arc<Client>,
    imported_block_notification_sender:
        SubspaceNotificationSender<ImportedBlockNotification<Block>>,
}

impl<Inner, Client, Block: BlockT> MockBlockImport<Inner, Client, Block> {
    fn new(
        inner: Inner,
        client: Arc<Client>,
        imported_block_notification_sender: SubspaceNotificationSender<
            ImportedBlockNotification<Block>,
        >,
    ) -> Self {
        MockBlockImport {
            inner,
            client,
            imported_block_notification_sender,
        }
    }
}

#[async_trait::async_trait]
impl<Inner, Client, Block> BlockImport<Block> for MockBlockImport<Inner, Client, Block>
where
    Block: BlockT,
    Inner: BlockImport<Block, Transaction = TransactionFor<Client, Block>, Error = ConsensusError>
        + Send
        + Sync,
    Inner::Error: Into<ConsensusError>,
    Client: ProvideRuntimeApi<Block> + HeaderBackend<Block> + Send + Sync + 'static,
    Client::Api: ApiExt<Block>,
{
    type Error = ConsensusError;
    type Transaction = TransactionFor<Client, Block>;

    async fn import_block(
        &mut self,
        mut block: BlockImportParams<Block, Self::Transaction>,
        new_cache: HashMap<CacheKeyId, Vec<u8>>,
    ) -> Result<ImportResult, Self::Error> {
        let block_number = *block.header.number();
        let current_best_number = self.client.info().best_number;
        block.fork_choice = Some(ForkChoiceStrategy::Custom(
            block_number > current_best_number,
        ));

        let import_result = self.inner.import_block(block, new_cache).await?;
        let (block_import_acknowledgement_sender, mut block_import_acknowledgement_receiver) =
            mpsc::channel(0);

        self.imported_block_notification_sender
            .notify(move || ImportedBlockNotification {
                block_number,
                block_import_acknowledgement_sender,
            });

        while (block_import_acknowledgement_receiver.next().await).is_some() {
            // Wait for all the acknowledgements to progress.
        }

        Ok(import_result)
    }

    async fn check_block(
        &mut self,
        block: BlockCheckParams<Block>,
    ) -> Result<ImportResult, Self::Error> {
        self.inner.check_block(block).await.map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sp_keyring::sr25519::Keyring::{Eve, Ferdie};
    use tempfile::TempDir;

    #[substrate_test_utils::test(flavor = "multi_thread")]
    async fn test_sync_from_peer() {
        let directory = TempDir::new().expect("Must be able to create temporary directory");

        let mut builder = sc_cli::LoggerBuilder::new("");
        builder.with_colors(false);
        let _ = builder.init();

        let tokio_handle = tokio::runtime::Handle::current();

        // Start Ferdie
        let mut ferdie = MockPrimaryNode::run_mock_primary_node(
            tokio_handle.clone(),
            Ferdie,
            BasePath::new(directory.path().join("ferdie")),
        );

        // Start Eve
        let mut eve = MockPrimaryNode::run_mock_primary_node(
            tokio_handle.clone(),
            Eve,
            BasePath::new(directory.path().join("eve")),
        );

        ferdie.produce_n_blocks(3).await.unwrap();
        eve.produce_n_blocks(10).await.unwrap();
        assert_ne!(ferdie.client.info().best_hash, eve.client.info().best_hash);

        // `sync_from_peer` will be no-op since `eve` have longer chain
        let pre_eve_best_hash = eve.client.info().best_hash;
        eve.sync_from_peer(&ferdie).await.unwrap();
        assert_eq!(pre_eve_best_hash, eve.client.info().best_hash);

        // `ferdie` is able to sync from `eve`
        ferdie.sync_from_peer(&eve).await.unwrap();
        let ferdie_info = ferdie.client.info();
        assert_eq!(ferdie_info.best_number, 10);
        assert_eq!(ferdie_info.best_hash, eve.client.info().best_hash);

        // `ferdie` is able to prudoce more block after sync
        ferdie.produce_n_blocks(1).await.unwrap();
    }
}
