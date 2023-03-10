use crate::node_config;
use futures::channel::mpsc;
use futures::{select, FutureExt, StreamExt};
use sc_block_builder::BlockBuilderProvider;
use sc_client_api::backend;
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
use sp_blockchain::HeaderBackend;
use sp_consensus::{BlockOrigin, CacheKeyId, Error as ConsensusError, NoNetwork, SyncOracle};
use sp_consensus_slots::Slot;
use sp_consensus_subspace::digests::{CompatibleDigestItem, PreDigest};
use sp_consensus_subspace::FarmerPublicKey;
use sp_inherents::{InherentData, InherentDataProvider};
use sp_keyring::Sr25519Keyring;
use sp_runtime::generic::{BlockId, Digest};
use sp_runtime::traits::{BlakeTwo256, Block as BlockT, NumberFor};
use sp_runtime::DigestItem;
use sp_timestamp::Timestamp;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::time;
use subspace_core_primitives::{Blake2b256Hash, Solution};
use subspace_runtime_primitives::opaque::Block;
use subspace_runtime_primitives::{AccountId, Hash};
use subspace_service::FullSelectChain;
use subspace_solving::create_chunk_signature;
use subspace_test_client::{Backend, Client, FraudProofVerifier, TestExecutorDispatch};
use subspace_test_runtime::{RuntimeApi, SLOT_DURATION};
use subspace_transaction_pool::bundle_validator::BundleValidator;
use subspace_transaction_pool::FullPool;

type StorageChanges = sp_api::StorageChanges<backend::StateBackendFor<Backend, Block>, Block>;

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

    pub select_chain: FullSelectChain,

    pub imported_block_notification_stream:
        SubspaceNotificationStream<ImportedBlockNotification<Block>>,

    pub manual_slot: ManualSlot,

    genesis_solution: Solution<FarmerPublicKey, AccountId>,
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

        let bundle_validator = BundleValidator::new(client.clone());

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

        let fraud_proof_block_import =
            sc_consensus_fraud_proof::block_import(client.clone(), client.clone(), proof_verifier);

        let (imported_block_notification_sender, imported_block_notification_stream) =
            notification::channel("subspace_new_slot_notification_stream");

        let block_import = Box::new(MockBlockImport::new(
            fraud_proof_block_import,
            client.clone(),
            imported_block_notification_sender,
        ));

        let manual_slot = ManualSlot::new();

        let genesis_solution = {
            let mut gs = Solution::genesis_solution(
                FarmerPublicKey::unchecked_from(key.public().0),
                key.to_account_id(),
            );
            gs.chunk_signature = create_chunk_signature(key.pair().as_ref(), &gs.chunk.to_bytes());
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
            manual_slot,
            genesis_solution,
        }
    }

    pub fn sync_oracle() -> Arc<dyn SyncOracle + Send + Sync> {
        Arc::new(NoNetwork)
    }

    async fn collect_txn_from_pool(
        &self,
        parent_number: NumberFor<Block>,
    ) -> Vec<<Block as BlockT>::Extrinsic> {
        let mut t1 = self.transaction_pool.ready_at(parent_number).fuse();
        let mut t2 = futures_timer::Delay::new(time::Duration::from_millis(5)).fuse();
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
        // TODO: limit the number of txn
        let pushing_duration = time::Duration::from_millis(1);
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
            solution: self.genesis_solution.clone(),
        };
        let mut digest = Digest::default();
        digest.push(DigestItem::subspace_pre_digest(&pre_digest));
        digest
    }

    async fn build_block(
        &self,
        slot: Slot,
        parent_hash: <Block as BlockT>::Hash,
        extrinsics: Vec<<Block as BlockT>::Extrinsic>,
    ) -> Result<(Block, StorageChanges), Box<dyn Error>> {
        let digest = self.mock_subspace_digest(slot);

        let mut block_builder =
            self.client
                .new_block_at(&BlockId::Hash(parent_hash), digest, false)?;

        let inherents = block_builder.create_inherents(Self::mock_inherent_data(slot).await?)?;

        for tx in inherents.into_iter().chain(extrinsics) {
            if let Err(err) = sc_block_builder::BlockBuilder::push(&mut block_builder, tx) {
                tracing::debug!("Got error {:?} while building block", err);
            }
        }

        let (block, storage_changes, _) = block_builder.build()?.into_inner();
        Ok((block, storage_changes))
    }

    async fn import_block(
        &mut self,
        block: Block,
        storage_changes: StorageChanges,
    ) -> Result<(), Box<dyn Error>> {
        let (header, body) = block.deconstruct();
        let block_import_params = {
            let mut import_block = BlockImportParams::new(BlockOrigin::Own, header);
            import_block.body = Some(body);
            import_block.state_action =
                StateAction::ApplyChanges(sc_consensus::StorageChanges::Changes(storage_changes));
            import_block
        };

        let import_result = self
            .block_import
            .import_block(block_import_params, Default::default())
            .await?;

        match import_result {
            ImportResult::Imported(_) | ImportResult::AlreadyInChain => Ok(()),
            bad_res => Err(format!("Fail to import block due to {:?}", bad_res).into()),
        }
    }

    pub async fn produce_n_block_and_slot(&mut self, num: u64) -> Result<(), Box<dyn Error>> {
        for _ in 0..num {
            let slot = self.produce_slot();
            if let Err(err) = self.produce_block(slot).await {
                panic!("produce_block err {err:?}");
            }
        }
        Ok(())
    }

    /// Produce block based on the current best block and the extrinsics with pool
    pub async fn produce_block(&mut self, slot: Slot) -> Result<(), Box<dyn Error>> {
        let block_timer = time::Instant::now();
        let parent_hash = self.client.info().best_hash;
        let paretn_number = self.client.info().best_number;

        let extrinsics = self.collect_txn_from_pool(paretn_number).await;

        let (block, storage_changes) = self.build_block(slot, parent_hash, extrinsics).await?;

        tracing::info!(
			"🎁 Prepared block for proposing at {} ({} ms) [hash: {:?}; parent_hash: {}; extrinsics ({}): [{}]]",
			block.header().number(),
			block_timer.elapsed().as_millis(),
			<Block as BlockT>::Hash::from(block.header().hash()),
			block.header().parent_hash(),
			block.extrinsics().len(),
			block.extrinsics()
				.iter()
				.map(|xt| BlakeTwo256::hash_of(xt).to_string())
				.collect::<Vec<_>>()
				.join(", ")
		);

        self.import_block(block, storage_changes).await?;

        Ok(())
    }

    /// Produce block based on the given `extrinsics`
    pub async fn produce_block_with_extrinsics(
        &mut self,
        extrinsics: Vec<<Block as BlockT>::Extrinsic>,
    ) -> Result<(), Box<dyn Error>> {
        let slot = self.produce_slot();
        self.produce_block_with(slot, self.client.info().best_hash, vec![], extrinsics)
            .await
    }

    /// Produce block based on the given `parent_hash` and `extrinsics`
    pub async fn produce_block_with(
        &mut self,
        slot: Slot,
        parent_hash: <Block as BlockT>::Hash,
        digest_items: Vec<DigestItem>,
        extrinsics: Vec<<Block as BlockT>::Extrinsic>,
    ) -> Result<(), Box<dyn Error>> {
        let (mut block, storage_changes) = self.build_block(slot, parent_hash, extrinsics).await?;

        // When `DigestItem::RuntimeEnvironmentUpdated` used as `inherent_digests`
        // it will not present at the block header thus we need to manually inject
        // digest item here.
        for i in digest_items {
            block.header.digest_mut().push(i);
        }

        self.import_block(block, storage_changes).await?;
        Ok(())
    }

    pub fn produce_slot(&mut self) -> Slot {
        self.manual_slot.produce_slot()
    }

    pub fn produce_slot_without_notify(&mut self) -> Slot {
        self.manual_slot.produce_slot_without_notify()
    }
}

pub struct ManualSlot {
    next_slot: u64,
    pub new_slot_notification_stream: SubspaceNotificationStream<(Slot, Blake2b256Hash)>,
    new_slot_notification_sender: SubspaceNotificationSender<(Slot, Blake2b256Hash)>,
}

impl ManualSlot {
    fn new() -> Self {
        let (new_slot_notification_sender, new_slot_notification_stream) =
            notification::channel("subspace_new_slot_notification_stream");
        ManualSlot {
            next_slot: 1,
            new_slot_notification_sender,
            new_slot_notification_stream,
        }
    }

    pub fn produce_slot(&mut self) -> Slot {
        let slot = Slot::from(self.next_slot);
        self.next_slot += 1;

        self.new_slot_notification_sender
            .notify(|| (slot, Hash::random().into()));

        slot
    }

    pub fn produce_slot_without_notify(&mut self) -> Slot {
        let slot = Slot::from(self.next_slot);
        self.next_slot += 1;
        slot
    }
}

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
        let fork_choice = ForkChoiceStrategy::Custom(block_number > current_best_number);
        block.fork_choice = Some(fork_choice);

        let import_result = self.inner.import_block(block, new_cache).await?;
        let (block_import_acknowledgement_sender, mut block_import_acknowledgement_receiver) =
            mpsc::channel(0);

        self.imported_block_notification_sender
            .notify(move || ImportedBlockNotification {
                block_number,
                fork_choice,
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
