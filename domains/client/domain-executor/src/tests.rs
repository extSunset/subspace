use codec::{Decode, Encode};
use domain_runtime_primitives::{DomainCoreApi, Hash};
use domain_test_service::run_mock_primary_chain_validator_node;
use domain_test_service::runtime::{Header, UncheckedExtrinsic};
use domain_test_service::Keyring::{Alice, Bob, Ferdie};
use sc_client_api::{BlockBackend, HeaderBackend};
use sc_executor_common::runtime_blob::RuntimeBlob;
use sc_service::{BasePath, Role};
use sc_transaction_pool_api::TransactionSource;
use sp_api::ProvideRuntimeApi;
use sp_core::Pair;
use sp_domain_digests::AsPredigest;
use sp_domains::fraud_proof::{ExecutionPhase, FraudProof, InvalidStateTransitionProof};
use sp_domains::transaction::InvalidTransactionCode;
use sp_domains::{
    Bundle, BundleHeader, BundleSolution, DomainId, ExecutorApi, ExecutorPair, ProofOfElection,
    SignedBundle,
};
use sp_runtime::generic::{BlockId, Digest, DigestItem};
use sp_runtime::traits::{BlakeTwo256, Hash as HashT, Header as HeaderT};
use std::time::Duration;
use subspace_core_primitives::BlockNumber;
use subspace_wasm_tools::read_core_domain_runtime_blob;
use tempfile::TempDir;

#[substrate_test_utils::test(flavor = "multi_thread")]
async fn test_executor_full_node_catching_up() {
    let directory = TempDir::new().expect("Must be able to create temporary directory");

    let mut builder = sc_cli::LoggerBuilder::new("");
    builder.with_colors(false);
    let _ = builder.init();

    let tokio_handle = tokio::runtime::Handle::current();

    // Start Ferdie
    let mut ferdie = run_mock_primary_chain_validator_node(
        tokio_handle.clone(),
        Ferdie,
        BasePath::new(directory.path().join("ferdie")),
    )
    .await;

    // Run Alice (a system domain authority node)
    let alice = domain_test_service::SystemDomainNodeBuilder::new(
        tokio_handle.clone(),
        Alice,
        BasePath::new(directory.path().join("alice")),
    )
    .build_with_mock(Role::Authority, &ferdie)
    .await;

    // Run Bob (a system domain full node)
    let bob = domain_test_service::SystemDomainNodeBuilder::new(
        tokio_handle,
        Bob,
        BasePath::new(directory.path().join("bob")),
    )
    .build_with_mock(Role::Full, &ferdie)
    .await;

    // Bob is able to sync blocks.
    futures::join!(
        alice.wait_for_blocks(3),
        bob.wait_for_blocks(3),
        ferdie.produce_n_block_and_slot(3),
    )
    .2
    .unwrap();

    let alice_block_hash = alice
        .client
        .expect_block_hash_from_id(&BlockId::Number(2))
        .unwrap();
    let bob_block_hash = bob
        .client
        .expect_block_hash_from_id(&BlockId::Number(2))
        .unwrap();
    assert_eq!(
        alice_block_hash, bob_block_hash,
        "Executor authority node and full node must have the same state"
    );
}

#[substrate_test_utils::test(flavor = "multi_thread")]
async fn fraud_proof_verification_in_tx_pool_should_work() {
    let directory = TempDir::new().expect("Must be able to create temporary directory");

    let mut builder = sc_cli::LoggerBuilder::new("");
    builder.with_colors(false);
    let _ = builder.init();

    let tokio_handle = tokio::runtime::Handle::current();

    // Start Ferdie
    let mut ferdie = run_mock_primary_chain_validator_node(
        tokio_handle.clone(),
        Ferdie,
        BasePath::new(directory.path().join("ferdie")),
    )
    .await;

    // Run Alice (a system domain authority node)
    let alice = domain_test_service::SystemDomainNodeBuilder::new(
        tokio_handle.clone(),
        Alice,
        BasePath::new(directory.path().join("alice")),
    )
    .build_with_mock(Role::Authority, &ferdie)
    .await;

    futures::join!(alice.wait_for_blocks(3), ferdie.produce_n_block_and_slot(3))
        .1
        .unwrap();

    // Wait until the domain bundles are submitted and applied to ensure the head
    // receipt number are updated
    let slot = ferdie.produce_slot();
    tokio::time::sleep(Duration::from_millis(100)).await;
    futures::join!(alice.wait_for_blocks(1), ferdie.produce_block(slot))
        .1
        .unwrap();

    let header = alice
        .client
        .header(alice.client.hash(1).unwrap().unwrap())
        .unwrap()
        .unwrap();
    let parent_header = alice.client.header(*header.parent_hash()).unwrap().unwrap();

    let intermediate_roots = alice
        .client
        .runtime_api()
        .intermediate_roots(header.hash())
        .expect("Get intermediate roots");

    let prover = subspace_fraud_proof::ExecutionProver::new(
        alice.backend.clone(),
        alice.code_executor.clone(),
        Box::new(alice.task_manager.spawn_handle()),
    );

    let digest = {
        let primary_block_info =
            DigestItem::primary_block_info((1, ferdie.client.hash(1).unwrap().unwrap()));

        Digest {
            logs: vec![primary_block_info],
        }
    };

    let new_header = Header::new(
        *header.number(),
        header.hash(),
        Default::default(),
        parent_header.hash(),
        digest,
    );
    let execution_phase = ExecutionPhase::InitializeBlock {
        call_data: new_header.encode(),
    };

    let storage_proof = prover
        .prove_execution::<sp_trie::PrefixedMemoryDB<BlakeTwo256>>(
            parent_header.hash(),
            &execution_phase,
            None,
        )
        .expect("Create `initialize_block` proof");

    let header_ferdie = ferdie
        .client
        .header(ferdie.client.hash(1).unwrap().unwrap())
        .unwrap()
        .unwrap();
    let parent_hash_ferdie = header_ferdie.hash();
    let parent_number_ferdie = *header_ferdie.number();

    let good_invalid_state_transition_proof = InvalidStateTransitionProof {
        domain_id: DomainId::SYSTEM,
        bad_signed_bundle_hash: Hash::random(),
        parent_number: parent_number_ferdie,
        parent_hash: parent_hash_ferdie,
        pre_state_root: *parent_header.state_root(),
        post_state_root: intermediate_roots[0].into(),
        proof: storage_proof,
        execution_phase: execution_phase.clone(),
    };
    let valid_fraud_proof =
        FraudProof::InvalidStateTransition(good_invalid_state_transition_proof.clone());

    let tx = subspace_test_runtime::UncheckedExtrinsic::new_unsigned(
        pallet_domains::Call::submit_fraud_proof {
            fraud_proof: valid_fraud_proof.clone(),
        }
        .into(),
    );

    let expected_tx_hash = tx.using_encoded(BlakeTwo256::hash);
    let tx_hash = ferdie
        .transaction_pool
        .pool()
        .submit_one(
            &BlockId::Hash(ferdie.client.info().best_hash),
            TransactionSource::External,
            tx.into(),
        )
        .await
        .expect("Error at submitting a valid fraud proof");
    assert_eq!(tx_hash, expected_tx_hash);

    let bad_invalid_state_transition_proof = InvalidStateTransitionProof {
        post_state_root: Hash::random(),
        ..good_invalid_state_transition_proof
    };
    let invalid_fraud_proof =
        FraudProof::InvalidStateTransition(bad_invalid_state_transition_proof);

    let tx = subspace_test_runtime::UncheckedExtrinsic::new_unsigned(
        pallet_domains::Call::submit_fraud_proof {
            fraud_proof: invalid_fraud_proof,
        }
        .into(),
    );

    let submit_invalid_fraud_proof_result = ferdie
        .transaction_pool
        .pool()
        .submit_one(
            &BlockId::Hash(ferdie.client.info().best_hash),
            TransactionSource::External,
            tx.into(),
        )
        .await;

    match submit_invalid_fraud_proof_result.unwrap_err() {
        sc_transaction_pool::error::Error::Pool(
            sc_transaction_pool_api::error::Error::InvalidTransaction(invalid_tx),
        ) => assert_eq!(invalid_tx, InvalidTransactionCode::FraudProof.into()),
        e => panic!("Unexpected error while submitting an invalid fraud proof: {e}"),
    }
}

// TODO: Add a new test which simulates a situation that an executor produces a fraud proof
// when an invalid receipt is received.

#[substrate_test_utils::test(flavor = "multi_thread")]
async fn set_new_code_should_work() {
    let directory = TempDir::new().expect("Must be able to create temporary directory");

    let mut builder = sc_cli::LoggerBuilder::new("");
    builder.with_colors(false);
    let _ = builder.init();

    let tokio_handle = tokio::runtime::Handle::current();

    // Start Ferdie
    let mut ferdie = run_mock_primary_chain_validator_node(
        tokio_handle.clone(),
        Ferdie,
        BasePath::new(directory.path().join("ferdie")),
    )
    .await;

    // Run Alice (a system domain authority node)
    let alice = domain_test_service::SystemDomainNodeBuilder::new(
        tokio_handle.clone(),
        Alice,
        BasePath::new(directory.path().join("alice")),
    )
    .build_with_mock(Role::Authority, &ferdie)
    .await;

    futures::join!(alice.wait_for_blocks(1), ferdie.produce_n_block_and_slot(1))
        .1
        .unwrap();

    let slot = ferdie.produce_slot();
    let best_hash = ferdie.client.info().best_hash;
    futures::join!(
        alice.wait_for_blocks(1),
        ferdie.produce_block_with(
            slot,
            best_hash,
            vec![DigestItem::RuntimeEnvironmentUpdated],
            vec![]
        )
    )
    .1
    .unwrap();

    let best_hash = alice.client.info().best_hash;
    let logs = alice.client.header(best_hash).unwrap().unwrap().digest.logs;
    if logs
        .iter()
        .find(|i| **i == DigestItem::RuntimeEnvironmentUpdated)
        .is_none()
    {
        let extrinsics = alice
            .client
            .block_body(best_hash)
            .unwrap()
            .unwrap()
            .into_iter()
            .map(|encoded_extrinsic| {
                UncheckedExtrinsic::decode(&mut encoded_extrinsic.encode().as_slice()).unwrap()
            })
            .collect::<Vec<_>>();
        panic!("`set_code` not executed, extrinsics in the block: {extrinsics:?}")
    }
}

#[substrate_test_utils::test(flavor = "multi_thread")]
async fn extract_core_domain_wasm_bundle_in_system_domain_runtime_should_work() {
    let directory = TempDir::new().expect("Must be able to create temporary directory");

    let mut builder = sc_cli::LoggerBuilder::new("");
    builder.with_colors(false);
    let _ = builder.init();

    let tokio_handle = tokio::runtime::Handle::current();

    // Start Ferdie
    let ferdie = run_mock_primary_chain_validator_node(
        tokio_handle.clone(),
        Ferdie,
        BasePath::new(directory.path().join("ferdie")),
    )
    .await;

    let system_domain_bundle = ferdie
        .client
        .runtime_api()
        .system_domain_wasm_bundle(ferdie.client.info().best_hash)
        .unwrap();

    let core_payments_runtime_blob =
        read_core_domain_runtime_blob(system_domain_bundle.as_ref(), DomainId::CORE_PAYMENTS)
            .unwrap();

    let core_payments_blob = RuntimeBlob::new(&core_payments_runtime_blob).unwrap();
    let core_payments_version = sc_executor::read_embedded_version(&core_payments_blob)
        .unwrap()
        .unwrap();

    assert_eq!(core_payments_version, core_payments_domain_runtime::VERSION);
}

#[substrate_test_utils::test(flavor = "multi_thread")]
async fn pallet_domains_unsigned_extrinsics_should_work() {
    let directory = TempDir::new().expect("Must be able to create temporary directory");

    let mut builder = sc_cli::LoggerBuilder::new("");
    builder.with_colors(false);
    let _ = builder.init();

    let tokio_handle = tokio::runtime::Handle::current();

    // Start Ferdie
    let mut ferdie = run_mock_primary_chain_validator_node(
        tokio_handle.clone(),
        Ferdie,
        BasePath::new(directory.path().join("ferdie")),
    )
    .await;

    // Run Alice (a system domain full node)
    // Run a full node deliberately in order to control the execution chain by
    // submitting the receipts manually later.
    let alice = domain_test_service::SystemDomainNodeBuilder::new(
        tokio_handle.clone(),
        Alice,
        BasePath::new(directory.path().join("alice")),
    )
    .build_with_mock(Role::Full, &ferdie)
    .await;

    // Wait for 5 blocks to make sure the execution receipts of block 1,2,3,4 are
    // able to be written to the database.
    futures::join!(alice.wait_for_blocks(5), ferdie.produce_n_block_and_slot(5))
        .1
        .unwrap();

    let ferdie_client = ferdie.client.clone();
    let construct_submit_bundle =
        |primary_number: BlockNumber| -> subspace_test_runtime::UncheckedExtrinsic {
            let execution_receipt = crate::aux_schema::load_execution_receipt(
                &*alice.backend,
                alice.client.hash(primary_number).unwrap().unwrap(),
            )
            .expect("Failed to load execution receipt from the local aux_db")
            .unwrap_or_else(|| {
                panic!("The requested execution receipt for block {primary_number} does not exist")
            });

            let bundle = Bundle {
                header: BundleHeader {
                    primary_number,
                    primary_hash: ferdie_client.hash(primary_number).unwrap().unwrap(),
                    slot_number: (std::time::SystemTime::now()
                        .duration_since(std::time::SystemTime::UNIX_EPOCH)
                        .expect("Current time is always after unix epoch; qed")
                        .as_millis()
                        / 2000) as u64,
                    extrinsics_root: Default::default(),
                },
                receipts: vec![execution_receipt],
                extrinsics: Vec::<UncheckedExtrinsic>::new(),
            };

            let pair = ExecutorPair::from_string("//Alice", None).unwrap();
            let signature = pair.sign(bundle.hash().as_ref());

            let signed_opaque_bundle = SignedBundle {
                bundle,
                bundle_solution: BundleSolution::System {
                    authority_stake_weight: Default::default(),
                    authority_witness: Default::default(),
                    proof_of_election: ProofOfElection::dummy(DomainId::SYSTEM, pair.public()),
                }, // TODO: mock ProofOfElection properly
                signature,
            }
            .into_signed_opaque_bundle();

            subspace_test_runtime::UncheckedExtrinsic::new_unsigned(
                pallet_domains::Call::submit_bundle {
                    signed_opaque_bundle,
                }
                .into(),
            )
        };

    futures::join!(
        alice.wait_for_blocks(1),
        ferdie.produce_block_with_extrinsics(vec![
            construct_submit_bundle(1).into(),
            construct_submit_bundle(2).into()
        ]),
    )
    .1
    .unwrap();
    let best_hash = ferdie.client.info().best_hash;
    assert_eq!(
        ferdie
            .client
            .runtime_api()
            .head_receipt_number(&BlockId::Hash(best_hash))
            .unwrap(),
        2,
    );

    // The bundle 4 will fail to execute due to the receipt is not consecutive
    futures::join!(
        alice.wait_for_blocks(1),
        ferdie.produce_block_with_extrinsics(vec![construct_submit_bundle(4).into()]),
    )
    .1
    .unwrap();
    let best_hash = ferdie.client.info().best_hash;
    assert_eq!(
        ferdie
            .client
            .runtime_api()
            .head_receipt_number(&BlockId::Hash(best_hash))
            .unwrap(),
        2,
    );

    // The bundle 3 will successfully executed and update the head receipt number
    futures::join!(
        alice.wait_for_blocks(1),
        ferdie.produce_block_with_extrinsics(vec![construct_submit_bundle(3).into()]),
    )
    .1
    .unwrap();
    let best_hash = ferdie.client.info().best_hash;
    assert_eq!(
        ferdie
            .client
            .runtime_api()
            .head_receipt_number(&BlockId::Hash(best_hash))
            .unwrap(),
        3,
    );
}
