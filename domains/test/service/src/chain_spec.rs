//! Chain specification for the domain test runtime.

use evm_domain_test_runtime::GenesisConfig;
use sc_service::{ChainSpec, ChainType, GenericChainSpec};
use sp_domains::storage::RawGenesis;
use sp_domains::DomainGenesisStorage;

/// Create chain spec
pub fn create_domain_spec(domain_genesis_storage: DomainGenesisStorage) -> Box<dyn ChainSpec> {
    let DomainGenesisStorage {
        runtime_code,
        raw_genesis_storage,
        ..
    } = domain_genesis_storage;

    let genesis_storage = serde_json::from_slice::<RawGenesis>(&raw_genesis_storage)
        .expect("Deserialize `RawGenesis` of the evm domain should not fail")
        .into_storage(runtime_code);

    let mut chain_spec = GenericChainSpec::from_genesis(
        "Local Testnet",
        "local_testnet",
        ChainType::Local,
        // The value of the `GenesisConfig` doesn't matter since it will be overwritten later
        GenesisConfig::default,
        vec![],
        None,
        None,
        None,
        None,
        None,
    );

    chain_spec.set_storage(genesis_storage);

    Box::new(chain_spec)
}
