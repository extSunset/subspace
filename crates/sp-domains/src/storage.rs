use serde::{Deserialize, Serialize};
use sp_core::storage::{
    well_known_keys, ChildInfo, Storage, StorageChild, StorageData, StorageKey,
};
use std::collections::btree_map::BTreeMap;

pub type GenesisStorage = BTreeMap<StorageKey, StorageData>;

/// Raw storage content for genesis block, it will be serialized and stored on the consensus chain
///
/// NOTE: the WASM runtime code state is removed from the storage to avoid redundancy
/// since it is already stored in the `RuntimeRegistry` state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct RawGenesis {
    top: GenesisStorage,
    children_default: BTreeMap<StorageKey, GenesisStorage>,
}

impl RawGenesis {
    /// Construct `RawGenesis` from a given storage
    //
    /// NOTE: This function is mostly part from `sc-chain-spec::GenesisSource::resolve`
    /// with slight change of the runtime code storage
    pub fn from_storage(mut storage: Storage) -> Self {
        // Remove the runtime code from the storage
        let _ = storage.top.remove(well_known_keys::CODE);

        let top = storage
            .top
            .into_iter()
            .map(|(k, v)| (StorageKey(k), StorageData(v)))
            .collect();

        let children_default = storage
            .children_default
            .into_iter()
            .map(|(k, child)| {
                (
                    StorageKey(k),
                    child
                        .data
                        .into_iter()
                        .map(|(k, v)| (StorageKey(k), StorageData(v)))
                        .collect(),
                )
            })
            .collect();

        RawGenesis {
            top,
            children_default,
        }
    }

    /// Convert `RawGenesis` to storage, the opposite of `from_storage`
    //
    /// NOTE: This function is mostly part from `<sc-chain-spec::ChainSpec as BuildStorage>::assimilate_storage`
    /// with slight change of the runtime code storage
    pub fn into_storage(self, runtime_code: Vec<u8>) -> Storage {
        let RawGenesis {
            top: map,
            children_default: children_map,
        } = self;
        let mut storage = Storage::default();

        // Add the runtime code back to the storage
        let _ = storage
            .top
            .insert(well_known_keys::CODE.to_owned(), runtime_code);

        storage.top.extend(map.into_iter().map(|(k, v)| (k.0, v.0)));

        children_map.into_iter().for_each(|(k, v)| {
            let child_info = ChildInfo::new_default(k.0.as_slice());
            storage
                .children_default
                .entry(k.0)
                .or_insert_with(|| StorageChild {
                    data: Default::default(),
                    child_info,
                })
                .data
                .extend(v.into_iter().map(|(k, v)| (k.0, v.0)));
        });

        storage
    }
}
