mod farming;
pub mod piece_cache;
pub mod piece_reader;
mod plotting;

use crate::identity::{Identity, IdentityError};
use crate::node_client::NodeClient;
use crate::reward_signing::reward_signing;
use crate::single_disk_farm::farming::farming;
pub use crate::single_disk_farm::farming::FarmingError;
use crate::single_disk_farm::piece_cache::{DiskPieceCache, DiskPieceCacheError};
use crate::single_disk_farm::piece_reader::PieceReader;
pub use crate::single_disk_farm::plotting::PlottingError;
use crate::single_disk_farm::plotting::{plotting, plotting_scheduler};
use crate::utils::JoinOnDrop;
use derive_more::{Display, From};
use event_listener_primitives::{Bag, HandlerId};
use futures::channel::{mpsc, oneshot};
use futures::future::{select, Either};
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use memmap2::{Mmap, MmapOptions};
use parity_scale_codec::{Decode, Encode};
use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use static_assertions::const_assert;
use std::fs::{File, OpenOptions};
use std::future::Future;
use std::io::{Seek, SeekFrom};
use std::num::{NonZeroU16, NonZeroU8};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::{fmt, fs, io, mem, thread};
use std_semaphore::{Semaphore, SemaphoreGuard};
use subspace_core_primitives::crypto::blake3_hash;
use subspace_core_primitives::crypto::kzg::Kzg;
use subspace_core_primitives::{
    Blake3Hash, HistorySize, Piece, PieceOffset, PublicKey, Record, SectorId, SectorIndex,
    SegmentIndex,
};
use subspace_erasure_coding::ErasureCoding;
use subspace_farmer_components::file_ext::FileExt;
use subspace_farmer_components::plotting::{PieceGetter, PlottedSector};
use subspace_farmer_components::sector::{sector_size, SectorMetadata, SectorMetadataChecksummed};
use subspace_farmer_components::FarmerProtocolInfo;
use subspace_networking::NetworkingParametersManager;
use subspace_proof_of_space::Table;
use subspace_rpc_primitives::{FarmerAppInfo, SolutionResponse};
use thiserror::Error;
use tokio::runtime::Handle;
use tokio::sync::broadcast;
use tracing::{debug, error, info, info_span, trace, warn, Instrument, Span};
use ulid::Ulid;

// Refuse to compile on non-64-bit platforms, offsets may fail on those when converting from u64 to
// usize depending on chain parameters
const_assert!(std::mem::size_of::<usize>() >= std::mem::size_of::<u64>());

/// Reserve 1M of space for plot metadata (for potential future expansion)
const RESERVED_PLOT_METADATA: u64 = 1024 * 1024;
/// Reserve 1M of space for farm info (for potential future expansion)
const RESERVED_FARM_INFO: u64 = 1024 * 1024;

/// Semaphore that limits disk access concurrency in strategic places to the number specified during
/// initialization
#[derive(Clone)]
pub struct SingleDiskSemaphore {
    inner: Arc<Semaphore>,
}

impl fmt::Debug for SingleDiskSemaphore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SingleDiskSemaphore").finish()
    }
}

impl SingleDiskSemaphore {
    /// Create new semaphore for limiting concurrency of the major processes working with the same
    /// disk
    pub fn new(concurrency: NonZeroU16) -> Self {
        Self {
            inner: Arc::new(Semaphore::new(concurrency.get() as isize)),
        }
    }

    /// Acquire access, will block current thread until previously acquired guards are dropped and
    /// access is released
    pub fn acquire(&self) -> SemaphoreGuard<'_> {
        self.inner.access()
    }
}

/// An identifier for single disk farm, can be used for in logs, thread names, etc.
#[derive(
    Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash, Serialize, Deserialize, Display, From,
)]
#[serde(untagged)]
pub enum SingleDiskFarmId {
    /// Farm ID
    Ulid(Ulid),
}

#[allow(clippy::new_without_default)]
impl SingleDiskFarmId {
    /// Creates new ID
    pub fn new() -> Self {
        Self::Ulid(Ulid::new())
    }
}

/// Important information about the contents of the `SingleDiskFarm`
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SingleDiskFarmInfo {
    /// V0 of the info
    #[serde(rename_all = "camelCase")]
    V0 {
        /// ID of the farm
        id: SingleDiskFarmId,
        /// Genesis hash of the chain used for farm creation
        #[serde(with = "hex::serde")]
        genesis_hash: [u8; 32],
        /// Public key of identity used for farm creation
        public_key: PublicKey,
        /// How many pieces does one sector contain.
        pieces_in_sector: u16,
        /// How much space in bytes is allocated for this farm
        allocated_space: u64,
    },
}

impl SingleDiskFarmInfo {
    const FILE_NAME: &'static str = "single_disk_farm.json";

    pub fn new(
        id: SingleDiskFarmId,
        genesis_hash: [u8; 32],
        public_key: PublicKey,
        pieces_in_sector: u16,
        allocated_space: u64,
    ) -> Self {
        Self::V0 {
            id,
            genesis_hash,
            public_key,
            pieces_in_sector,
            allocated_space,
        }
    }

    /// Load `SingleDiskFarm` from path is supposed to be stored, `None` means no info file was
    /// found, happens during first start.
    pub fn load_from(directory: &Path) -> io::Result<Option<Self>> {
        // TODO: Remove this compatibility hack after enough time has passed
        if directory.join("single_disk_plot.json").exists() {
            fs::rename(
                directory.join("single_disk_plot.json"),
                directory.join(Self::FILE_NAME),
            )?;
        }
        let bytes = match fs::read(directory.join(Self::FILE_NAME)) {
            Ok(bytes) => bytes,
            Err(error) => {
                return if error.kind() == io::ErrorKind::NotFound {
                    Ok(None)
                } else {
                    Err(error)
                };
            }
        };

        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Store `SingleDiskFarm` info to path so it can be loaded again upon restart.
    pub fn store_to(&self, directory: &Path) -> io::Result<()> {
        fs::write(
            directory.join(Self::FILE_NAME),
            serde_json::to_vec(self).expect("Info serialization never fails; qed"),
        )
    }

    // ID of the farm
    pub fn id(&self) -> &SingleDiskFarmId {
        let Self::V0 { id, .. } = self;
        id
    }

    // Genesis hash of the chain used for farm creation
    pub fn genesis_hash(&self) -> &[u8; 32] {
        let Self::V0 { genesis_hash, .. } = self;
        genesis_hash
    }

    // Public key of identity used for farm creation
    pub fn public_key(&self) -> &PublicKey {
        let Self::V0 { public_key, .. } = self;
        public_key
    }

    /// How many pieces does one sector contain.
    pub fn pieces_in_sector(&self) -> u16 {
        let Self::V0 {
            pieces_in_sector, ..
        } = self;
        *pieces_in_sector
    }

    /// How much space in bytes is allocated for this farm
    pub fn allocated_space(&self) -> u64 {
        let Self::V0 {
            allocated_space, ..
        } = self;
        *allocated_space
    }
}

/// Summary of single disk farm for presentational purposes
pub enum SingleDiskFarmSummary {
    /// Farm was found and read successfully
    Found {
        /// Farm info
        info: SingleDiskFarmInfo,
        /// Path to directory where farm is stored.
        directory: PathBuf,
    },
    /// Farm was not found
    NotFound {
        /// Path to directory where farm is stored.
        directory: PathBuf,
    },
    /// Failed to open farm
    Error {
        /// Path to directory where farm is stored.
        directory: PathBuf,
        /// Error itself
        error: io::Error,
    },
}

#[derive(Debug, Encode, Decode)]
struct PlotMetadataHeader {
    version: u8,
    plotted_sector_count: SectorIndex,
}

impl PlotMetadataHeader {
    #[inline]
    fn encoded_size() -> usize {
        let default = PlotMetadataHeader {
            version: 0,
            plotted_sector_count: 0,
        };

        default.encoded_size()
    }
}

/// Options used to open single disk farm
pub struct SingleDiskFarmOptions<NC, PG> {
    /// Path to directory where farm is stored.
    pub directory: PathBuf,
    /// Information necessary for farmer application
    pub farmer_app_info: FarmerAppInfo,
    /// How much space in bytes was allocated
    pub allocated_space: u64,
    /// How many pieces one sector is supposed to contain (max)
    pub max_pieces_in_sector: u16,
    /// RPC client connected to Subspace node
    pub node_client: NC,
    /// Address where farming rewards should go
    pub reward_address: PublicKey,
    /// Piece receiver implementation for plotting purposes.
    pub piece_getter: PG,
    /// Kzg instance to use.
    pub kzg: Kzg,
    /// Erasure coding instance to use.
    pub erasure_coding: ErasureCoding,
    /// Percentage of allocated space dedicated for caching purposes
    pub cache_percentage: NonZeroU8,
}

/// Errors happening when trying to create/open single disk farm
#[derive(Debug, Error)]
pub enum SingleDiskFarmError {
    // TODO: Make more variants out of this generic one
    /// I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    /// Piece cache error
    #[error("Piece cache error: {0}")]
    PieceCacheError(#[from] DiskPieceCacheError),
    /// Can't preallocate metadata file, probably not enough space on disk
    #[error("Can't preallocate metadata file, probably not enough space on disk: {0}")]
    CantPreallocateMetadataFile(io::Error),
    /// Can't preallocate plot file, probably not enough space on disk
    #[error("Can't preallocate plot file, probably not enough space on disk: {0}")]
    CantPreallocatePlotFile(io::Error),
    /// Wrong chain (genesis hash)
    #[error(
        "Genesis hash of farm {id} {wrong_chain} is different from {correct_chain} when farm was \
        created, it is not possible to use farm on a different chain"
    )]
    WrongChain {
        /// Farm ID
        id: SingleDiskFarmId,
        /// Hex-encoded genesis hash during farm creation
        // TODO: Wrapper type with `Display` impl for genesis hash
        correct_chain: String,
        /// Hex-encoded current genesis hash
        wrong_chain: String,
    },
    /// Public key in identity doesn't match metadata
    #[error(
        "Public key of farm {id} {wrong_public_key} is different from {correct_public_key} when \
        farm was created, something went wrong, likely due to manual edits"
    )]
    IdentityMismatch {
        /// Farm ID
        id: SingleDiskFarmId,
        /// Public key used during farm creation
        correct_public_key: PublicKey,
        /// Current public key
        wrong_public_key: PublicKey,
    },
    /// Invalid number pieces in sector
    #[error(
        "Invalid number pieces in sector: max supported {max_supported}, farm initialized with \
        {initialized_with}"
    )]
    InvalidPiecesInSector {
        /// Farm ID
        id: SingleDiskFarmId,
        /// Max supported pieces in sector
        max_supported: u16,
        /// Number of pieces in sector farm is initialized with
        initialized_with: u16,
    },
    /// Failed to decode metadata header
    #[error("Failed to decode metadata header: {0}")]
    FailedToDecodeMetadataHeader(parity_scale_codec::Error),
    /// Failed to decode sector metadata
    #[error("Failed to decode sector metadata: {0}")]
    FailedToDecodeSectorMetadata(parity_scale_codec::Error),
    /// Unexpected metadata version
    #[error("Unexpected metadata version {0}")]
    UnexpectedMetadataVersion(u8),
    /// Allocated space is not enough for one sector
    #[error(
        "Allocated space is not enough for one sector. \
        The lowest acceptable value for allocated space is {min_space} bytes, \
        provided {allocated_space} bytes."
    )]
    InsufficientAllocatedSpace {
        /// Minimal allocated space
        min_space: u64,
        /// Current allocated space
        allocated_space: u64,
    },
    /// Farm is too large
    #[error(
        "Farm is too large: allocated {allocated_sectors} sectors ({allocated_space} bytes), max \
        supported is {max_sectors} ({max_space} bytes). Consider creating multiple smaller farms \
        instead."
    )]
    FarmTooLarge {
        allocated_space: u64,
        allocated_sectors: u64,
        max_space: u64,
        max_sectors: u16,
    },
}

/// Errors happening during scrubbing
#[derive(Debug, Error)]
pub enum SingleDiskFarmScrubError {
    /// Failed to determine file size
    #[error("Failed to file size of {file}: {error}")]
    FailedToDetermineFileSize {
        /// Affected file
        file: PathBuf,
        /// Low-level error
        error: io::Error,
    },
    /// Failed to read bytes from file
    #[error("Failed to read {size} bytes from {file} at offset {offset}: {error}")]
    FailedToReadBytes {
        /// Affected file
        file: PathBuf,
        /// Number of bytes to read
        size: u64,
        /// Offset in the file
        offset: u64,
        /// Low-level error
        error: io::Error,
    },
    /// Failed to write bytes from file
    #[error("Failed to write {size} bytes from {file} at offset {offset}: {error}")]
    FailedToWriteBytes {
        /// Affected file
        file: PathBuf,
        /// Number of bytes to read
        size: u64,
        /// Offset in the file
        offset: u64,
        /// Low-level error
        error: io::Error,
    },
    /// Farm info file does not exist
    #[error("Farm info file does not exist at {file}")]
    FarmInfoFileDoesNotExist {
        /// Info file
        file: PathBuf,
    },
    /// Farm info can't be opened
    #[error("Farm info at {file} can't be opened: {error}")]
    FarmInfoCantBeOpened {
        /// Info file
        file: PathBuf,
        /// Low-level error
        error: io::Error,
    },
    /// Identity file does not exist
    #[error("Identity file does not exist at {file}")]
    IdentityFileDoesNotExist {
        /// Identity file
        file: PathBuf,
    },
    /// Identity can't be opened
    #[error("Identity at {file} can't be opened: {error}")]
    IdentityCantBeOpened {
        /// Identity file
        file: PathBuf,
        /// Low-level error
        error: IdentityError,
    },
    /// Identity public key doesn't match public key in the disk farm info
    #[error(
        "Identity public key {identity} doesn't match public key in the disk farm info {info}"
    )]
    PublicKeyMismatch {
        /// Identity public key
        identity: PublicKey,
        /// Disk farm info public key
        info: PublicKey,
    },
    /// Metadata file does not exist
    #[error("Metadata file does not exist at {file}")]
    MetadataFileDoesNotExist {
        /// Metadata file
        file: PathBuf,
    },
    /// Metadata can't be opened
    #[error("Metadata at {file} can't be opened: {error}")]
    MetadataCantBeOpened {
        /// Metadata file
        file: PathBuf,
        /// Low-level error
        error: io::Error,
    },
    /// Metadata file too small
    #[error(
        "Metadata file at {file} is too small: reserved size is {reserved_size} bytes, file size \
        is {size}"
    )]
    MetadataFileTooSmall {
        /// Metadata file
        file: PathBuf,
        /// Reserved size
        reserved_size: u64,
        /// File size
        size: u64,
    },
    /// Failed to decode metadata header
    #[error("Failed to decode metadata header: {0}")]
    FailedToDecodeMetadataHeader(parity_scale_codec::Error),
    /// Unexpected metadata version
    #[error("Unexpected metadata version {0}")]
    UnexpectedMetadataVersion(u8),
    /// Cache file does not exist
    #[error("Cache file does not exist at {file}")]
    CacheFileDoesNotExist {
        /// Cache file
        file: PathBuf,
    },
    /// Cache can't be opened
    #[error("Cache at {file} can't be opened: {error}")]
    CacheCantBeOpened {
        /// Cache file
        file: PathBuf,
        /// Low-level error
        error: io::Error,
    },
}

/// Errors that happen in background tasks
#[derive(Debug, Error)]
pub enum BackgroundTaskError {
    /// Plotting error
    #[error(transparent)]
    Plotting(#[from] PlottingError),
    /// Farming error
    #[error(transparent)]
    Farming(#[from] FarmingError),
}

type BackgroundTask = Pin<Box<dyn Future<Output = Result<(), BackgroundTaskError>> + Send>>;

type HandlerFn<A> = Arc<dyn Fn(&A) + Send + Sync + 'static>;
type Handler<A> = Bag<HandlerFn<A>, A>;

#[derive(Default, Debug)]
struct Handlers {
    sector_plotted: Handler<(PlottedSector, Option<PlottedSector>)>,
    solution: Handler<SolutionResponse>,
}

/// Single disk farm abstraction is a container for everything necessary to plot/farm with a single
/// disk.
///
/// Farm starts operating during creation and doesn't stop until dropped (or error happens).
#[must_use = "Plot does not function properly unless run() method is called"]
pub struct SingleDiskFarm {
    farmer_protocol_info: FarmerProtocolInfo,
    single_disk_farm_info: SingleDiskFarmInfo,
    /// Metadata of all sectors plotted so far
    sectors_metadata: Arc<RwLock<Vec<SectorMetadataChecksummed>>>,
    pieces_in_sector: u16,
    span: Span,
    tasks: FuturesUnordered<BackgroundTask>,
    handlers: Arc<Handlers>,
    piece_cache: DiskPieceCache,
    piece_reader: PieceReader,
    _plotting_join_handle: JoinOnDrop,
    _farming_join_handle: JoinOnDrop,
    _reading_join_handle: JoinOnDrop,
    /// Sender that will be used to signal to background threads that they should start
    start_sender: Option<broadcast::Sender<()>>,
    /// Sender that will be used to signal to background threads that they must stop
    stop_sender: Option<broadcast::Sender<()>>,
}

impl Drop for SingleDiskFarm {
    fn drop(&mut self) {
        self.piece_reader.close_all_readers();
        // Make background threads that are waiting to do something exit immediately
        self.start_sender.take();
        // Notify background tasks that they must stop
        self.stop_sender.take();
    }
}

impl SingleDiskFarm {
    const PLOT_FILE: &'static str = "plot.bin";
    const METADATA_FILE: &'static str = "metadata.bin";
    const SUPPORTED_PLOT_VERSION: u8 = 0;

    /// Create new single disk farm instance
    ///
    /// NOTE: Thought this function is async, it will do some blocking I/O.
    pub async fn new<NC, PG, PosTable>(
        options: SingleDiskFarmOptions<NC, PG>,
        disk_farm_index: usize,
    ) -> Result<Self, SingleDiskFarmError>
    where
        NC: NodeClient,
        PG: PieceGetter + Send + 'static,
        PosTable: Table,
    {
        let handle = Handle::current();

        let SingleDiskFarmOptions {
            directory,
            farmer_app_info,
            allocated_space,
            max_pieces_in_sector,
            node_client,
            reward_address,
            piece_getter,
            kzg,
            erasure_coding,
            cache_percentage,
        } = options;
        fs::create_dir_all(&directory)?;

        // TODO: Parametrize concurrency, much higher default due to SSD focus
        // TODO: Use this or remove
        let _single_disk_semaphore =
            SingleDiskSemaphore::new(NonZeroU16::new(10).expect("Not a zero; qed"));

        // TODO: Update `Identity` to use more specific error type and remove this `.unwrap()`
        let identity = Identity::open_or_create(&directory).unwrap();
        let public_key = identity.public_key().to_bytes().into();

        let single_disk_farm_info = match SingleDiskFarmInfo::load_from(&directory)? {
            Some(mut single_disk_farm_info) => {
                if &farmer_app_info.genesis_hash != single_disk_farm_info.genesis_hash() {
                    return Err(SingleDiskFarmError::WrongChain {
                        id: *single_disk_farm_info.id(),
                        correct_chain: hex::encode(single_disk_farm_info.genesis_hash()),
                        wrong_chain: hex::encode(farmer_app_info.genesis_hash),
                    });
                }

                if &public_key != single_disk_farm_info.public_key() {
                    return Err(SingleDiskFarmError::IdentityMismatch {
                        id: *single_disk_farm_info.id(),
                        correct_public_key: *single_disk_farm_info.public_key(),
                        wrong_public_key: public_key,
                    });
                }

                let pieces_in_sector = single_disk_farm_info.pieces_in_sector();

                if max_pieces_in_sector < pieces_in_sector {
                    return Err(SingleDiskFarmError::InvalidPiecesInSector {
                        id: *single_disk_farm_info.id(),
                        max_supported: max_pieces_in_sector,
                        initialized_with: pieces_in_sector,
                    });
                }

                if max_pieces_in_sector > pieces_in_sector {
                    info!(
                        pieces_in_sector,
                        max_pieces_in_sector,
                        "Farm initialized with smaller number of pieces in sector, farm needs to \
                        be re-created for increase"
                    );
                }

                if allocated_space != single_disk_farm_info.allocated_space() {
                    info!(
                        old_space = %bytesize::to_string(single_disk_farm_info.allocated_space(), true),
                        new_space = %bytesize::to_string(allocated_space, true),
                        "Farm size has changed"
                    );

                    {
                        let new_allocated_space = allocated_space;
                        let SingleDiskFarmInfo::V0 {
                            allocated_space, ..
                        } = &mut single_disk_farm_info;
                        *allocated_space = new_allocated_space;
                    }

                    single_disk_farm_info.store_to(&directory)?;
                }

                single_disk_farm_info
            }
            None => {
                let single_disk_farm_info = SingleDiskFarmInfo::new(
                    SingleDiskFarmId::new(),
                    farmer_app_info.genesis_hash,
                    public_key,
                    max_pieces_in_sector,
                    allocated_space,
                );

                single_disk_farm_info.store_to(&directory)?;

                single_disk_farm_info
            }
        };

        let pieces_in_sector = single_disk_farm_info.pieces_in_sector();
        let sector_size = sector_size(pieces_in_sector);
        let sector_metadata_size = SectorMetadataChecksummed::encoded_size();
        let single_sector_overhead = (sector_size + sector_metadata_size) as u64;
        // Fixed space usage regardless of plot size
        let fixed_space_usage = RESERVED_PLOT_METADATA
            + RESERVED_FARM_INFO
            + Identity::file_size() as u64
            + NetworkingParametersManager::file_size() as u64;
        // Calculate how many sectors can fit
        let target_sector_count = {
            let potentially_plottable_space = allocated_space.saturating_sub(fixed_space_usage)
                / 100
                * (100 - u64::from(cache_percentage.get()));
            // Do the rounding to make sure we have exactly as much space as fits whole number of
            // sectors
            potentially_plottable_space / single_sector_overhead
        };

        if target_sector_count == 0 {
            let mut single_plot_with_cache_space =
                single_sector_overhead.div_ceil(100 - u64::from(cache_percentage.get())) * 100;
            // Cache must not be empty, ensure it contains at least one element even if
            // percentage-wise it will use more space
            if single_plot_with_cache_space - single_sector_overhead
                < DiskPieceCache::element_size() as u64
            {
                single_plot_with_cache_space =
                    single_sector_overhead + DiskPieceCache::element_size() as u64;
            }

            return Err(SingleDiskFarmError::InsufficientAllocatedSpace {
                min_space: fixed_space_usage + single_plot_with_cache_space,
                allocated_space,
            });
        }

        // Remaining space will be used for caching purposes
        let cache_capacity = {
            let cache_space = allocated_space
                - fixed_space_usage
                - (target_sector_count * single_sector_overhead);
            cache_space as usize / DiskPieceCache::element_size()
        };
        let target_sector_count = match SectorIndex::try_from(target_sector_count) {
            Ok(target_sector_count) if target_sector_count < SectorIndex::MAX => {
                target_sector_count
            }
            _ => {
                // We use this for both count and index, hence index must not reach actual `MAX`
                // (consensus doesn't care about this, just farmer implementation detail)
                let max_sectors = SectorIndex::MAX - 1;
                return Err(SingleDiskFarmError::FarmTooLarge {
                    allocated_space: target_sector_count * sector_size as u64,
                    allocated_sectors: target_sector_count,
                    max_space: max_sectors as u64 * sector_size as u64,
                    max_sectors,
                });
            }
        };

        // TODO: Consider file locking to prevent other apps from modifying it
        let mut metadata_file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(directory.join(Self::METADATA_FILE))?;

        let metadata_size = metadata_file.seek(SeekFrom::End(0))?;
        let expected_metadata_size =
            RESERVED_PLOT_METADATA + sector_metadata_size as u64 * u64::from(target_sector_count);
        let (metadata_header, metadata_header_mmap) = if metadata_size == 0 {
            let metadata_header = PlotMetadataHeader {
                version: 0,
                plotted_sector_count: 0,
            };

            metadata_file
                .preallocate(expected_metadata_size)
                .map_err(SingleDiskFarmError::CantPreallocateMetadataFile)?;
            metadata_file.write_all_at(metadata_header.encode().as_slice(), 0)?;

            let metadata_header_mmap = unsafe {
                MmapOptions::new()
                    .len(PlotMetadataHeader::encoded_size())
                    .map_mut(&metadata_file)?
            };

            (metadata_header, metadata_header_mmap)
        } else {
            if metadata_size != expected_metadata_size {
                // Allocating the whole file (`set_len` below can create a sparse file, which will
                // cause writes to fail later)
                metadata_file
                    .preallocate(expected_metadata_size)
                    .map_err(SingleDiskFarmError::CantPreallocateMetadataFile)?;
                // Truncating file (if necessary)
                metadata_file.set_len(expected_metadata_size)?;
            }
            let mut metadata_header_mmap = unsafe {
                MmapOptions::new()
                    .len(PlotMetadataHeader::encoded_size())
                    .map_mut(&metadata_file)?
            };

            let mut metadata_header =
                PlotMetadataHeader::decode(&mut metadata_header_mmap.as_ref())
                    .map_err(SingleDiskFarmError::FailedToDecodeMetadataHeader)?;

            if metadata_header.version != Self::SUPPORTED_PLOT_VERSION {
                return Err(SingleDiskFarmError::UnexpectedMetadataVersion(
                    metadata_header.version,
                ));
            }

            if metadata_header.plotted_sector_count > target_sector_count {
                metadata_header.plotted_sector_count = target_sector_count;
                metadata_header.encode_to(&mut metadata_header_mmap.as_mut());
            }

            (metadata_header, metadata_header_mmap)
        };

        let sectors_metadata = {
            let metadata_mmap = unsafe {
                MmapOptions::new()
                    .offset(RESERVED_PLOT_METADATA)
                    .len(sector_metadata_size * usize::from(target_sector_count))
                    .map(&metadata_file)?
            };

            let mut sectors_metadata =
                Vec::<SectorMetadataChecksummed>::with_capacity(usize::from(target_sector_count));

            for mut sector_metadata_bytes in metadata_mmap
                .chunks_exact(sector_metadata_size)
                .take(metadata_header.plotted_sector_count as usize)
            {
                sectors_metadata.push(
                    SectorMetadataChecksummed::decode(&mut sector_metadata_bytes)
                        .map_err(SingleDiskFarmError::FailedToDecodeSectorMetadata)?,
                );
            }

            Arc::new(RwLock::new(sectors_metadata))
        };

        let plot_file = Arc::new(
            OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(directory.join(Self::PLOT_FILE))?,
        );

        // Allocating the whole file (`set_len` below can create a sparse file, which will cause
        // writes to fail later)
        plot_file
            .preallocate(sector_size as u64 * u64::from(target_sector_count))
            .map_err(SingleDiskFarmError::CantPreallocatePlotFile)?;
        // Truncating file (if necessary)
        plot_file.set_len(sector_size as u64 * u64::from(target_sector_count))?;

        let piece_cache = DiskPieceCache::open(&directory, cache_capacity)?;

        let (error_sender, error_receiver) = oneshot::channel();
        let error_sender = Arc::new(Mutex::new(Some(error_sender)));

        let tasks = FuturesUnordered::<BackgroundTask>::new();

        tasks.push(Box::pin(async move {
            if let Ok(error) = error_receiver.await {
                return Err(error);
            }

            Ok(())
        }));

        let handlers = Arc::<Handlers>::default();
        let (start_sender, mut start_receiver) = broadcast::channel::<()>(1);
        let (stop_sender, mut stop_receiver) = broadcast::channel::<()>(1);
        let modifying_sector_index = Arc::<RwLock<Option<SectorIndex>>>::default();
        let (sectors_to_plot_sender, sectors_to_plot_receiver) = mpsc::channel(0);
        // Some sectors may already be plotted, skip them
        let sectors_indices_left_to_plot =
            metadata_header.plotted_sector_count..target_sector_count;

        let span = info_span!("single_disk_farm", %disk_farm_index);

        let plotting_join_handle = thread::Builder::new()
            .name(format!("plotting-{disk_farm_index}"))
            .spawn({
                let handle = handle.clone();
                let sectors_metadata = Arc::clone(&sectors_metadata);
                let kzg = kzg.clone();
                let erasure_coding = erasure_coding.clone();
                let handlers = Arc::clone(&handlers);
                let modifying_sector_index = Arc::clone(&modifying_sector_index);
                let node_client = node_client.clone();
                let plot_file = Arc::clone(&plot_file);
                let error_sender = Arc::clone(&error_sender);
                let span = span.clone();

                move || {
                    let _tokio_handle_guard = handle.enter();
                    let _span_guard = span.enter();

                    // Initial plotting
                    let initial_plotting_fut = async move {
                        if start_receiver.recv().await.is_err() {
                            // Dropped before starting
                            return Ok(());
                        }

                        plotting::<_, _, PosTable>(
                            public_key,
                            node_client,
                            pieces_in_sector,
                            sector_size,
                            sector_metadata_size,
                            metadata_header,
                            metadata_header_mmap,
                            plot_file,
                            metadata_file,
                            sectors_metadata,
                            piece_getter,
                            kzg,
                            erasure_coding,
                            handlers,
                            modifying_sector_index,
                            target_sector_count,
                            sectors_to_plot_receiver,
                        )
                        .await
                    };

                    let initial_plotting_result = handle.block_on(select(
                        Box::pin(initial_plotting_fut),
                        Box::pin(stop_receiver.recv()),
                    ));

                    if let Either::Left((Err(error), _)) = initial_plotting_result {
                        if let Some(error_sender) = error_sender.lock().take() {
                            if let Err(error) = error_sender.send(error.into()) {
                                error!(%error, "Plotting failed to send error to background task");
                            }
                        }
                    }
                }
            })?;

        tasks.push(Box::pin(plotting_scheduler(
            public_key.hash(),
            sectors_indices_left_to_plot,
            target_sector_count,
            farmer_app_info.protocol_info.history_size.segment_index(),
            farmer_app_info.protocol_info.min_sector_lifetime,
            node_client.clone(),
            Arc::clone(&sectors_metadata),
            sectors_to_plot_sender,
        )));

        let (mut slot_info_forwarder_sender, slot_info_forwarder_receiver) = mpsc::channel(0);

        tasks.push(Box::pin({
            let node_client = node_client.clone();

            async move {
                info!("Subscribing to slot info notifications");

                let mut slot_info_notifications = node_client
                    .subscribe_slot_info()
                    .await
                    .map_err(|error| FarmingError::FailedToSubscribeSlotInfo { error })?;

                while let Some(slot_info) = slot_info_notifications.next().await {
                    debug!(?slot_info, "New slot");

                    let slot = slot_info.slot_number;

                    // Error means farmer is still solving for previous slot, which is too late and
                    // we need to skip this slot
                    if slot_info_forwarder_sender.try_send(slot_info).is_err() {
                        debug!(%slot, "Slow farming, skipping slot");
                    }
                }

                Ok(())
            }
        }));

        let farming_join_handle = thread::Builder::new()
            .name(format!("farming-{disk_farm_index}"))
            .spawn({
                let plot_mmap = unsafe { Mmap::map(&*plot_file)? };
                #[cfg(unix)]
                {
                    plot_mmap.advise(memmap2::Advice::Random)?;
                }

                let handle = handle.clone();
                let erasure_coding = erasure_coding.clone();
                let handlers = Arc::clone(&handlers);
                let modifying_sector_index = Arc::clone(&modifying_sector_index);
                let sectors_metadata = Arc::clone(&sectors_metadata);
                let mut start_receiver = start_sender.subscribe();
                let mut stop_receiver = stop_sender.subscribe();
                let node_client = node_client.clone();
                let span = span.clone();

                move || {
                    let _tokio_handle_guard = handle.enter();
                    let _span_guard = span.enter();

                    let farming_fut = async move {
                        if start_receiver.recv().await.is_err() {
                            // Dropped before starting
                            return Ok(());
                        }

                        farming::<_, PosTable>(
                            public_key,
                            reward_address,
                            node_client,
                            sector_size,
                            plot_mmap,
                            sectors_metadata,
                            kzg,
                            erasure_coding,
                            handlers,
                            modifying_sector_index,
                            slot_info_forwarder_receiver,
                        )
                        .await
                    };

                    let farming_result = handle.block_on(select(
                        Box::pin(farming_fut),
                        Box::pin(stop_receiver.recv()),
                    ));

                    if let Either::Left((Err(error), _)) = farming_result {
                        if let Some(error_sender) = error_sender.lock().take() {
                            if let Err(error) = error_sender.send(error.into()) {
                                error!(%error, "Farming failed to send error to background task");
                            }
                        }
                    }
                }
            })?;

        let (piece_reader, reading_fut) = PieceReader::new::<PosTable>(
            public_key,
            pieces_in_sector,
            unsafe { Mmap::map(&*plot_file)? },
            Arc::clone(&sectors_metadata),
            erasure_coding,
            modifying_sector_index,
        );

        let reading_join_handle = thread::Builder::new()
            .name(format!("reading-{disk_farm_index}"))
            .spawn({
                let mut stop_receiver = stop_sender.subscribe();
                let reading_fut = reading_fut.instrument(span.clone());

                move || {
                    let _tokio_handle_guard = handle.enter();

                    handle.block_on(select(
                        Box::pin(reading_fut),
                        Box::pin(stop_receiver.recv()),
                    ));
                }
            })?;

        tasks.push(Box::pin(async move {
            // TODO: Error handling here
            reward_signing(node_client, identity).await.unwrap().await;

            Ok(())
        }));

        let farm = Self {
            farmer_protocol_info: farmer_app_info.protocol_info,
            single_disk_farm_info,
            sectors_metadata,
            pieces_in_sector,
            span,
            tasks,
            handlers,
            piece_cache,
            piece_reader,
            _plotting_join_handle: JoinOnDrop::new(plotting_join_handle),
            _farming_join_handle: JoinOnDrop::new(farming_join_handle),
            _reading_join_handle: JoinOnDrop::new(reading_join_handle),
            start_sender: Some(start_sender),
            stop_sender: Some(stop_sender),
        };

        Ok(farm)
    }

    /// Collect summary of single disk farm for presentational purposes
    pub fn collect_summary(directory: PathBuf) -> SingleDiskFarmSummary {
        let single_disk_farm_info = match SingleDiskFarmInfo::load_from(&directory) {
            Ok(Some(single_disk_farm_info)) => single_disk_farm_info,
            Ok(None) => {
                return SingleDiskFarmSummary::NotFound { directory };
            }
            Err(error) => {
                return SingleDiskFarmSummary::Error { directory, error };
            }
        };

        SingleDiskFarmSummary::Found {
            info: single_disk_farm_info,
            directory,
        }
    }

    /// ID of this farm
    pub fn id(&self) -> &SingleDiskFarmId {
        self.single_disk_farm_info.id()
    }

    /// Number of sectors successfully plotted so far
    pub fn plotted_sectors_count(&self) -> usize {
        self.sectors_metadata.read().len()
    }

    /// Read information about sectors plotted so far
    pub fn plotted_sectors(
        &self,
    ) -> impl Iterator<Item = Result<PlottedSector, parity_scale_codec::Error>> + '_ {
        let public_key = self.single_disk_farm_info.public_key();

        (0..).zip(self.sectors_metadata.read().clone()).map(
            move |(sector_index, sector_metadata)| {
                let sector_id = SectorId::new(public_key.hash(), sector_index);

                let mut piece_indexes = Vec::with_capacity(usize::from(self.pieces_in_sector));
                (PieceOffset::ZERO..)
                    .take(usize::from(self.pieces_in_sector))
                    .map(|piece_offset| {
                        sector_id.derive_piece_index(
                            piece_offset,
                            sector_metadata.history_size,
                            self.farmer_protocol_info.max_pieces_in_sector,
                            self.farmer_protocol_info.recent_segments,
                            self.farmer_protocol_info.recent_history_fraction,
                        )
                    })
                    .collect_into(&mut piece_indexes);

                Ok(PlottedSector {
                    sector_id,
                    sector_index,
                    sector_metadata,
                    piece_indexes,
                })
            },
        )
    }

    /// Get piece cache instance
    pub fn piece_cache(&self) -> DiskPieceCache {
        self.piece_cache.clone()
    }

    /// Get piece reader to read plotted pieces later
    pub fn piece_reader(&self) -> PieceReader {
        self.piece_reader.clone()
    }

    /// Subscribe to sector plotting notification
    ///
    /// Plotting permit is given such that it can be dropped later by the implementation is
    /// throttling of the plotting process is desired.
    pub fn on_sector_plotted(
        &self,
        callback: HandlerFn<(PlottedSector, Option<PlottedSector>)>,
    ) -> HandlerId {
        self.handlers.sector_plotted.add(callback)
    }

    /// Subscribe to new solution notification
    pub fn on_solution(&self, callback: HandlerFn<SolutionResponse>) -> HandlerId {
        self.handlers.solution.add(callback)
    }

    /// Run and wait for background threads to exit or return an error
    pub async fn run(mut self) -> anyhow::Result<()> {
        if let Some(start_sender) = self.start_sender.take() {
            // Do not care if anyone is listening on the other side
            let _ = start_sender.send(());
        }

        while let Some(result) = self.tasks.next().instrument(self.span.clone()).await {
            result?;
        }

        Ok(())
    }

    /// Wipe everything that belongs to this single disk farm
    pub fn wipe(directory: &Path) -> io::Result<()> {
        let single_disk_info_info_path = directory.join(SingleDiskFarmInfo::FILE_NAME);
        match SingleDiskFarmInfo::load_from(directory) {
            Ok(Some(single_disk_farm_info)) => {
                info!("Found single disk farm {}", single_disk_farm_info.id());
            }
            Ok(None) => {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!(
                        "Single disk farm info not found at {}",
                        single_disk_info_info_path.display()
                    ),
                ));
            }
            Err(error) => {
                warn!("Found unknown single disk farm: {}", error);
            }
        }

        {
            let plot = directory.join(Self::PLOT_FILE);
            info!("Deleting plot file at {}", plot.display());
            fs::remove_file(plot)?;
        }
        {
            let metadata = directory.join(Self::METADATA_FILE);
            info!("Deleting metadata file at {}", metadata.display());
            fs::remove_file(metadata)?;
        }
        // TODO: Identity should be able to wipe itself instead of assuming a specific file name
        //  here
        {
            let identity = directory.join("identity.bin");
            info!("Deleting identity file at {}", identity.display());
            fs::remove_file(identity)?;
        }

        DiskPieceCache::wipe(directory)?;

        // TODO: Remove this compatibility hack after enough time has passed
        if directory.join("single_disk_plot.json").exists() {
            info!(
                "Deleting info file at {}",
                directory.join("single_disk_plot.json").display()
            );
            fs::remove_file(directory.join("single_disk_plot.json"))
        } else {
            info!(
                "Deleting info file at {}",
                single_disk_info_info_path.display()
            );
            fs::remove_file(single_disk_info_info_path)
        }
    }

    /// Check the farm for corruption and repair errors (caused by disk errors or something else),
    /// returns an error when irrecoverable errors occur.
    pub fn scrub(directory: &Path) -> Result<(), SingleDiskFarmScrubError> {
        let span = Span::current();

        let info = {
            let file = directory.join(SingleDiskFarmInfo::FILE_NAME);
            info!(path = %file.display(), "Checking info file");

            match SingleDiskFarmInfo::load_from(directory) {
                Ok(Some(info)) => info,
                Ok(None) => {
                    return Err(SingleDiskFarmScrubError::FarmInfoFileDoesNotExist { file });
                }
                Err(error) => {
                    return Err(SingleDiskFarmScrubError::FarmInfoCantBeOpened { file, error });
                }
            }
        };
        let identity = {
            let file = directory.join(Identity::FILE_NAME);
            info!(path = %file.display(), "Checking identity file");

            match Identity::open(directory) {
                Ok(Some(identity)) => identity,
                Ok(None) => {
                    return Err(SingleDiskFarmScrubError::IdentityFileDoesNotExist { file });
                }
                Err(error) => {
                    return Err(SingleDiskFarmScrubError::IdentityCantBeOpened { file, error });
                }
            }
        };

        if PublicKey::from(identity.public.to_bytes()) != *info.public_key() {
            return Err(SingleDiskFarmScrubError::PublicKeyMismatch {
                identity: PublicKey::from(identity.public.to_bytes()),
                info: *info.public_key(),
            });
        }

        let sector_metadata_size = SectorMetadataChecksummed::encoded_size();

        let metadata_file_path = directory.join(Self::METADATA_FILE);
        let (metadata_file, mut metadata_header) = {
            info!(path = %metadata_file_path.display(), "Checking metadata file");

            let mut metadata_file = match OpenOptions::new()
                .read(true)
                .write(true)
                .open(&metadata_file_path)
            {
                Ok(metadata_file) => metadata_file,
                Err(error) => {
                    return Err(if error.kind() == io::ErrorKind::NotFound {
                        SingleDiskFarmScrubError::MetadataFileDoesNotExist {
                            file: metadata_file_path,
                        }
                    } else {
                        SingleDiskFarmScrubError::MetadataCantBeOpened {
                            file: metadata_file_path,
                            error,
                        }
                    });
                }
            };

            // Error doesn't matter here
            let _ = metadata_file.advise_sequential_access();

            let metadata_size = match metadata_file.seek(SeekFrom::End(0)) {
                Ok(metadata_size) => metadata_size,
                Err(error) => {
                    return Err(SingleDiskFarmScrubError::FailedToDetermineFileSize {
                        file: metadata_file_path,
                        error,
                    });
                }
            };

            if metadata_size < RESERVED_PLOT_METADATA {
                return Err(SingleDiskFarmScrubError::MetadataFileTooSmall {
                    file: metadata_file_path,
                    reserved_size: RESERVED_PLOT_METADATA,
                    size: metadata_size,
                });
            }

            let mut metadata_header = {
                let mut reserved_metadata = vec![0; RESERVED_PLOT_METADATA as usize];

                if let Err(error) = metadata_file.read_exact_at(&mut reserved_metadata, 0) {
                    return Err(SingleDiskFarmScrubError::FailedToReadBytes {
                        file: metadata_file_path,
                        size: RESERVED_PLOT_METADATA,
                        offset: 0,
                        error,
                    });
                }

                PlotMetadataHeader::decode(&mut reserved_metadata.as_slice())
                    .map_err(SingleDiskFarmScrubError::FailedToDecodeMetadataHeader)?
            };

            if metadata_header.version != Self::SUPPORTED_PLOT_VERSION {
                return Err(SingleDiskFarmScrubError::UnexpectedMetadataVersion(
                    metadata_header.version,
                ));
            }

            let plotted_sector_count = metadata_header.plotted_sector_count;

            let expected_metadata_size = RESERVED_PLOT_METADATA
                + sector_metadata_size as u64 * u64::from(plotted_sector_count);

            if metadata_size < expected_metadata_size {
                warn!(
                    %metadata_size,
                    %expected_metadata_size,
                    "Metadata file size is smaller than expected, shrinking number of plotted \
                    sectors to correct value"
                );

                metadata_header.plotted_sector_count = ((metadata_size - RESERVED_PLOT_METADATA)
                    / sector_metadata_size as u64)
                    as SectorIndex;
                let metadata_header_bytes = metadata_header.encode();
                if let Err(error) = metadata_file.write_all_at(&metadata_header_bytes, 0) {
                    return Err(SingleDiskFarmScrubError::FailedToWriteBytes {
                        file: metadata_file_path,
                        size: metadata_header_bytes.len() as u64,
                        offset: 0,
                        error,
                    });
                }
            }

            (metadata_file, metadata_header)
        };

        let pieces_in_sector = info.pieces_in_sector();
        let sector_size = sector_size(pieces_in_sector) as u64;

        let plot_file_path = directory.join(Self::PLOT_FILE);
        let plot_file = {
            let plot_file_path = directory.join(Self::PLOT_FILE);
            info!(path = %plot_file_path.display(), "Checking plot file");

            let mut plot_file = match OpenOptions::new()
                .read(true)
                .write(true)
                .open(&plot_file_path)
            {
                Ok(plot_file) => plot_file,
                Err(error) => {
                    return Err(if error.kind() == io::ErrorKind::NotFound {
                        SingleDiskFarmScrubError::MetadataFileDoesNotExist {
                            file: plot_file_path,
                        }
                    } else {
                        SingleDiskFarmScrubError::MetadataCantBeOpened {
                            file: plot_file_path,
                            error,
                        }
                    });
                }
            };

            // Error doesn't matter here
            let _ = plot_file.advise_sequential_access();

            let plot_size = match plot_file.seek(SeekFrom::End(0)) {
                Ok(metadata_size) => metadata_size,
                Err(error) => {
                    return Err(SingleDiskFarmScrubError::FailedToDetermineFileSize {
                        file: plot_file_path,
                        error,
                    });
                }
            };

            let min_expected_plot_size =
                u64::from(metadata_header.plotted_sector_count) * sector_size;
            if plot_size < min_expected_plot_size {
                warn!(
                    %plot_size,
                    %min_expected_plot_size,
                    "Plot file size is smaller than expected, shrinking number of plotted \
                    sectors to correct value"
                );

                metadata_header.plotted_sector_count = (plot_size / sector_size) as SectorIndex;
                let metadata_header_bytes = metadata_header.encode();
                if let Err(error) = metadata_file.write_all_at(&metadata_header_bytes, 0) {
                    return Err(SingleDiskFarmScrubError::FailedToWriteBytes {
                        file: plot_file_path,
                        size: metadata_header_bytes.len() as u64,
                        offset: 0,
                        error,
                    });
                }
            }

            plot_file
        };

        info!("Checking sectors and corresponding metadata");
        (0..metadata_header.plotted_sector_count)
            .into_par_iter()
            .map_init(
                || {
                    let sector_metadata_bytes = vec![0; sector_metadata_size];
                    let piece = Piece::default();

                    (sector_metadata_bytes, piece)
                },
                |(sector_metadata_bytes, piece), sector_index| {
                    let _span_guard = span.enter();

                    let offset = RESERVED_PLOT_METADATA
                        + u64::from(sector_index) * sector_metadata_size as u64;
                    if let Err(error) = metadata_file.read_exact_at(sector_metadata_bytes, offset) {
                        warn!(
                            path = %metadata_file_path.display(),
                            %error,
                            %sector_index,
                            %offset,
                            "Failed to read sector metadata, replacing with dummy expired sector \
                            metadata"
                        );

                        write_dummy_sector_metadata(
                            &metadata_file,
                            &metadata_file_path,
                            sector_index,
                            pieces_in_sector,
                        )?;
                        return Ok(());
                    }

                    let sector_metadata = match SectorMetadataChecksummed::decode(
                        &mut sector_metadata_bytes.as_slice(),
                    ) {
                        Ok(sector_metadata) => sector_metadata,
                        Err(error) => {
                            warn!(
                                path = %metadata_file_path.display(),
                                %error,
                                %sector_index,
                                "Failed to decode sector metadata, replacing with dummy expired \
                                sector metadata"
                            );

                            write_dummy_sector_metadata(
                                &metadata_file,
                                &metadata_file_path,
                                sector_index,
                                pieces_in_sector,
                            )?;
                            return Ok(());
                        }
                    };

                    if sector_metadata.sector_index != sector_index {
                        warn!(
                            path = %metadata_file_path.display(),
                            %sector_index,
                            found_sector_index = sector_metadata.sector_index,
                            "Sector index mismatch, replacing with dummy expired sector metadata"
                        );

                        write_dummy_sector_metadata(
                            &metadata_file,
                            &metadata_file_path,
                            sector_index,
                            pieces_in_sector,
                        )?;
                        return Ok(());
                    }

                    if sector_metadata.pieces_in_sector != pieces_in_sector {
                        warn!(
                            path = %metadata_file_path.display(),
                            %sector_index,
                            %pieces_in_sector,
                            found_pieces_in_sector = sector_metadata.pieces_in_sector,
                            "Pieces in sector mismatch, replacing with dummy expired sector \
                            metadata"
                        );

                        write_dummy_sector_metadata(
                            &metadata_file,
                            &metadata_file_path,
                            sector_index,
                            pieces_in_sector,
                        )?;
                        return Ok(());
                    }

                    let mut hasher = blake3::Hasher::new();
                    for piece_offset in 0..pieces_in_sector {
                        let offset = u64::from(sector_index) * sector_size
                            + u64::from(piece_offset) * Piece::SIZE as u64;

                        if let Err(error) = plot_file.read_exact_at(piece.as_mut(), offset) {
                            warn!(
                                path = %plot_file_path.display(),
                                %error,
                                %sector_index,
                                %piece_offset,
                                size = %piece.len() as u64,
                                %offset,
                                "Failed to read piece bytes"
                            );
                            return Err(SingleDiskFarmScrubError::FailedToReadBytes {
                                file: plot_file_path.clone(),
                                size: piece.len() as u64,
                                offset,
                                error,
                            });
                        }

                        hasher.update(piece.as_ref());
                    }

                    let actual_checksum = *hasher.finalize().as_bytes();
                    let mut expected_checksum = [0; mem::size_of::<Blake3Hash>()];
                    {
                        let offset = u64::from(sector_index) * sector_size
                            + u64::from(pieces_in_sector) * Piece::SIZE as u64;
                        if let Err(error) = plot_file.read_exact_at(&mut expected_checksum, offset)
                        {
                            return Err(SingleDiskFarmScrubError::FailedToReadBytes {
                                file: plot_file_path.clone(),
                                size: expected_checksum.len() as u64,
                                offset,
                                error,
                            });
                        }
                    }

                    // Verify checksum
                    if actual_checksum != expected_checksum {
                        debug!(
                            path = %plot_file_path.display(),
                            %sector_index,
                            actual_checksum = %hex::encode(actual_checksum),
                            expected_checksum = %hex::encode(expected_checksum),
                            "Plotted sector checksum mismatch, replacing with dummy expired sector"
                        );

                        write_dummy_sector_metadata(
                            &metadata_file,
                            &metadata_file_path,
                            sector_index,
                            pieces_in_sector,
                        )?;

                        *piece = Piece::default();

                        // Write dummy pieces
                        let mut hasher = blake3::Hasher::new();
                        for piece_offset in 0..pieces_in_sector {
                            let offset = u64::from(sector_index) * sector_size
                                + u64::from(piece_offset) * Piece::SIZE as u64;

                            if let Err(error) = plot_file.write_all_at(piece.as_ref(), offset) {
                                return Err(SingleDiskFarmScrubError::FailedToWriteBytes {
                                    file: plot_file_path.clone(),
                                    size: piece.len() as u64,
                                    offset,
                                    error,
                                });
                            }

                            hasher.update(piece.as_ref());
                        }

                        let offset = u64::from(sector_index) * sector_size
                            + u64::from(pieces_in_sector) * Piece::SIZE as u64;

                        // Write checksum
                        if let Err(error) =
                            plot_file.write_all_at(hasher.finalize().as_bytes(), offset)
                        {
                            return Err(SingleDiskFarmScrubError::FailedToWriteBytes {
                                file: plot_file_path.clone(),
                                size: hasher.finalize().as_bytes().len() as u64,
                                offset,
                                error,
                            });
                        }

                        return Ok(());
                    }

                    trace!(%sector_index, "Sector is in good shape");

                    Ok(())
                },
            )
            .try_for_each({
                let span = &span;
                let checked_sectors = AtomicUsize::new(0);

                move |result| {
                    let _span_guard = span.enter();

                    let checked_sectors = checked_sectors.fetch_add(1, Ordering::Relaxed);
                    if checked_sectors > 1 && checked_sectors % 10 == 0 {
                        info!(
                            "Checked {}/{} sectors",
                            checked_sectors, metadata_header.plotted_sector_count
                        );
                    }

                    result
                }
            })?;

        {
            let file = directory.join(DiskPieceCache::FILE_NAME);
            info!(path = %file.display(), "Checking cache file");

            let mut cache_file = match OpenOptions::new().read(true).write(true).open(&file) {
                Ok(plot_file) => plot_file,
                Err(error) => {
                    return Err(if error.kind() == io::ErrorKind::NotFound {
                        SingleDiskFarmScrubError::CacheFileDoesNotExist { file }
                    } else {
                        SingleDiskFarmScrubError::CacheCantBeOpened { file, error }
                    });
                }
            };

            // Error doesn't matter here
            let _ = cache_file.advise_sequential_access();

            let cache_size = match cache_file.seek(SeekFrom::End(0)) {
                Ok(metadata_size) => metadata_size,
                Err(error) => {
                    return Err(SingleDiskFarmScrubError::FailedToDetermineFileSize {
                        file,
                        error,
                    });
                }
            };

            let element_size = DiskPieceCache::element_size();
            let number_of_cached_elements = cache_size / element_size as u64;
            let dummy_element = vec![0; element_size];
            (0..number_of_cached_elements)
                .into_par_iter()
                .map_with(vec![0; element_size], |element, cache_offset| {
                    let _span_guard = span.enter();

                    let offset = cache_offset * element_size as u64;
                    if let Err(error) = cache_file.read_exact_at(element, offset) {
                        warn!(
                            path = %file.display(),
                            %cache_offset,
                            size = %element.len() as u64,
                            %offset,
                            %error,
                            "Failed to read cached piece, replacing with dummy element"
                        );

                        if let Err(error) = cache_file.write_all_at(&dummy_element, offset) {
                            return Err(SingleDiskFarmScrubError::FailedToWriteBytes {
                                file: file.clone(),
                                size: element_size as u64,
                                offset,
                                error,
                            });
                        }

                        return Ok(());
                    }

                    let (index_and_piece_bytes, expected_checksum) =
                        element.split_at(element_size - mem::size_of::<Blake3Hash>());
                    let actual_checksum = blake3_hash(index_and_piece_bytes);
                    if actual_checksum != expected_checksum && element != &dummy_element {
                        warn!(
                            %cache_offset,
                            actual_checksum = %hex::encode(actual_checksum),
                            expected_checksum = %hex::encode(expected_checksum),
                            "Cached piece checksum mismatch, replacing with dummy element"
                        );

                        if let Err(error) = cache_file.write_all_at(&dummy_element, offset) {
                            return Err(SingleDiskFarmScrubError::FailedToWriteBytes {
                                file: file.clone(),
                                size: element_size as u64,
                                offset,
                                error,
                            });
                        }

                        return Ok(());
                    }

                    Ok(())
                })
                .try_for_each({
                    let span = &span;
                    let checked_elements = AtomicUsize::new(0);

                    move |result| {
                        let _span_guard = span.enter();

                        let checked_elements = checked_elements.fetch_add(1, Ordering::Relaxed);
                        if checked_elements > 1 && checked_elements % 1000 == 0 {
                            info!(
                                "Checked {}/{} cache elements",
                                checked_elements, number_of_cached_elements
                            );
                        }

                        result
                    }
                })?;
        }

        info!("Farm check completed");

        Ok(())
    }
}

fn write_dummy_sector_metadata(
    metadata_file: &File,
    metadata_file_path: &Path,
    sector_index: SectorIndex,
    pieces_in_sector: u16,
) -> Result<(), SingleDiskFarmScrubError> {
    let dummy_sector_bytes = SectorMetadataChecksummed::from(SectorMetadata {
        sector_index,
        pieces_in_sector,
        s_bucket_sizes: Box::new([0; Record::NUM_S_BUCKETS]),
        history_size: HistorySize::from(SegmentIndex::ZERO),
    })
    .encode();
    let sector_offset = RESERVED_PLOT_METADATA
        + u64::from(sector_index) * SectorMetadataChecksummed::encoded_size() as u64;
    metadata_file
        .write_all_at(&dummy_sector_bytes, sector_offset)
        .map_err(|error| SingleDiskFarmScrubError::FailedToWriteBytes {
            file: metadata_file_path.to_path_buf(),
            size: dummy_sector_bytes.len() as u64,
            offset: sector_offset,
            error,
        })
}
