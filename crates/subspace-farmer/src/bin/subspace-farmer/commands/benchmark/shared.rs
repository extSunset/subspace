use parity_scale_codec::{Decode, Encode};
use subspace_core_primitives::SectorIndex;

pub(crate) const RESERVED_PLOT_METADATA: u64 = 1024 * 1024;

#[derive(Debug, Encode, Decode)]
pub(crate) struct PlotMetadataHeader {
    pub(crate) version: u8,
    pub(crate) plotted_sector_count: SectorIndex,
}

impl PlotMetadataHeader {
    #[inline]
    pub(crate) fn encoded_size() -> usize {
        let default = PlotMetadataHeader {
            version: 0,
            plotted_sector_count: 0,
        };

        default.encoded_size()
    }
}

