use crate::BenchmarkArgs;
use anyhow::{anyhow, Context, Result};
use criterion::{black_box, Criterion, SamplingMode, Throughput};
use memmap2::{Mmap, MmapOptions};
use parity_scale_codec::Decode;
use rand::prelude::*;
use std::fs::OpenOptions;
use std::num::NonZeroUsize;
use std::time::Instant;
use subspace_core_primitives::crypto::kzg;
use subspace_core_primitives::crypto::kzg::Kzg;
use subspace_core_primitives::{
    Blake2b256Hash, Record, RecordedHistorySegment, SectorIndex, SolutionRange,
};
use subspace_erasure_coding::ErasureCoding;
use subspace_farmer::single_disk_farm::{
    PlotMetadataHeader, SingleDiskFarmInfo, RESERVED_PLOT_METADATA,
};
use subspace_farmer_components::auditing::audit_sector;
use subspace_farmer_components::sector::{sector_size, SectorMetadataChecksummed};
use subspace_proof_of_space::chia::ChiaTable;
use subspace_proof_of_space::Table;
use tracing::{debug, error, info, trace, warn};

type PosTable = ChiaTable;

pub(crate) fn proving(benchmark_args: BenchmarkArgs) -> Result<()> {
    let BenchmarkArgs {
        base_path,
        sector_count,
        sample_size,
        resamples_count,
        noise_threshold,
        confidence_level,
        significance_level,
        sampling_mode,
    } = benchmark_args;

    info!("Initializing proving benchmark.");

    let sampling_mode = match sampling_mode.as_str() {
        "auto" => SamplingMode::Auto,
        "linear" => SamplingMode::Linear,
        "flat" => SamplingMode::Flat,
        other => {
            error!("Invalid sampling mode provided: {}.", other);
            return Err(anyhow!("Invalid sampling mode: {}.", other));
        }
    };

    debug!("Using sampling mode: {:?}.", sampling_mode);

    debug!("Initializing criterion.");

    let mut c = Criterion::default()
        .sample_size(sample_size)
        .nresamples(resamples_count)
        .noise_threshold(noise_threshold)
        .confidence_level(confidence_level)
        .significance_level(significance_level);

    if !base_path.exists() {
        error!(
            "Base path does not exist. base_path = {}",
            base_path.display()
        );
        return Err(anyhow!(
            "Base path does not exist. base_path = {}",
            base_path.display()
        ));
    }

    if !base_path.is_dir() {
        error!(
            "Base path is not a directory. base_path = {}",
            base_path.display()
        );
        return Err(anyhow!(
            "Base path is not a directory. base_path = {}",
            base_path.display()
        ));
    }

    debug!("Base path exists and is valid.");

    debug!("Loading single disk farm info.");
    let single_disk_farm_info = match SingleDiskFarmInfo::load_from(&base_path)
        .context("Failed to load SingleDiskFarmInfo.")
        .unwrap()
    {
        Some(i) => i,
        None => {
            return Err(anyhow!(
                "SingleDiskFarmInfo is empty. base_path = {}",
                base_path.display()
            ));
        }
    };

    let public_key = single_disk_farm_info.public_key();
    let pieces_in_sector = single_disk_farm_info.pieces_in_sector();
    let solution_range = SolutionRange::MAX;
    let sector_size = sector_size(pieces_in_sector);
    let sector_metadata_size = SectorMetadataChecksummed::encoded_size();
    let mut input = RecordedHistorySegment::new_boxed();
    let mut rng = StdRng::seed_from_u64(42);
    rng.fill(AsMut::<[u8]>::as_mut(input.as_mut()));
    let kzg = Kzg::new(kzg::embedded_kzg_settings());
    let erasure_coding = ErasureCoding::new(
        NonZeroUsize::new(Record::NUM_S_BUCKETS.next_power_of_two().ilog2() as usize).unwrap(),
    )
    .unwrap();
    let mut table_generator = PosTable::generator();

    let plot_file_path = base_path.join("plot.bin");
    debug!("Opening a plot file. path = {}", plot_file_path.display());
    let plot_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&plot_file_path)
        .context(format!(
            "Failed to open plot file. path = {}",
            plot_file_path.display()
        ))?;

    let target_sector_count = plot_file
        .metadata()
        .context("Failed to retrieve plot file metadata.")?
        .len()
        / sector_size as u64;
    let mut sectors_metadata =
        Vec::<SectorMetadataChecksummed>::with_capacity(target_sector_count as usize);

    let metadata_file_path = base_path.join("metadata.bin");
    debug!(
        "Opening a metadata file. path = {}",
        metadata_file_path.display()
    );
    let metadata_file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&metadata_file_path)
        .context(format!(
            "Failed to open metadata file. path = {}",
            metadata_file_path.display()
        ))?;

    debug!("Memory mapping of plot.");
    let plot_mmap =
        unsafe { Mmap::map(&plot_file).context("Failed to create memory mapping for plot file.")? };

    debug!("Memory mapping of metadata header.");
    let metadata_header_mmap = unsafe {
        MmapOptions::new()
            .len(PlotMetadataHeader::encoded_size())
            .map_mut(&metadata_file)
            .context("Failed to create mmap for metadata header.")?
    };
    debug!("Successfully memory mapped metadata header.");

    debug!("Memory mapping of metadata.");
    let metadata_mmap = unsafe {
        MmapOptions::new()
            .offset(RESERVED_PLOT_METADATA)
            .len(sector_metadata_size * target_sector_count as usize)
            .map(&metadata_file)
            .context("Failed to create mmap for metadata.")?
    };

    #[cfg(unix)]
    {
        debug!("Advicing random IO for plot and metadata memory mappings.");
        plot_mmap
            .advise(memmap2::Advice::Random)
            .context("Failed to set advice for plot mmap.")?;
        metadata_mmap
            .advise(memmap2::Advice::Random)
            .context("Failed to set advice for metadata mmap.")?;
    }

    debug!("Decoding metadata headers");
    let metadata_header = PlotMetadataHeader::decode(&mut metadata_header_mmap.as_ref())
        .context("Failed to decode metadata header.")?;

    debug!("Reading and decoding sectors metadata.");
    for mut sector_metadata_bytes in metadata_mmap
        .chunks_exact(sector_metadata_size)
        .take(metadata_header.plotted_sector_count as usize)
    {
        sectors_metadata.push(
            SectorMetadataChecksummed::decode(&mut sector_metadata_bytes)
                .context("Failed to decode sector metadata")?,
        );
    }

    if metadata_header.plotted_sector_count == 0 {
        error!("No plotted sectors found.");
        return Err(anyhow!("There are no plotted sectors to benchmark on."));
    }

    let mut sector_count = match sector_count {
        Some(n) => u16::from(n),
        None => metadata_header.plotted_sector_count,
    };

    if sector_count == 0 {
        sector_count = metadata_header.plotted_sector_count;
    }

    if (metadata_header.plotted_sector_count as u16) < sector_count {
        warn!(
            "Specified amount of sectors ({}) exceeds amount of plotted sectors ({}). Using the latter number.",
            sector_count, metadata_header.plotted_sector_count
        );
        sector_count = metadata_header.plotted_sector_count;
    }

    let single_sector = (&plot_mmap[..sector_size]).to_vec();

    debug!("Obtaining global challenge.");

    let global_challenge = loop {
        let mut global_challenge = Blake2b256Hash::default();
        rng.fill_bytes(&mut global_challenge);

        let maybe_solution_candidates = audit_sector(
            &public_key,
            0 as SectorIndex,
            &global_challenge,
            solution_range,
            &single_sector,
            &sectors_metadata[0],
        );

        let solution_candidates = match maybe_solution_candidates {
            Some(solution_candidates) => solution_candidates,
            None => {
                continue;
            }
        };

        let num_actual_solutions = solution_candidates
            .clone()
            .into_iter::<_, PosTable>(&public_key, &kzg, &erasure_coding, &mut table_generator)
            .unwrap()
            .len();

        if num_actual_solutions > 0 {
            break global_challenge;
        }
    };

    info!("Running proving benchmark with single sector.");

    let mut group = c.benchmark_group("proving");
    group.throughput(Throughput::Elements(1));
    {
        let solution_candidates = audit_sector(
            &public_key,
            0 as SectorIndex,
            &global_challenge,
            solution_range,
            &single_sector,
            &sectors_metadata[0],
        )
        .unwrap();

        group.bench_function("memory", |b| {
            b.iter(|| {
                solution_candidates
                    .clone()
                    .into_iter::<_, PosTable>(
                        black_box(&public_key),
                        black_box(&kzg),
                        black_box(&erasure_coding),
                        black_box(&mut table_generator),
                    )
                    .unwrap()
                    // Process just one solution
                    .next()
                    .unwrap()
                    .unwrap();
            })
        });
    }

    drop(single_sector);

    let solution_candidates = plot_mmap
        .chunks_exact(sector_size)
        .enumerate()
        .map(|(sector_index, sector)| {
            trace!("Processing sector {}", sector_index);
            audit_sector(
                &public_key,
                sector_index as SectorIndex,
                &global_challenge,
                solution_range,
                sector,
                &sectors_metadata[sector_index],
            )
            .unwrap()
        })
        .collect::<Vec<_>>();

    info!("Running proving benchmark. sector_count = {}", sector_count);

    group.throughput(Throughput::Elements(sector_count as u64));
    group.sampling_mode(sampling_mode);
    group.bench_function("disk", move |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _i in 0..iters {
                for solution_candidates in solution_candidates.clone() {
                    solution_candidates
                        .into_iter::<_, PosTable>(
                            black_box(&public_key),
                            black_box(&kzg),
                            black_box(&erasure_coding),
                            black_box(&mut table_generator),
                        )
                        .unwrap()
                        // Process just one solution
                        .next()
                        .unwrap()
                        .unwrap();
                }
            }
            start.elapsed()
        });
    });

    drop(plot_file);
    drop(metadata_file);
    group.finish();

    c.final_summary();

    Ok(())
}
