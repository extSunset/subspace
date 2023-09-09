#![feature(const_option, type_changing_struct_update)]

mod commands;
mod ss58;
mod utils;

use bytesize::ByteSize;
use clap::builder::PossibleValuesParser;
use clap::{Parser, Subcommand, ValueHint};
use ss58::parse_ss58_reward_address;
use std::fs;
use std::num::{NonZeroU16, NonZeroU8};
use std::path::PathBuf;
use std::str::FromStr;
use subspace_core_primitives::PublicKey;
use subspace_farmer::single_disk_farm::SingleDiskFarm;
use subspace_networking::libp2p::Multiaddr;
use subspace_proof_of_space::chia::ChiaTable;
use tracing::info;
use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::prelude::*;
use tracing_subscriber::{fmt, EnvFilter};

type PosTable = ChiaTable;

#[cfg(all(
    target_arch = "x86_64",
    target_vendor = "unknown",
    target_os = "linux",
    target_env = "gnu"
))]
#[global_allocator]
static GLOBAL: jemallocator::Jemalloc = jemallocator::Jemalloc;

/// Arguments for farmer
#[derive(Debug, Parser)]
struct FarmingArgs {
    /// One or more farm located at specified path, each with its own allocated space.
    ///
    /// In case of multiple disks, it is recommended to specify them individually rather than using
    /// RAID 0, that way farmer will be able to better take advantage of concurrency of individual
    /// drives.
    ///
    /// Format for each farm is coma-separated list of strings like this:
    ///
    ///   path=/path/to/directory,size=5T
    ///
    /// `size` is max allocated size in human readable format (e.g. 10GB, 2TiB) or just bytes that
    /// farmer will make sure not not exceed (and will pre-allocated all the space on startup to
    /// ensure it will not run out of space in runtime).
    disk_farms: Vec<DiskFarm>,
    /// WebSocket RPC URL of the Subspace node to connect to
    #[arg(long, value_hint = ValueHint::Url, default_value = "ws://127.0.0.1:9944")]
    node_rpc_url: String,
    /// Address for farming rewards
    #[arg(long, value_parser = parse_ss58_reward_address)]
    reward_address: PublicKey,
    /// Percentage of allocated space dedicated for caching purposes, 99% max
    #[arg(long, default_value = "1", value_parser = cache_percentage_parser)]
    cache_percentage: NonZeroU8,
    /// Sets some flags that are convenient during development, currently `--enable-private-ips`.
    #[arg(long)]
    dev: bool,
    /// Run temporary farmer with specified plot size in human readable format (e.g. 10GB, 2TiB) or
    /// just bytes (e.g. 4096), this will create a temporary directory for storing farmer data that
    /// will be deleted at the end of the process.
    #[arg(long, conflicts_with = "disk_farms")]
    tmp: Option<ByteSize>,
    /// Maximum number of pieces in sector (can override protocol value to something lower).
    ///
    /// This will make plotting of individual sectors faster, decrease load on CPU proving, but also
    /// proportionally increase amount of disk reads during audits since every sector needs to be
    /// audited and there will be more of them.
    ///
    /// This is primarily for development and not recommended to use by regular users.
    #[arg(long)]
    max_pieces_in_sector: Option<u16>,
    /// DSN parameters
    #[clap(flatten)]
    dsn: DsnArgs,
    /// Do not print info about configured farms on startup.
    #[arg(long)]
    no_info: bool,
}

fn cache_percentage_parser(s: &str) -> anyhow::Result<NonZeroU8> {
    let cache_percentage = NonZeroU8::from_str(s)?;

    if cache_percentage.get() > 99 {
        return Err(anyhow::anyhow!("Cache percentage can't exceed 99"));
    }

    Ok(cache_percentage)
}

/// Arguments for DSN
#[derive(Debug, Parser)]
struct DsnArgs {
    /// Multiaddrs of bootstrap nodes to connect to on startup, multiple are supported
    #[arg(long)]
    bootstrap_nodes: Vec<Multiaddr>,
    /// Multiaddr to listen on for subspace networking, for instance `/ip4/0.0.0.0/tcp/0`,
    /// multiple are supported.
    #[arg(long, default_value = "/ip4/0.0.0.0/tcp/30533")]
    listen_on: Vec<Multiaddr>,
    /// Determines whether we allow keeping non-global (private, shared, loopback..) addresses in
    /// Kademlia DHT.
    #[arg(long, default_value_t = false)]
    enable_private_ips: bool,
    /// Multiaddrs of reserved nodes to maintain a connection to, multiple are supported
    #[arg(long)]
    reserved_peers: Vec<Multiaddr>,
    /// Defines max established incoming connection limit.
    #[arg(long, default_value_t = 50)]
    in_connections: u32,
    /// Defines max established outgoing swarm connection limit.
    #[arg(long, default_value_t = 50)]
    out_connections: u32,
    /// Defines max pending incoming connection limit.
    #[arg(long, default_value_t = 50)]
    pending_in_connections: u32,
    /// Defines max pending outgoing swarm connection limit.
    #[arg(long, default_value_t = 50)]
    pending_out_connections: u32,
    /// Defines target total (in and out) connection number that should be maintained.
    #[arg(long, default_value_t = 50)]
    target_connections: u32,
    /// Known external addresses
    #[arg(long, alias = "external-address")]
    external_addresses: Vec<Multiaddr>,
}

#[derive(Debug, Parser)]
struct BenchmarkArgs {
    /// Single farm to run benchmark on.
    base_path: PathBuf,
    /// Amount of sectors from plot to use for this benchmark.
    #[arg(long)]
    sector_count: Option<NonZeroU16>,
    #[arg(long, default_value_t = false)]
    parallel: bool,
    /// Changes the size of the sample for this benchmark.
    #[arg(long, default_value_t = 10, value_parser = sample_size_parser)]
    sample_size: usize,
    /// Number of resamples to use for the bootstrap for this benchmark.
    #[arg(long, default_value_t = 100_000, value_parser = resamples_count_parser)]
    resamples_count: usize,
    #[arg(long, default_value_t = 0.01, value_parser = positive_f64_parser)]
    noise_threshold: f64,
    /// Changes the confidence level for this benchmark.
    /// The confidence level is the desired probability that the true runtime lies within the estimated confidence interval.
    /// Can be in range (0..1).
    #[arg(long, default_value_t = 0.95, value_parser = percent_f64_parser)]
    confidence_level: f64,
    /// Sets the probability that two identical code measurements are considered 'different' due to measurement noise.
    /// Lower values increase robustness against noise but may miss small true performance changes;
    /// higher values detect minute changes but may report more noise. Adjust based on desired noise sensitivity.
    /// Can be in range (0..1).
    #[arg(long, default_value_t = 0.05, value_parser = percent_f64_parser)]
    significance_level: f64,
    /// Set the sampling mode for this benchmark.
    /// Auto: default, criterion chooses the best method, suitable for most benchmarks.
    /// Linear: scales iteration count linearly, suitable for most benchmarks but can be slow for lengthy ones.
    /// Flat: same iteration count for all samples, not recommended due to reduced statistical precision, but faster for very long benchmarks.
    #[arg(long, default_value = "auto", value_parser = PossibleValuesParser::new(["auto", "linear", "flat"]))]
    sampling_mode: String,
}

fn sample_size_parser(s: &str) -> anyhow::Result<usize> {
    let sample_size = usize::from_str(s)?;

    if sample_size < 10 {
        return Err(anyhow::anyhow!("The sample size cannot be less than 10."));
    }

    Ok(sample_size)
}

fn resamples_count_parser(s: &str) -> anyhow::Result<usize> {
    let resamples_count = usize::from_str(s)?;

    if resamples_count < 1 {
        return Err(anyhow::anyhow!(
            "The resamples count cannot be less than 1."
        ));
    }

    Ok(resamples_count)
}

fn positive_f64_parser(s: &str) -> anyhow::Result<f64> {
    let positive_f64 = f64::from_str(s)?;

    if positive_f64 <= 0.0 {
        return Err(anyhow::anyhow!("The value cannot be less than zero."));
    }

    Ok(positive_f64)
}

fn percent_f64_parser(s: &str) -> anyhow::Result<f64> {
    let percent_f64 = f64::from_str(s)?;

    if percent_f64 > 1.0 || percent_f64 < 0.0 {
        return Err(anyhow::anyhow!("Tte value should be in range (0..1)."));
    }

    Ok(percent_f64)
}

#[derive(Debug, Subcommand)]
enum BenchCommand {
    /// Run sector audit benchmark.
    Audit(BenchmarkArgs),
    /// Run proving benchmark.
    Proving(BenchmarkArgs),
}

#[derive(Debug, Clone)]
struct DiskFarm {
    /// Path to directory where data is stored.
    directory: PathBuf,
    /// How much space in bytes can farm use for plots (metadata space is not included)
    allocated_plotting_space: u64,
}

impl FromStr for DiskFarm {
    type Err = String;

    fn from_str(s: &str) -> anyhow::Result<Self, Self::Err> {
        let parts = s.split(',').collect::<Vec<_>>();
        if parts.len() != 2 {
            return Err("Must contain 2 coma-separated components".to_string());
        }

        let mut plot_directory = None;
        let mut allocated_plotting_space = None;

        for part in parts {
            let part = part.splitn(2, '=').collect::<Vec<_>>();
            if part.len() != 2 {
                return Err("Each component must contain = separating key from value".to_string());
            }

            let key = *part.first().expect("Length checked above; qed");
            let value = *part.get(1).expect("Length checked above; qed");

            match key {
                "path" => {
                    plot_directory.replace(
                        PathBuf::try_from(value).map_err(|error| {
                            format!("Failed to parse `path` \"{value}\": {error}")
                        })?,
                    );
                }
                "size" => {
                    allocated_plotting_space.replace(
                        value
                            .parse::<ByteSize>()
                            .map_err(|error| {
                                format!("Failed to parse `size` \"{value}\": {error}")
                            })?
                            .as_u64(),
                    );
                }
                key => {
                    return Err(format!(
                        "Key \"{key}\" is not supported, only `path` or `size`"
                    ));
                }
            }
        }

        Ok(DiskFarm {
            directory: plot_directory.ok_or({
                "`path` key is required with path to directory where plots will be stored"
            })?,
            allocated_plotting_space: allocated_plotting_space.ok_or({
                "`size` key is required with path to directory where plots will be stored"
            })?,
        })
    }
}

#[derive(Debug, Parser)]
#[clap(about, version)]
enum Command {
    /// Start a farmer, does plotting and farming
    Farm(FarmingArgs),
    /// Print information about farm and its content
    Info {
        /// One or more farm located at specified path.
        ///
        /// Example:
        ///   /path/to/directory
        disk_farms: Vec<PathBuf>,
    },
    /// Checks the farm for corruption and repairs errors (caused by disk errors or something else)
    Scrub {
        /// One or more farm located at specified path.
        ///
        /// Example:
        ///   /path/to/directory
        disk_farms: Vec<PathBuf>,
    },
    /// Wipes the farm
    Wipe {
        /// One or more farm located at specified path.
        ///
        /// Example:
        ///   /path/to/directory
        disk_farms: Vec<PathBuf>,
    },
    /// Runs benchmarks of the farmer's components.
    Bench {
        /// Benchmark to run.
        #[command(subcommand)]
        subcommand: BenchCommand,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                // TODO: Workaround for https://github.com/tokio-rs/tracing/issues/2214
                .with_ansi(supports_color::on(supports_color::Stream::Stderr).is_some())
                .with_filter(
                    EnvFilter::builder()
                        .with_default_directive(LevelFilter::INFO.into())
                        .from_env_lossy(),
                ),
        )
        .init();
    utils::raise_fd_limit();

    let command = Command::parse();

    match command {
        Command::Wipe { disk_farms } => {
            for disk_farm in &disk_farms {
                if !disk_farm.exists() {
                    panic!("Directory {} doesn't exist", disk_farm.display());
                }
            }

            for disk_farm in &disk_farms {
                // TODO: Delete this section once we don't have shared data anymore
                info!("Wiping shared data");
                let _ = fs::remove_file(disk_farm.join("known_addresses_db"));
                let _ = fs::remove_file(disk_farm.join("known_addresses.bin"));
                let _ = fs::remove_file(disk_farm.join("piece_cache_db"));
                let _ = fs::remove_file(disk_farm.join("providers_db"));

                SingleDiskFarm::wipe(disk_farm)?;
            }

            info!("Done");
        }
        Command::Farm(farming_args) => {
            commands::farm::<PosTable>(farming_args).await?;
        }
        Command::Info { disk_farms } => {
            commands::info(disk_farms);
        }
        Command::Scrub { disk_farms } => {
            commands::scrub(&disk_farms);
        }
        Command::Bench { subcommand } => match subcommand {
            BenchCommand::Audit(benchmark_args) => {
                commands::bench::audit(benchmark_args).unwrap();
            }
            BenchCommand::Proving(benchmark_args) => {
                commands::bench::proving(benchmark_args).unwrap();
            }
        },
    }
    Ok(())
}
