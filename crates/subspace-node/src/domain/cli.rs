// Copyright (C) 2021 Subspace Labs, Inc.
// SPDX-License-Identifier: GPL-3.0-or-later

// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

use crate::domain::evm_chain_spec;
use clap::Parser;
use domain_runtime_primitives::opaque::Block as DomainBlock;
use domain_service::DomainConfiguration;
use sc_cli::{
    BlockNumberOrHash, ChainSpec, CliConfiguration, DefaultConfigurationValues, ImportParams,
    KeystoreParams, NetworkParams, Result, Role, RunCmd as SubstrateRunCmd, RuntimeVersion,
    SharedParams, SubstrateCli,
};
use sc_client_api::backend::AuxStore;
use sc_service::config::PrometheusConfig;
use sc_service::BasePath;
use sp_blockchain::HeaderBackend;
use sp_core::crypto::AccountId32;
use sp_domains::DomainId;
use sp_runtime::generic::BlockId;
use sp_runtime::traits::Convert;
use std::net::SocketAddr;
use std::num::ParseIntError;
use std::path::PathBuf;
use std::str::FromStr;
use subspace_runtime::Block;

/// Sub-commands supported by the executor.
#[derive(Debug, clap::Subcommand)]
#[allow(clippy::large_enum_variant)]
pub enum Subcommand {
    /// Export the state of a given block into a chain spec.
    ExportState(sc_cli::ExportStateCmd),

    /// Revert the chain to a previous state.
    Revert(sc_cli::RevertCmd),

    /// Sub-commands concerned with benchmarking.
    #[clap(subcommand)]
    Benchmark(frame_benchmarking_cli::BenchmarkCmd),

    /// The `export-execution-receipt` command used to get the ER from the auxiliary storage of the operator client
    ExportExecutionReceipt(ExportExecutionReceiptCmd),
}

/// The `export-execution-receipt` command used to get the ER from the auxiliary storage of the operator client
#[derive(Debug, Clone, Parser)]
pub struct ExportExecutionReceiptCmd {
    /// Get the `ExecutionReceipt` by domain block number or hash
    #[arg(long, conflicts_with_all = &["consensus_block_hash"])]
    pub domain_block: Option<BlockNumberOrHash>,

    /// Get the `ExecutionReceipt` by consensus block hash
    #[arg(long, conflicts_with_all = &["domain_block"])]
    pub consensus_block_hash: Option<BlockNumberOrHash>,

    /// The base struct of the export-execution-receipt command.
    #[clap(flatten)]
    pub shared_params: SharedParams,

    /// Domain arguments
    ///
    /// The command-line arguments provided first will be passed to the embedded consensus node,
    /// while the arguments provided after `--` will be passed to the domain node.
    ///
    /// subspace-node export-execution-receipt [consensus-chain-args] -- [domain-args]
    #[arg(raw = true)]
    pub domain_args: Vec<String>,
}

impl CliConfiguration for ExportExecutionReceiptCmd {
    fn shared_params(&self) -> &SharedParams {
        &self.shared_params
    }
}

impl ExportExecutionReceiptCmd {
    /// Run the export-execution-receipt command
    pub fn run<Backend, Client>(
        &self,
        domain_client: &Client,
        domain_backend: &Backend,
    ) -> sc_cli::Result<()>
    where
        Backend: AuxStore,
        Client: HeaderBackend<DomainBlock>,
    {
        // Get ER by consensus block
        if let Some(consensus_block_hash) = &self.consensus_block_hash {
            let consensus_block_hash = match consensus_block_hash.parse::<Block>()? {
                BlockId::Hash(h) => h,
                BlockId::Number(_) => {
                    eprintln!(
                        "unexpected input {consensus_block_hash:?}, expected consensus block hash",
                    );
                    return Ok(());
                }
            };
            match domain_client_operator::load_execution_receipt::<Backend, DomainBlock, Block>(
                domain_backend,
                consensus_block_hash,
            )? {
                Some(er) => {
                    println!(
                        "ExecutionReceipt of consensus block {consensus_block_hash:?}:\n{er:?}",
                    );
                }
                None => {
                    println!(
                        "ExecutionReceipt of consensus block {consensus_block_hash:?} not found",
                    );
                }
            }
            return Ok(());
        }

        // Get ER by domain block
        if let Some(domain_block) = &self.domain_block {
            let domain_block_hash = match domain_block.parse::<DomainBlock>()? {
                BlockId::Hash(h) => h,
                BlockId::Number(number) => domain_client.hash(number)?.ok_or_else(|| {
                    sp_blockchain::Error::Backend(format!(
                        "Domain block hash for #{number:?} not found",
                    ))
                })?,
            };
            match domain_client_operator::load_execution_receipt_by_domain_hash::<
                Backend,
                DomainBlock,
                Block,
            >(domain_backend, domain_block_hash)?
            {
                Some(er) => {
                    println!("ExecutionReceipt for domain block {domain_block:?}:\n{er:?}",);
                }
                None => {
                    println!("ExecutionReceipt for domain block {domain_block:?} not found",);
                }
            }
            return Ok(());
        }
        eprintln!("Expect the domain-block or consensus-block-hash argument",);
        Ok(())
    }
}

fn parse_domain_id(s: &str) -> std::result::Result<DomainId, ParseIntError> {
    s.parse::<u32>().map(Into::into)
}

#[derive(Debug, Parser)]
pub struct DomainCli {
    /// Run a domain node.
    #[clap(flatten)]
    pub run: SubstrateRunCmd,

    #[clap(long, value_parser = parse_domain_id)]
    pub domain_id: DomainId,

    /// Optional relayer address to relay messages on behalf.
    #[clap(long)]
    pub relayer_id: Option<String>,

    /// Run the node as an Operator
    #[arg(long, conflicts_with = "validator")]
    pub operator: bool,

    /// Additional args for domain.
    #[clap(raw = true)]
    additional_args: Vec<String>,
}

impl DomainCli {
    /// Constructs a new instance of [`DomainCli`].
    pub fn new(
        consensus_base_path: Option<PathBuf>,
        domain_args: impl Iterator<Item = String>,
    ) -> Self {
        let mut cli =
            DomainCli::parse_from([Self::executable_name()].into_iter().chain(domain_args));

        // Use `consensus_base_path/domain-{domain_id}` as the domain base path if it's not
        // specified explicitly but there is an explicit consensus base path.
        match consensus_base_path {
            Some(c_path) if cli.run.shared_params.base_path.is_none() => {
                cli.run
                    .shared_params
                    .base_path
                    .replace(c_path.join(format!("domain-{}", u32::from(cli.domain_id))));
            }
            _ => {}
        }

        cli
    }

    pub fn additional_args(&self) -> impl Iterator<Item = String> {
        [Self::executable_name()]
            .into_iter()
            .chain(self.additional_args.clone())
    }

    pub fn maybe_relayer_id<AccountId, CA>(&self) -> sc_cli::Result<Option<AccountId>>
    where
        CA: Convert<AccountId32, AccountId>,
        AccountId: FromStr,
    {
        // if is dev, use the known key ring to start relayer
        let res = if self.shared_params().is_dev() && self.relayer_id.is_none() {
            self.run
                .get_keyring()
                .map(|kr| CA::convert(kr.to_account_id()))
        } else if let Some(relayer_id) = self.relayer_id.clone() {
            Some(AccountId::from_str(&relayer_id).map_err(|_err| {
                sc_cli::Error::Input(format!("Invalid Relayer Id: {relayer_id}"))
            })?)
        } else {
            None
        };
        Ok(res)
    }

    /// Creates domain configuration from domain cli.
    pub fn create_domain_configuration<AccountId, CA>(
        &self,
        tokio_handle: tokio::runtime::Handle,
    ) -> sc_cli::Result<DomainConfiguration<AccountId>>
    where
        CA: Convert<AccountId32, AccountId>,
        AccountId: FromStr,
    {
        // if is dev, use the known key ring to start relayer
        let maybe_relayer_id = if self.shared_params().is_dev() && self.relayer_id.is_none() {
            self.run
                .get_keyring()
                .map(|kr| CA::convert(kr.to_account_id()))
        } else if let Some(relayer_id) = self.relayer_id.clone() {
            Some(AccountId::from_str(&relayer_id).map_err(|_err| {
                sc_cli::Error::Input(format!("Invalid Relayer Id: {relayer_id}"))
            })?)
        } else {
            None
        };

        let service_config = SubstrateCli::create_configuration(self, self, tokio_handle)?;
        Ok(DomainConfiguration {
            service_config,
            maybe_relayer_id,
        })
    }
}

impl SubstrateCli for DomainCli {
    fn impl_name() -> String {
        "Subspace Domain".into()
    }

    fn impl_version() -> String {
        env!("SUBSTRATE_CLI_IMPL_VERSION").into()
    }

    fn executable_name() -> String {
        // Customize to make sure directory used for data by default is the same regardless of the
        // name of the executable file.
        "subspace-node".to_string()
    }

    fn description() -> String {
        "Subspace Domain".into()
    }

    fn author() -> String {
        env!("CARGO_PKG_AUTHORS").into()
    }

    fn support_url() -> String {
        "https://github.com/subspace/subspace/issues/new".into()
    }

    fn copyright_start_year() -> i32 {
        2022
    }

    fn load_spec(&self, id: &str) -> std::result::Result<Box<dyn ChainSpec>, String> {
        // TODO: Fetch the runtime name of `self.domain_id` properly.
        let runtime_name = "evm";
        match runtime_name {
            "evm" => evm_chain_spec::load_chain_spec(id),
            unknown_name => Err(format!("Unknown runtime: {unknown_name}")),
        }
    }

    fn native_runtime_version(_chain_spec: &Box<dyn ChainSpec>) -> &'static RuntimeVersion {
        // TODO: Fetch the runtime name of `self.domain_id` properly.
        let runtime_name = "evm";
        match runtime_name {
            "evm" => &evm_domain_runtime::VERSION,
            unknown_name => unreachable!("Unknown runtime: {unknown_name}"),
        }
    }
}

impl DefaultConfigurationValues for DomainCli {
    fn p2p_listen_port() -> u16 {
        30334
    }

    fn rpc_listen_port() -> u16 {
        9945
    }

    fn prometheus_listen_port() -> u16 {
        9616
    }
}

impl CliConfiguration<Self> for DomainCli {
    fn shared_params(&self) -> &SharedParams {
        self.run.shared_params()
    }

    fn import_params(&self) -> Option<&ImportParams> {
        self.run.import_params()
    }

    fn network_params(&self) -> Option<&NetworkParams> {
        self.run.network_params()
    }

    fn keystore_params(&self) -> Option<&KeystoreParams> {
        self.run.keystore_params()
    }

    fn base_path(&self) -> Result<Option<BasePath>> {
        self.shared_params().base_path()
    }

    fn rpc_addr(&self, default_listen_port: u16) -> Result<Option<SocketAddr>> {
        self.run.rpc_addr(default_listen_port)
    }

    fn prometheus_config(
        &self,
        default_listen_port: u16,
        chain_spec: &Box<dyn ChainSpec>,
    ) -> Result<Option<PrometheusConfig>> {
        self.run.prometheus_config(default_listen_port, chain_spec)
    }

    fn chain_id(&self, is_dev: bool) -> Result<String> {
        self.run.chain_id(is_dev)
    }

    fn role(&self, is_dev: bool) -> Result<sc_service::Role> {
        // is authority when operator is enabled or in dev mode
        let is_authority = self.operator || self.run.validator || is_dev;

        Ok(if is_authority {
            Role::Authority
        } else {
            Role::Full
        })
    }

    fn transaction_pool(&self, is_dev: bool) -> Result<sc_service::config::TransactionPoolOptions> {
        self.run.transaction_pool(is_dev)
    }

    fn trie_cache_maximum_size(&self) -> Result<Option<usize>> {
        self.run.trie_cache_maximum_size()
    }

    fn rpc_methods(&self) -> Result<sc_service::config::RpcMethods> {
        self.run.rpc_methods()
    }

    fn rpc_max_connections(&self) -> Result<u32> {
        self.run.rpc_max_connections()
    }

    fn rpc_cors(&self, is_dev: bool) -> Result<Option<Vec<String>>> {
        self.run.rpc_cors(is_dev)
    }

    fn default_heap_pages(&self) -> Result<Option<u64>> {
        self.run.default_heap_pages()
    }

    fn force_authoring(&self) -> Result<bool> {
        self.run.force_authoring()
    }

    fn disable_grandpa(&self) -> Result<bool> {
        self.run.disable_grandpa()
    }

    fn max_runtime_instances(&self) -> Result<Option<usize>> {
        self.run.max_runtime_instances()
    }

    fn announce_block(&self) -> Result<bool> {
        self.run.announce_block()
    }

    fn dev_key_seed(&self, is_dev: bool) -> Result<Option<String>> {
        self.run.dev_key_seed(is_dev)
    }

    fn telemetry_endpoints(
        &self,
        chain_spec: &Box<dyn ChainSpec>,
    ) -> Result<Option<sc_telemetry::TelemetryEndpoints>> {
        self.run.telemetry_endpoints(chain_spec)
    }
}
