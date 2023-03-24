use codec::{Codec, Encode};
use domain_runtime_primitives::DomainCoreApi;
use sc_executor::RuntimeVersionOf;
use sp_api::{ApiError, Core, Hasher, ProvideRuntimeApi, RuntimeVersion};
use sp_blockchain::HeaderBackend;
use sp_core::traits::{CallContext, CodeExecutor, FetchRuntimeCode, RuntimeCode};
use sp_core::ExecutionContext;
use sp_domains::SignedOpaqueBundle;
use sp_runtime::traits::{Block as BlockT, NumberFor};
use sp_state_machine::BasicExternalities;
use std::borrow::Cow;
use std::marker::PhantomData;
use std::sync::Arc;
use system_runtime_primitives::SystemDomainApi;

type ExtractSignerResult<Block, AccountId> = Vec<(Option<AccountId>, <Block as BlockT>::Extrinsic)>;

/// Trait abstracting the runtime calls to process a domain block.
trait DomainBlockProcessorRuntimeApi<Block, PBlock, AccountId>
where
    Block: BlockT,
    PBlock: BlockT,
{
    fn construct_submit_core_bundle_extrinsics(
        &self,
        at: Block::Hash,
        signed_opaque_bundles: Vec<
            SignedOpaqueBundle<NumberFor<PBlock>, PBlock::Hash, Block::Hash>,
        >,
    ) -> Result<Vec<Vec<u8>>, ApiError>;

    fn extract_signer(
        &self,
        at: Block::Hash,
        extrinsics: Vec<<Block as BlockT>::Extrinsic>,
    ) -> Result<ExtractSignerResult<Block, AccountId>, ApiError>;
}

/// Wrapper around client that provides the access to runtime api.
pub struct DomainBlockProcessorRuntimeApiFull<Client, Block, PBlock, AccountId> {
    client: Arc<Client>,
    _data: PhantomData<(Block, PBlock, AccountId)>,
}

impl<Client, Block, PBlock, AccountId>
    DomainBlockProcessorRuntimeApiFull<Client, Block, PBlock, AccountId>
{
    pub fn new(client: Arc<Client>) -> Self {
        Self {
            client,
            _data: Default::default(),
        }
    }
}

impl<Client, Block, PBlock, AccountId> DomainBlockProcessorRuntimeApi<Block, PBlock, AccountId>
    for DomainBlockProcessorRuntimeApiFull<Client, Block, PBlock, AccountId>
where
    Block: BlockT,
    PBlock: BlockT,
    AccountId: Codec,
    Client: HeaderBackend<Block> + ProvideRuntimeApi<Block>,
    Client::Api:
        DomainCoreApi<Block, AccountId> + SystemDomainApi<Block, NumberFor<PBlock>, PBlock::Hash>,
{
    fn construct_submit_core_bundle_extrinsics(
        &self,
        at: Block::Hash,
        signed_opaque_bundles: Vec<
            SignedOpaqueBundle<NumberFor<PBlock>, PBlock::Hash, Block::Hash>,
        >,
    ) -> Result<Vec<Vec<u8>>, ApiError>
    where
        PBlock: BlockT,
    {
        self.client
            .runtime_api()
            .construct_submit_core_bundle_extrinsics(at, signed_opaque_bundles)
    }

    fn extract_signer(
        &self,
        at: Block::Hash,
        extrinsics: Vec<<Block as BlockT>::Extrinsic>,
    ) -> Result<Vec<(Option<AccountId>, <Block as BlockT>::Extrinsic)>, ApiError>
where {
        self.client.runtime_api().extract_signer(at, extrinsics)
    }
}

struct DomainBlockProcessorRuntimeApiStateless<Executor, Block, PBlock, AccountId> {
    executor: Arc<Executor>,
    runtime_code: Cow<'static, [u8]>,
    _data: PhantomData<(Block, PBlock, AccountId)>,
}

impl<Executor, Block, PBlock, AccountId> FetchRuntimeCode
    for DomainBlockProcessorRuntimeApiStateless<Executor, Block, PBlock, AccountId>
{
    fn fetch_runtime_code(&self) -> Option<Cow<[u8]>> {
        Some(self.runtime_code.clone())
    }
}

impl<Executor, Block, PBlock, AccountId>
    DomainBlockProcessorRuntimeApiStateless<Executor, Block, PBlock, AccountId>
where
    Block: BlockT,
    Executor: CodeExecutor + RuntimeVersionOf,
{
    // TODO: remove this once this call is used.
    #[allow(dead_code)]
    pub fn new(executor: Arc<Executor>, runtime_code: Cow<'static, [u8]>) -> Self {
        Self {
            executor,
            runtime_code,
            _data: Default::default(),
        }
    }

    fn runtime_code(&self) -> RuntimeCode {
        let code_hash = sp_core::Blake2Hasher::hash(&self.runtime_code);
        RuntimeCode {
            code_fetcher: self,
            heap_pages: None,
            hash: code_hash.encode(),
        }
    }

    fn runtime_version(&self) -> Result<RuntimeVersion, ApiError> {
        let mut ext = BasicExternalities::new_empty();
        self.executor
            .runtime_version(&mut ext, &self.runtime_code())
            .map_err(|err| ApiError::Application(Box::new(err)))
    }

    fn dispatch_call(
        &self,
        fn_name: &dyn Fn(RuntimeVersion) -> &'static str,
        input: Vec<u8>,
    ) -> Result<Vec<u8>, ApiError> {
        let runtime_version = self.runtime_version()?;
        let fn_name = fn_name(runtime_version);
        let mut ext = BasicExternalities::new_empty();
        self.executor
            .call(
                &mut ext,
                &self.runtime_code(),
                fn_name,
                &input,
                false,
                CallContext::Offchain,
            )
            .0
            .map_err(|err| {
                ApiError::Application(format!("Failed to invoke call to {fn_name}: {err}").into())
            })
    }
}

impl<Executor, Block, PBlock, AccountId> Core<Block>
    for DomainBlockProcessorRuntimeApiStateless<Executor, Block, PBlock, AccountId>
where
    Block: BlockT,
    PBlock: BlockT,
    AccountId: Send + Sync + 'static,
    Executor: CodeExecutor + RuntimeVersionOf,
{
    fn __runtime_api_internal_call_api_at(
        &self,
        _at: <Block as BlockT>::Hash,
        _context: ExecutionContext,
        params: Vec<u8>,
        fn_name: &dyn Fn(RuntimeVersion) -> &'static str,
    ) -> Result<Vec<u8>, ApiError> {
        self.dispatch_call(fn_name, params)
    }
}

impl<Executor, Block, PBlock, AccountId> DomainCoreApi<Block, AccountId>
    for DomainBlockProcessorRuntimeApiStateless<Executor, Block, PBlock, AccountId>
where
    Block: BlockT,
    PBlock: BlockT,
    AccountId: Codec + Send + Sync + 'static,
    Executor: CodeExecutor + RuntimeVersionOf,
{
    fn __runtime_api_internal_call_api_at(
        &self,
        _at: <Block as BlockT>::Hash,
        _context: ExecutionContext,
        params: Vec<u8>,
        fn_name: &dyn Fn(RuntimeVersion) -> &'static str,
    ) -> Result<Vec<u8>, ApiError> {
        self.dispatch_call(fn_name, params)
    }
}

impl<Executor, Block, PBlock, AccountId> SystemDomainApi<Block, NumberFor<PBlock>, PBlock::Hash>
    for DomainBlockProcessorRuntimeApiStateless<Executor, Block, PBlock, AccountId>
where
    Block: BlockT,
    PBlock: BlockT,
    AccountId: Codec + Send + Sync + 'static,
    Executor: CodeExecutor + RuntimeVersionOf,
{
    fn __runtime_api_internal_call_api_at(
        &self,
        _at: <Block as BlockT>::Hash,
        _context: ExecutionContext,
        params: Vec<u8>,
        fn_name: &dyn Fn(RuntimeVersion) -> &'static str,
    ) -> Result<Vec<u8>, ApiError> {
        self.dispatch_call(fn_name, params)
    }
}

impl<Executor, Block, PBlock, AccountId> DomainBlockProcessorRuntimeApi<Block, PBlock, AccountId>
    for DomainBlockProcessorRuntimeApiStateless<Executor, Block, PBlock, AccountId>
where
    Block: BlockT,
    PBlock: BlockT,
    AccountId: Codec + Send + Sync + 'static,
    Executor: CodeExecutor + RuntimeVersionOf,
{
    fn construct_submit_core_bundle_extrinsics(
        &self,
        at: Block::Hash,
        signed_opaque_bundles: Vec<
            SignedOpaqueBundle<NumberFor<PBlock>, PBlock::Hash, Block::Hash>,
        >,
    ) -> Result<Vec<Vec<u8>>, ApiError> {
        <Self as SystemDomainApi<Block, NumberFor<PBlock>, PBlock::Hash>>::construct_submit_core_bundle_extrinsics(self, at, signed_opaque_bundles)
    }

    fn extract_signer(
        &self,
        at: Block::Hash,
        extrinsics: Vec<<Block as BlockT>::Extrinsic>,
    ) -> Result<ExtractSignerResult<Block, AccountId>, ApiError> {
        <Self as DomainCoreApi<Block, AccountId>>::extract_signer(self, at, extrinsics)
    }
}
