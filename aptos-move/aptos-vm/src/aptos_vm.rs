// Copyright © Aptos Foundation
// Parts of the project are originally copyright © Meta Platforms, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::{
    block_executor::{AptosTransactionOutput, BlockAptosVM},
    counters::*,
    data_cache::{AsMoveResolver, StorageAdapter},
    errors::{discarded_output, expect_only_successful_execution},
    gas::{check_gas, get_gas_parameters},
    keyless_validation,
    move_vm_ext::{
        get_max_binary_format_version, get_max_identifier_size,
        session::user_transaction_sessions::{
            abort_hook::AbortHookSession, epilogue::EpilogueSession, prologue::PrologueSession,
            user::UserSession,
        },
        AptosMoveResolver, MoveVmExt, SessionExt, SessionId,
    },
    sharded_block_executor::{executor_client::ExecutorClient, ShardedBlockExecutor},
    system_module_names::*,
    transaction_metadata::TransactionMetadata,
    transaction_validation, verifier,
    verifier::randomness::has_randomness_attribute,
    VMExecutor, VMValidator,
};
use anyhow::anyhow;
use aptos_block_executor::txn_commit_hook::NoOpTransactionCommitHook;
use aptos_crypto::HashValue;
use aptos_framework::{
    natives::{code::PublishRequest, randomness::RandomnessContext},
    RuntimeModuleMetadataV1,
};
use aptos_gas_algebra::{Gas, GasQuantity, NumBytes, Octa};
use aptos_gas_meter::{AptosGasMeter, GasAlgebra, StandardGasAlgebra, StandardGasMeter};
use aptos_gas_schedule::{AptosGasParameters, TransactionGasParameters, VMGasParameters};
use aptos_logger::{enabled, prelude::*, Level};
use aptos_memory_usage_tracker::MemoryTrackedGasMeter;
use aptos_metrics_core::TimerHelper;
#[cfg(any(test, feature = "testing"))]
use aptos_types::state_store::StateViewId;
use aptos_types::{
    account_config,
    account_config::{new_block_event_key, AccountResource},
    block_executor::{
        config::{BlockExecutorConfig, BlockExecutorConfigFromOnchain, BlockExecutorLocalConfig},
        partitioner::PartitionedTransactions,
    },
    block_metadata::BlockMetadata,
    block_metadata_ext::{BlockMetadataExt, BlockMetadataWithRandomness},
    chain_id::ChainId,
    fee_statement::FeeStatement,
    move_utils::as_move_value::AsMoveValue,
    on_chain_config::{
        new_epoch_event_key, ConfigurationResource, FeatureFlag, Features, OnChainConfig,
        OnChainConsensusConfig, OnChainRandomnessConfig, RandomnessConfigMoveStruct,
        TimedFeatureOverride, TimedFeatures, TimedFeaturesBuilder,
    },
    randomness::Randomness,
    state_store::{StateView, TStateView},
    transaction::{
        authenticator::AnySignature, signature_verified_transaction::SignatureVerifiedTransaction,
        BlockOutput, EntryFunction, ExecutionError, ExecutionStatus, ModuleBundle, Multisig,
        MultisigTransactionPayload, Script, SignatureCheckedTransaction, SignedTransaction,
        Transaction, TransactionAuxiliaryData, TransactionOutput, TransactionPayload,
        TransactionStatus, VMValidatorResult, ViewFunctionOutput, WriteSetPayload,
    },
    vm_status::{AbortLocation, StatusCode, VMStatus},
};
use aptos_utils::{aptos_try, return_on_failure};
use aptos_vm_logging::{log_schema::AdapterLogSchema, speculative_error, speculative_log};
use aptos_vm_types::{
    abstract_write_op::AbstractResourceWriteOp,
    change_set::VMChangeSet,
    output::VMOutput,
    resolver::{ExecutorView, ResourceGroupView},
    storage::{change_set_configs::ChangeSetConfigs, StorageGasParameters},
};
use claims::assert_err;
use fail::fail_point;
use move_binary_format::{
    access::ModuleAccess,
    compatibility::Compatibility,
    deserializer::DeserializerConfig,
    errors::{Location, PartialVMError, PartialVMResult, VMError, VMResult},
    CompiledModule,
};
use move_core_types::{
    account_address::AccountAddress,
    ident_str,
    identifier::Identifier,
    language_storage::{ModuleId, TypeTag},
    move_resource::MoveStructType,
    transaction_argument::convert_txn_args,
    value::{serialize_values, MoveValue},
    vm_status::StatusType,
};
use move_vm_runtime::{
    logging::expect_no_verification_errors,
    module_traversal::{TraversalContext, TraversalStorage},
};
use move_vm_types::gas::{GasMeter, UnmeteredGasMeter};
use num_cpus;
use once_cell::sync::{Lazy, OnceCell};
use std::{
    cmp::{max, min},
    collections::{BTreeMap, BTreeSet},
    marker::Sync,
    sync::Arc,
};

static EXECUTION_CONCURRENCY_LEVEL: OnceCell<usize> = OnceCell::new();
static NUM_EXECUTION_SHARD: OnceCell<usize> = OnceCell::new();
static NUM_PROOF_READING_THREADS: OnceCell<usize> = OnceCell::new();
static PARANOID_TYPE_CHECKS: OnceCell<bool> = OnceCell::new();
static DISCARD_FAILED_BLOCKS: OnceCell<bool> = OnceCell::new();
static PROCESSED_TRANSACTIONS_DETAILED_COUNTERS: OnceCell<bool> = OnceCell::new();
static TIMED_FEATURE_OVERRIDE: OnceCell<TimedFeatureOverride> = OnceCell::new();

// TODO: Don't expose this in AptosVM, and use only in BlockAptosVM!
pub static RAYON_EXEC_POOL: Lazy<Arc<rayon::ThreadPool>> = Lazy::new(|| {
    Arc::new(
        rayon::ThreadPoolBuilder::new()
            .num_threads(num_cpus::get())
            .thread_name(|index| format!("par_exec-{}", index))
            .build()
            .unwrap(),
    )
});

macro_rules! deprecated_module_bundle {
    () => {
        VMStatus::error(
            StatusCode::FEATURE_UNDER_GATING,
            Some("Module bundle payload has been removed".to_string()),
        )
    };
}

macro_rules! unwrap_or_discard {
    ($res:expr) => {
        match $res {
            Ok(s) => s,
            Err(e) => {
                // covers both VMStatus itself and VMError which can convert to VMStatus
                let s: VMStatus = e.into();

                let o = discarded_output(s.status_code());
                return (s, o);
            },
        }
    };
}

pub(crate) fn get_system_transaction_output(
    session: SessionExt,
    fee_statement: FeeStatement,
    status: ExecutionStatus,
    change_set_configs: &ChangeSetConfigs,
) -> Result<VMOutput, VMStatus> {
    get_transaction_output(
        session,
        fee_statement,
        status,
        change_set_configs,
        TransactionAuxiliaryData::default(),
    )
}

pub(crate) fn get_transaction_output(
    session: SessionExt,
    fee_statement: FeeStatement,
    status: ExecutionStatus,
    change_set_configs: &ChangeSetConfigs,
    auxiliary_data: TransactionAuxiliaryData,
) -> Result<VMOutput, VMStatus> {
    let change_set = session.finish(change_set_configs)?;
    Ok(VMOutput::new(
        change_set,
        fee_statement,
        TransactionStatus::Keep(status),
        auxiliary_data,
    ))
}

pub(crate) fn get_or_vm_startup_failure<'a, T>(
    gas_params: &'a Result<T, String>,
    log_context: &AdapterLogSchema,
) -> Result<&'a T, VMStatus> {
    gas_params.as_ref().map_err(|err| {
        let msg = format!("VM Startup Failed. {}", err);
        speculative_error!(log_context, msg.clone());
        VMStatus::error(StatusCode::VM_STARTUP_FAILURE, Some(msg))
    })
}

pub struct AptosVM {
    is_simulation: bool,
    move_vm: MoveVmExt,
    gas_feature_version: u64,
    gas_params: Result<AptosGasParameters, String>,
    pub(crate) storage_gas_params: Result<StorageGasParameters, String>,
    timed_features: TimedFeatures,
    randomness_enabled: bool,
}

impl AptosVM {
    pub fn new(
        resolver: &impl AptosMoveResolver,
        override_is_delayed_field_optimization_capable: Option<bool>,
    ) -> Self {
        let _timer = TIMER.timer_with(&["AptosVM::new"]);

        let features = Features::fetch_config(resolver).unwrap_or_default();
        let (
            gas_params,
            storage_gas_params,
            native_gas_params,
            misc_gas_params,
            gas_feature_version,
        ) = get_gas_parameters(&features, resolver);

        // If no chain ID is in storage, we assume we are in a testing environment and use ChainId::TESTING
        let chain_id = ChainId::fetch_config(resolver).unwrap_or_else(ChainId::test);

        let timestamp = ConfigurationResource::fetch_config(resolver)
            .map(|config| config.last_reconfiguration_time())
            .unwrap_or(0);

        let mut timed_features_builder = TimedFeaturesBuilder::new(chain_id, timestamp);
        if let Some(profile) = Self::get_timed_feature_override() {
            timed_features_builder = timed_features_builder.with_override_profile(profile)
        }
        let timed_features = timed_features_builder.build();

        // If aggregator execution is enabled, we need to tag aggregator_v2 types,
        // so they can be exchanged with identifiers during VM execution.
        let override_is_delayed_field_optimization_capable =
            override_is_delayed_field_optimization_capable
                .unwrap_or_else(|| resolver.is_delayed_field_optimization_capable());
        let aggregator_v2_type_tagging = override_is_delayed_field_optimization_capable
            && features.is_aggregator_v2_delayed_fields_enabled();

        let consensus_config = OnChainConsensusConfig::fetch_config(resolver).unwrap_or_default();
        let randomness_config = RandomnessConfigMoveStruct::fetch_config(resolver)
            .and_then(|x| OnChainRandomnessConfig::try_from(x).ok())
            .unwrap_or_else(OnChainRandomnessConfig::default_if_missing);
        let randomness_enabled =
            consensus_config.is_vtxn_enabled() && randomness_config.randomness_enabled();
        let move_vm = MoveVmExt::new(
            native_gas_params,
            misc_gas_params,
            gas_feature_version,
            chain_id.id(),
            features,
            timed_features.clone(),
            resolver,
            aggregator_v2_type_tagging,
        )
        .expect("should be able to create Move VM; check if there are duplicated natives");

        Self {
            is_simulation: false,
            move_vm,
            gas_feature_version,
            gas_params,
            storage_gas_params,
            timed_features,
            randomness_enabled,
        }
    }

    pub fn new_session<'r, S: AptosMoveResolver>(
        &self,
        resolver: &'r S,
        session_id: SessionId,
    ) -> SessionExt<'r, '_> {
        self.move_vm.new_session(resolver, session_id)
    }

    #[inline(always)]
    fn features(&self) -> &Features {
        self.move_vm.features()
    }

    /// Sets execution concurrency level when invoked the first time.
    pub fn set_concurrency_level_once(mut concurrency_level: usize) {
        concurrency_level = min(concurrency_level, num_cpus::get());
        // Only the first call succeeds, due to OnceCell semantics.
        EXECUTION_CONCURRENCY_LEVEL.set(concurrency_level).ok();
    }

    /// Get the concurrency level if already set, otherwise return default 1
    /// (sequential execution).
    ///
    /// The concurrency level is fixed to 1 if gas profiling is enabled.
    pub fn get_concurrency_level() -> usize {
        match EXECUTION_CONCURRENCY_LEVEL.get() {
            Some(concurrency_level) => *concurrency_level,
            None => 1,
        }
    }

    pub fn set_num_shards_once(mut num_shards: usize) {
        num_shards = max(num_shards, 1);
        // Only the first call succeeds, due to OnceCell semantics.
        NUM_EXECUTION_SHARD.set(num_shards).ok();
    }

    pub fn get_num_shards() -> usize {
        match NUM_EXECUTION_SHARD.get() {
            Some(num_shards) => *num_shards,
            None => 1,
        }
    }

    /// Sets runtime config when invoked the first time.
    pub fn set_paranoid_type_checks(enable: bool) {
        // Only the first call succeeds, due to OnceCell semantics.
        PARANOID_TYPE_CHECKS.set(enable).ok();
    }

    /// Get the paranoid type check flag if already set, otherwise return default true
    pub fn get_paranoid_checks() -> bool {
        match PARANOID_TYPE_CHECKS.get() {
            Some(enable) => *enable,
            None => true,
        }
    }

    /// Sets runtime config when invoked the first time.
    pub fn set_discard_failed_blocks(enable: bool) {
        // Only the first call succeeds, due to OnceCell semantics.
        DISCARD_FAILED_BLOCKS.set(enable).ok();
    }

    /// Get the discard failed blocks flag if already set, otherwise return default (false)
    pub fn get_discard_failed_blocks() -> bool {
        match DISCARD_FAILED_BLOCKS.get() {
            Some(enable) => *enable,
            None => false,
        }
    }

    // Set the override profile for timed features.
    pub fn set_timed_feature_override(profile: TimedFeatureOverride) {
        TIMED_FEATURE_OVERRIDE.set(profile).ok();
    }

    pub fn get_timed_feature_override() -> Option<TimedFeatureOverride> {
        TIMED_FEATURE_OVERRIDE.get().cloned()
    }

    /// Sets the # of async proof reading threads.
    pub fn set_num_proof_reading_threads_once(mut num_threads: usize) {
        // TODO(grao): Do more analysis to tune this magic number.
        num_threads = min(num_threads, 256);
        // Only the first call succeeds, due to OnceCell semantics.
        NUM_PROOF_READING_THREADS.set(num_threads).ok();
    }

    /// Returns the # of async proof reading threads if already set, otherwise return default value
    /// (32).
    pub fn get_num_proof_reading_threads() -> usize {
        match NUM_PROOF_READING_THREADS.get() {
            Some(num_threads) => *num_threads,
            None => 32,
        }
    }

    /// Sets additional details in counters when invoked the first time.
    pub fn set_processed_transactions_detailed_counters() {
        // Only the first call succeeds, due to OnceCell semantics.
        PROCESSED_TRANSACTIONS_DETAILED_COUNTERS.set(true).ok();
    }

    /// Get whether we should capture additional details in counters
    pub fn get_processed_transactions_detailed_counters() -> bool {
        match PROCESSED_TRANSACTIONS_DETAILED_COUNTERS.get() {
            Some(value) => *value,
            None => false,
        }
    }

    /// Returns the internal gas schedule if it has been loaded, or an error if it hasn't.
    #[cfg(any(test, feature = "testing"))]
    pub fn gas_params(&self) -> Result<&AptosGasParameters, VMStatus> {
        let log_context = AdapterLogSchema::new(StateViewId::Miscellaneous, 0);
        get_or_vm_startup_failure(&self.gas_params, &log_context)
    }

    pub fn as_move_resolver<'r, R: ExecutorView>(
        &self,
        executor_view: &'r R,
    ) -> StorageAdapter<'r, R> {
        StorageAdapter::new_with_config(
            executor_view,
            self.gas_feature_version,
            self.features(),
            None,
        )
    }

    pub fn as_move_resolver_with_group_view<'r, R: ExecutorView + ResourceGroupView>(
        &self,
        executor_view: &'r R,
    ) -> StorageAdapter<'r, R> {
        StorageAdapter::new_with_config(
            executor_view,
            self.gas_feature_version,
            self.features(),
            Some(executor_view),
        )
    }

    fn fee_statement_from_gas_meter(
        txn_data: &TransactionMetadata,
        gas_meter: &impl AptosGasMeter,
        storage_fee_refund: u64,
    ) -> FeeStatement {
        let gas_used = Self::gas_used(txn_data.max_gas_amount(), gas_meter);
        FeeStatement::new(
            gas_used,
            u64::from(gas_meter.execution_gas_used()),
            u64::from(gas_meter.io_gas_used()),
            u64::from(gas_meter.storage_fee_used()),
            storage_fee_refund,
        )
    }

    pub(crate) fn failed_transaction_cleanup(
        &self,
        prologue_change_set: VMChangeSet,
        error_vm_status: VMStatus,
        gas_meter: &mut impl AptosGasMeter,
        txn_data: &TransactionMetadata,
        resolver: &impl AptosMoveResolver,
        log_context: &AdapterLogSchema,
        change_set_configs: &ChangeSetConfigs,
    ) -> (VMStatus, VMOutput) {
        if self.gas_feature_version >= 12 {
            // Check if the gas meter's internal counters are consistent.
            //
            // Since we are already in the failure epilogue, there is not much we can do
            // other than logging the inconsistency.
            //
            // This is a tradeoff. We have to either
            //   1. Continue to calculate the gas cost based on the numbers we have.
            //   2. Discard the transaction.
            //
            // Option (2) does not work, since it would enable DoS attacks.
            // Option (1) is not ideal, but optimistically, it should allow the network
            // to continue functioning, less the transactions that run into this problem.
            if let Err(err) = gas_meter.algebra().check_consistency() {
                println!(
                    "[aptos-vm][gas-meter][failure-epilogue] {}",
                    err.message()
                        .unwrap_or("No message found -- this should not happen.")
                );
            }
        }
        let (txn_status, txn_aux_data) = TransactionStatus::from_vm_status(
            error_vm_status.clone(),
            self.features()
                .is_enabled(FeatureFlag::CHARGE_INVARIANT_VIOLATION),
            self.features(),
        );

        match txn_status {
            TransactionStatus::Keep(status) => {
                // The transaction should be kept. Run the appropriate post transaction workflows
                // including epilogue. This runs a new session that ignores any side effects that
                // might abort the execution (e.g., spending additional funds needed to pay for
                // gas). Even if the previous failure occurred while running the epilogue, it
                // should not fail now. If it somehow fails here, there is no choice but to
                // discard the transaction.
                let txn_output = match self.finish_aborted_transaction(
                    prologue_change_set,
                    gas_meter,
                    txn_data,
                    resolver,
                    status,
                    log_context,
                    change_set_configs,
                ) {
                    Ok((change_set, fee_statement, status)) => VMOutput::new(
                        change_set,
                        fee_statement,
                        TransactionStatus::Keep(status),
                        txn_aux_data,
                    ),
                    Err(err) => discarded_output(err.status_code()),
                };
                (error_vm_status, txn_output)
            },
            TransactionStatus::Discard(status_code) => {
                let discarded_output = discarded_output(status_code);
                (error_vm_status, discarded_output)
            },
            TransactionStatus::Retry => unreachable!(),
        }
    }

    fn inject_abort_info_if_available(&self, status: ExecutionStatus) -> ExecutionStatus {
        match status {
            ExecutionStatus::MoveAbort {
                location: AbortLocation::Module(module),
                code,
                ..
            } => {
                let info = self
                    .extract_module_metadata(&module)
                    .and_then(|m| m.extract_abort_info(code));
                ExecutionStatus::MoveAbort {
                    location: AbortLocation::Module(module),
                    code,
                    info,
                }
            },
            _ => status,
        }
    }

    fn finish_aborted_transaction(
        &self,
        prologue_change_set: VMChangeSet,
        gas_meter: &mut impl AptosGasMeter,
        txn_data: &TransactionMetadata,
        resolver: &impl AptosMoveResolver,
        status: ExecutionStatus,
        log_context: &AdapterLogSchema,
        change_set_configs: &ChangeSetConfigs,
    ) -> Result<(VMChangeSet, FeeStatement, ExecutionStatus), VMStatus> {
        // Storage refund is zero since no slots are deleted in aborted transactions.
        const ZERO_STORAGE_REFUND: u64 = 0;

        let is_account_init_for_sponsored_transaction =
            is_account_init_for_sponsored_transaction(txn_data, self.features(), resolver)?;

        if is_account_init_for_sponsored_transaction {
            let mut abort_hook_session =
                AbortHookSession::new(self, txn_data, resolver, prologue_change_set)?;
            // Abort information is injected using the user defined error in the Move contract.
            let status = self.inject_abort_info_if_available(status);

            abort_hook_session.execute(|session| {
                create_account_if_does_not_exist(session, gas_meter, txn_data.sender())
                    // if this fails, it is likely due to out of gas, so we try again without metering
                    // and then validate below that we charged sufficiently.
                    .or_else(|_err| {
                        create_account_if_does_not_exist(
                            session,
                            &mut UnmeteredGasMeter,
                            txn_data.sender(),
                        )
                    })
                    .map_err(expect_no_verification_errors)
                    .or_else(|err| {
                        expect_only_successful_execution(
                            err,
                            &format!("{:?}::{}", ACCOUNT_MODULE, CREATE_ACCOUNT_IF_DOES_NOT_EXIST),
                            log_context,
                        )
                    })
            })?;

            let mut change_set = abort_hook_session.finish(change_set_configs)?;
            if let Err(err) = self.charge_change_set(&mut change_set, gas_meter, txn_data, resolver)
            {
                info!(
                    *log_context,
                    "Failed during charge_change_set: {:?}. Most likely exceeded gas limited.", err,
                );
            };

            let fee_statement =
                AptosVM::fee_statement_from_gas_meter(txn_data, gas_meter, ZERO_STORAGE_REFUND);

            // Verify we charged sufficiently for creating an account slot
            let gas_params = get_or_vm_startup_failure(&self.gas_params, log_context)?;
            let gas_unit_price = u64::from(txn_data.gas_unit_price());
            let gas_used = fee_statement.gas_used();
            let storage_fee = fee_statement.storage_fee_used();
            let storage_refund = fee_statement.storage_fee_refund();

            let actual = gas_used * gas_unit_price + storage_fee - storage_refund;
            let expected = u64::from(
                gas_meter
                    .disk_space_pricing()
                    .hack_account_creation_fee_lower_bound(&gas_params.vm.txn),
            );
            if actual < expected {
                expect_only_successful_execution(
                    PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                        .with_message(
                            "Insufficient fee for storing account for sponsored transaction"
                                .to_string(),
                        )
                        .finish(Location::Undefined),
                    &format!("{:?}::{}", ACCOUNT_MODULE, CREATE_ACCOUNT_IF_DOES_NOT_EXIST),
                    log_context,
                )?;
            }

            let mut epilogue_session = EpilogueSession::new(
                self,
                txn_data,
                resolver,
                change_set,
                ZERO_STORAGE_REFUND.into(),
            )?;

            epilogue_session.execute(|session| {
                transaction_validation::run_failure_epilogue(
                    session,
                    gas_meter.balance(),
                    fee_statement,
                    self.features(),
                    txn_data,
                    log_context,
                )
            })?;
            epilogue_session
                .finish(change_set_configs)
                .map(|set| (set, fee_statement, status))
        } else {
            let mut epilogue_session = EpilogueSession::new(
                self,
                txn_data,
                resolver,
                prologue_change_set,
                ZERO_STORAGE_REFUND.into(),
            )?;

            let status = self.inject_abort_info_if_available(status);

            let fee_statement =
                AptosVM::fee_statement_from_gas_meter(txn_data, gas_meter, ZERO_STORAGE_REFUND);
            epilogue_session.execute(|session| {
                transaction_validation::run_failure_epilogue(
                    session,
                    gas_meter.balance(),
                    fee_statement,
                    self.features(),
                    txn_data,
                    log_context,
                )
            })?;
            epilogue_session
                .finish(change_set_configs)
                .map(|set| (set, fee_statement, status))
        }
    }

    fn success_transaction_cleanup(
        &self,
        mut epilogue_session: EpilogueSession,
        gas_meter: &impl AptosGasMeter,
        txn_data: &TransactionMetadata,
        log_context: &AdapterLogSchema,
        change_set_configs: &ChangeSetConfigs,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        if self.gas_feature_version >= 12 {
            // Check if the gas meter's internal counters are consistent.
            //
            // It's better to fail the transaction due to invariant violation than to allow
            // potentially bogus states to be committed.
            if let Err(err) = gas_meter.algebra().check_consistency() {
                println!(
                    "[aptos-vm][gas-meter][success-epilogue] {}",
                    err.message()
                        .unwrap_or("No message found -- this should not happen.")
                );
                return Err(err.finish(Location::Undefined).into());
            }
        }

        let fee_statement = AptosVM::fee_statement_from_gas_meter(
            txn_data,
            gas_meter,
            u64::from(epilogue_session.get_storage_fee_refund()),
        );
        epilogue_session.execute(|session| {
            transaction_validation::run_success_epilogue(
                session,
                gas_meter.balance(),
                fee_statement,
                self.features(),
                txn_data,
                log_context,
            )
        })?;
        let change_set = epilogue_session.finish(change_set_configs)?;
        let output = VMOutput::new(
            change_set,
            fee_statement,
            TransactionStatus::Keep(ExecutionStatus::Success),
            TransactionAuxiliaryData::default(),
        );

        Ok((VMStatus::Executed, output))
    }

    fn validate_and_execute_script(
        &self,
        session: &mut SessionExt,
        // Note: cannot use AptosGasMeter because it is not implemented for
        //       UnmeteredGasMeter.
        gas_meter: &mut impl GasMeter,
        traversal_context: &mut TraversalContext,
        senders: Vec<AccountAddress>,
        script: &Script,
    ) -> Result<(), VMStatus> {
        // Note: Feature gating is needed here because the traversal of the dependencies could
        //       result in shallow-loading of the modules and therefore subtle changes in
        //       the error semantics.
        if self.gas_feature_version >= 15 {
            session.check_script_dependencies_and_check_gas(
                gas_meter,
                traversal_context,
                script.code(),
            )?;
        }

        let loaded_func = session.load_script(script.code(), script.ty_args().to_vec())?;

        // TODO(Gerardo): consolidate the extended validation to verifier.
        verifier::event_validation::verify_no_event_emission_in_script(
            script.code(),
            &session.get_vm_config().deserializer_config,
        )?;

        let args = verifier::transaction_arg_validation::validate_combine_signer_and_txn_args(
            session,
            senders,
            convert_txn_args(script.args()),
            &loaded_func,
            self.features().is_enabled(FeatureFlag::STRUCT_CONSTRUCTORS),
        )?;

        session.execute_script(script.code(), script.ty_args().to_vec(), args, gas_meter)?;
        Ok(())
    }

    fn validate_and_execute_entry_function(
        &self,
        session: &mut SessionExt,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext,
        senders: Vec<AccountAddress>,
        entry_fn: &EntryFunction,
        txn_data: &TransactionMetadata,
    ) -> Result<(), VMStatus> {
        // Note: Feature gating is needed here because the traversal of the dependencies could
        //       result in shallow-loading of the modules and therefore subtle changes in
        //       the error semantics.
        if self.gas_feature_version >= 15 {
            let module_id = traversal_context
                .referenced_module_ids
                .alloc(entry_fn.module().clone());
            session.check_dependencies_and_charge_gas(gas_meter, traversal_context, [(
                module_id.address(),
                module_id.name(),
            )])?;
        }

        let (function, is_friend_or_private) = session.load_function_and_is_friend_or_private_def(
            entry_fn.module(),
            entry_fn.function(),
            entry_fn.ty_args(),
        )?;

        if is_friend_or_private && txn_data.required_deposit.is_some() {
            let txn_context = session
                .get_native_extensions()
                .get_mut::<RandomnessContext>();
            txn_context.mark_unbiasable();
        }

        let struct_constructors_enabled =
            self.features().is_enabled(FeatureFlag::STRUCT_CONSTRUCTORS);
        let args = verifier::transaction_arg_validation::validate_combine_signer_and_txn_args(
            session,
            senders,
            entry_fn.args().to_vec(),
            &function,
            struct_constructors_enabled,
        )?;
        session.execute_entry_function(
            entry_fn.module(),
            entry_fn.function(),
            entry_fn.ty_args().to_vec(),
            args,
            gas_meter,
        )?;
        Ok(())
    }

    fn execute_script_or_entry_function<'a, 'r, 'l>(
        &'l self,
        resolver: &'r impl AptosMoveResolver,
        mut session: UserSession<'r, 'l>,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext<'a>,
        txn_data: &TransactionMetadata,
        payload: &'a TransactionPayload,
        log_context: &AdapterLogSchema,
        new_published_modules_loaded: &mut bool,
        change_set_configs: &ChangeSetConfigs,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        fail_point!("aptos_vm::execute_script_or_entry_function", |_| {
            Err(VMStatus::Error {
                status_code: StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
                sub_status: Some(move_core_types::vm_status::sub_status::unknown_invariant_violation::EPARANOID_FAILURE),
                message: None,
            })
        });

        gas_meter.charge_intrinsic_gas_for_transaction(txn_data.transaction_size())?;

        match payload {
            TransactionPayload::Script(script) => {
                session.execute(|session| {
                    self.validate_and_execute_script(
                        session,
                        gas_meter,
                        traversal_context,
                        txn_data.senders(),
                        script,
                    )
                })?;
            },
            TransactionPayload::EntryFunction(entry_fn) => {
                session.execute(|session| {
                    self.validate_and_execute_entry_function(
                        session,
                        gas_meter,
                        traversal_context,
                        txn_data.senders(),
                        entry_fn,
                        txn_data,
                    )
                })?;
            },

            // Not reachable as this function should only be invoked for entry or script
            // transaction payload.
            _ => unreachable!("Only scripts or entry functions are executed"),
        };

        session.execute(|session| {
            self.resolve_pending_code_publish(
                session,
                gas_meter,
                traversal_context,
                new_published_modules_loaded,
            )
        })?;

        let epilogue_session = self.charge_change_set_and_respawn_session(
            session,
            resolver,
            gas_meter,
            change_set_configs,
            txn_data,
        )?;

        self.success_transaction_cleanup(
            epilogue_session,
            gas_meter,
            txn_data,
            log_context,
            change_set_configs,
        )
    }

    fn charge_change_set(
        &self,
        change_set: &mut VMChangeSet,
        gas_meter: &mut impl AptosGasMeter,
        txn_data: &TransactionMetadata,
        resolver: &impl AptosMoveResolver,
    ) -> Result<GasQuantity<Octa>, VMStatus> {
        gas_meter.charge_io_gas_for_transaction(txn_data.transaction_size())?;
        for (event, _layout) in change_set.events() {
            gas_meter.charge_io_gas_for_event(event)?;
        }
        for (key, op_size) in change_set.write_set_size_iter() {
            gas_meter.charge_io_gas_for_write(key, &op_size)?;
        }

        let mut storage_refund = gas_meter.process_storage_fee_for_all(
            change_set,
            txn_data.transaction_size,
            txn_data.gas_unit_price,
            resolver.as_executor_view(),
        )?;
        if !self.features().is_storage_deletion_refund_enabled() {
            storage_refund = 0.into();
        }

        Ok(storage_refund)
    }

    fn charge_change_set_and_respawn_session<'r, 'l>(
        &'l self,
        user_session: UserSession<'r, 'l>,
        resolver: &'r impl AptosMoveResolver,
        gas_meter: &mut impl AptosGasMeter,
        change_set_configs: &ChangeSetConfigs,
        txn_data: &'l TransactionMetadata,
    ) -> Result<EpilogueSession<'r, 'l>, VMStatus> {
        let mut change_set = user_session.finish(change_set_configs)?;

        let storage_refund =
            self.charge_change_set(&mut change_set, gas_meter, txn_data, resolver)?;

        // TODO[agg_v1](fix): Charge for aggregator writes
        EpilogueSession::new(self, txn_data, resolver, change_set, storage_refund)
    }

    fn simulate_multisig_transaction<'a, 'r, 'l>(
        &'l self,
        resolver: &'r impl AptosMoveResolver,
        mut session: UserSession<'r, 'l>,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext<'a>,
        txn_data: &TransactionMetadata,
        payload: &'a Multisig,
        log_context: &AdapterLogSchema,
        new_published_modules_loaded: &mut bool,
        change_set_configs: &ChangeSetConfigs,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        match &payload.transaction_payload {
            None => Err(VMStatus::error(StatusCode::MISSING_DATA, None)),
            Some(multisig_payload) => {
                match multisig_payload {
                    MultisigTransactionPayload::EntryFunction(entry_function) => {
                        aptos_try!({
                            return_on_failure!(session.execute(|session| self
                                .execute_multisig_entry_function(
                                    session,
                                    gas_meter,
                                    traversal_context,
                                    payload.multisig_address,
                                    entry_function,
                                    new_published_modules_loaded,
                                    txn_data,
                                )));
                            // TODO: Deduplicate this against execute_multisig_transaction
                            // A bit tricky since we need to skip success/failure cleanups,
                            // which is in the middle. Introducing a boolean would make the code
                            // messier.
                            let epilogue_session = self.charge_change_set_and_respawn_session(
                                session,
                                resolver,
                                gas_meter,
                                change_set_configs,
                                txn_data,
                            )?;

                            self.success_transaction_cleanup(
                                epilogue_session,
                                gas_meter,
                                txn_data,
                                log_context,
                                change_set_configs,
                            )
                        })
                    },
                }
            },
        }
    }

    // Execute a multisig transaction:
    // 1. Obtain the payload of the transaction to execute. This could have been stored on chain
    // when the multisig transaction was created.
    // 2. Execute the target payload. If this fails, discard the session and keep the gas meter and
    // failure object. In case of success, keep the session and also do any necessary module publish
    // cleanup.
    // 3. Call post transaction cleanup function in multisig account module with the result from (2)
    fn execute_multisig_transaction<'r, 'l>(
        &'l self,
        resolver: &'r impl AptosMoveResolver,
        mut session: UserSession<'r, 'l>,
        prologue_change_set: &VMChangeSet,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext,
        txn_data: &TransactionMetadata,
        txn_payload: &Multisig,
        log_context: &AdapterLogSchema,
        new_published_modules_loaded: &mut bool,
        change_set_configs: &ChangeSetConfigs,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        fail_point!("move_adapter::execute_multisig_transaction", |_| {
            Err(VMStatus::error(
                StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
                None,
            ))
        });

        gas_meter.charge_intrinsic_gas_for_transaction(txn_data.transaction_size())?;

        // Step 1: Obtain the payload. If any errors happen here, the entire transaction should fail
        let invariant_violation_error = || {
            PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                .with_message("MultiSig transaction error".to_string())
                .finish(Location::Undefined)
        };
        let provided_payload = if let Some(payload) = &txn_payload.transaction_payload {
            bcs::to_bytes(&payload).map_err(|_| invariant_violation_error())?
        } else {
            // Default to empty bytes if payload is not provided.
            bcs::to_bytes::<Vec<u8>>(&vec![]).map_err(|_| invariant_violation_error())?
        };
        // Failures here will be propagated back.
        let payload_bytes: Vec<Vec<u8>> = session
            .execute(|session| {
                session.execute_function_bypass_visibility(
                    &MULTISIG_ACCOUNT_MODULE,
                    GET_NEXT_TRANSACTION_PAYLOAD,
                    vec![],
                    serialize_values(&vec![
                        MoveValue::Address(txn_payload.multisig_address),
                        MoveValue::vector_u8(provided_payload),
                    ]),
                    gas_meter,
                )
            })?
            .return_values
            .into_iter()
            .map(|(bytes, _ty)| bytes)
            .collect::<Vec<_>>();
        let payload_bytes = payload_bytes
            .first()
            // We expect the payload to either exists on chain or be passed along with the
            // transaction.
            .ok_or_else(|| {
                PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                    .with_message("Multisig payload bytes return error".to_string())
                    .finish(Location::Undefined)
            })?;
        // We have to deserialize twice as the first time returns the actual return type of the
        // function, which is vec<u8>. The second time deserializes it into the correct
        // EntryFunction payload type.
        // If either deserialization fails for some reason, that means the user provided incorrect
        // payload data either during transaction creation or execution.
        let deserialization_error = PartialVMError::new(StatusCode::FAILED_TO_DESERIALIZE_ARGUMENT)
            .finish(Location::Undefined);
        let payload_bytes =
            bcs::from_bytes::<Vec<u8>>(payload_bytes).map_err(|_| deserialization_error.clone())?;
        let payload = bcs::from_bytes::<MultisigTransactionPayload>(&payload_bytes)
            .map_err(|_| deserialization_error)?;

        // Step 2: Execute the target payload. Transaction failure here is tolerated. In case of any
        // failures, we'll discard the session and start a new one. This ensures that any data
        // changes are not persisted.
        // The multisig transaction would still be considered executed even if execution fails.
        let execution_result = match payload {
            MultisigTransactionPayload::EntryFunction(entry_function) => {
                session.execute(|session| {
                    self.execute_multisig_entry_function(
                        session,
                        gas_meter,
                        traversal_context,
                        txn_payload.multisig_address,
                        &entry_function,
                        new_published_modules_loaded,
                        txn_data,
                    )
                })
            },
        };

        // Step 3: Call post transaction cleanup function in multisig account module with the result
        // from Step 2.
        // Note that we don't charge execution or writeset gas for cleanup routines. This is
        // consistent with the high-level success/failure cleanup routines for user transactions.
        let cleanup_args = serialize_values(&vec![
            MoveValue::Address(txn_data.sender),
            MoveValue::Address(txn_payload.multisig_address),
            MoveValue::vector_u8(payload_bytes),
        ]);
        let epilogue_session = if let Err(execution_error) = execution_result {
            // Invalidate the loader cache in case there was a new module loaded from a module
            // publish request that failed.
            // This is redundant with the logic in execute_user_transaction but unfortunately is
            // necessary here as executing the underlying call can fail without this function
            // returning an error to execute_user_transaction.
            if *new_published_modules_loaded {
                self.move_vm.mark_loader_cache_as_invalid();
            };
            self.failure_multisig_payload_cleanup(
                resolver,
                prologue_change_set,
                execution_error,
                txn_data,
                cleanup_args,
            )?
        } else {
            self.success_multisig_payload_cleanup(
                resolver,
                session,
                gas_meter,
                txn_data,
                cleanup_args,
                change_set_configs,
            )?
        };

        // TODO(Gas): Charge for aggregator writes
        self.success_transaction_cleanup(
            epilogue_session,
            gas_meter,
            txn_data,
            log_context,
            change_set_configs,
        )
    }

    fn execute_or_simulate_multisig_transaction<'a, 'r, 'l>(
        &'l self,
        resolver: &'r impl AptosMoveResolver,
        session: UserSession<'r, 'l>,
        proglogue_change_set: &VMChangeSet,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext<'a>,
        txn_data: &TransactionMetadata,
        payload: &'a Multisig,
        log_context: &AdapterLogSchema,
        new_published_modules_loaded: &mut bool,
        change_set_configs: &ChangeSetConfigs,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        if self.is_simulation {
            self.simulate_multisig_transaction(
                resolver,
                session,
                gas_meter,
                traversal_context,
                txn_data,
                payload,
                log_context,
                new_published_modules_loaded,
                change_set_configs,
            )
        } else {
            self.execute_multisig_transaction(
                resolver,
                session,
                proglogue_change_set,
                gas_meter,
                traversal_context,
                txn_data,
                payload,
                log_context,
                new_published_modules_loaded,
                change_set_configs,
            )
        }
    }

    fn execute_multisig_entry_function(
        &self,
        session: &mut SessionExt,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext,
        multisig_address: AccountAddress,
        payload: &EntryFunction,
        new_published_modules_loaded: &mut bool,
        txn_data: &TransactionMetadata,
    ) -> Result<(), VMStatus> {
        // If txn args are not valid, we'd still consider the transaction as executed but
        // failed. This is primarily because it's unrecoverable at this point.
        self.validate_and_execute_entry_function(
            session,
            gas_meter,
            traversal_context,
            vec![multisig_address],
            payload,
            txn_data,
        )?;

        // Resolve any pending module publishes in case the multisig transaction is deploying
        // modules.
        self.resolve_pending_code_publish(
            session,
            gas_meter,
            traversal_context,
            new_published_modules_loaded,
        )?;
        Ok(())
    }

    fn success_multisig_payload_cleanup<'r, 'l>(
        &'l self,
        resolver: &'r impl AptosMoveResolver,
        session: UserSession<'r, 'l>,
        gas_meter: &mut impl AptosGasMeter,
        txn_data: &'l TransactionMetadata,
        cleanup_args: Vec<Vec<u8>>,
        change_set_configs: &ChangeSetConfigs,
    ) -> Result<EpilogueSession<'r, 'l>, VMStatus> {
        // Charge gas for write set before we do cleanup. This ensures we don't charge gas for
        // cleanup write set changes, which is consistent with outer-level success cleanup
        // flow. We also wouldn't need to worry that we run out of gas when doing cleanup.
        let mut epilogue_session = self.charge_change_set_and_respawn_session(
            session,
            resolver,
            gas_meter,
            change_set_configs,
            txn_data,
        )?;
        epilogue_session.execute(|session| {
            session
                .execute_function_bypass_visibility(
                    &MULTISIG_ACCOUNT_MODULE,
                    SUCCESSFUL_TRANSACTION_EXECUTION_CLEANUP,
                    vec![],
                    cleanup_args,
                    &mut UnmeteredGasMeter,
                )
                .map_err(|e| e.into_vm_status())
        })?;
        Ok(epilogue_session)
    }

    fn failure_multisig_payload_cleanup<'r, 'l>(
        &'l self,
        resolver: &'r impl AptosMoveResolver,
        prologue_change_set: &VMChangeSet,
        execution_error: VMStatus,
        txn_data: &'l TransactionMetadata,
        mut cleanup_args: Vec<Vec<u8>>,
    ) -> Result<EpilogueSession<'r, 'l>, VMStatus> {
        // Start a fresh session for running cleanup that does not contain any changes from
        // the inner function call earlier (since it failed).
        let mut epilogue_session = EpilogueSession::new(
            self,
            txn_data,
            resolver,
            prologue_change_set.clone(),
            0.into(),
        )?;
        let execution_error = ExecutionError::try_from(execution_error)
            .map_err(|_| VMStatus::error(StatusCode::UNREACHABLE, None))?;
        // Serialization is not expected to fail so we're using invariant_violation error here.
        cleanup_args.push(bcs::to_bytes(&execution_error).map_err(|_| {
            PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                .with_message("MultiSig payload cleanup error.".to_string())
                .finish(Location::Undefined)
        })?);
        epilogue_session.execute(|session| {
            session
                .execute_function_bypass_visibility(
                    &MULTISIG_ACCOUNT_MODULE,
                    FAILED_TRANSACTION_EXECUTION_CLEANUP,
                    vec![],
                    cleanup_args,
                    &mut UnmeteredGasMeter,
                )
                .map_err(|e| e.into_vm_status())
        })?;
        Ok(epilogue_session)
    }

    /// Execute all module initializers.
    fn execute_module_initialization(
        &self,
        session: &mut SessionExt,
        gas_meter: &mut impl AptosGasMeter,
        modules: &[CompiledModule],
        exists: BTreeSet<ModuleId>,
        senders: &[AccountAddress],
        new_published_modules_loaded: &mut bool,
    ) -> VMResult<()> {
        let init_func_name = ident_str!("init_module");
        for module in modules {
            if exists.contains(&module.self_id()) {
                // Call initializer only on first publish.
                continue;
            }
            *new_published_modules_loaded = true;
            let init_function = session.load_function(&module.self_id(), init_func_name, &[]);
            // it is ok to not have init_module function
            // init_module function should be (1) private and (2) has no return value
            // Note that for historic reasons, verification here is treated
            // as StatusCode::CONSTRAINT_NOT_SATISFIED, there this cannot be unified
            // with the general verify_module above.
            if init_function.is_ok() {
                if verifier::module_init::verify_module_init_function(module).is_ok() {
                    let args: Vec<Vec<u8>> = senders
                        .iter()
                        .map(|s| MoveValue::Signer(*s).simple_serialize().unwrap())
                        .collect();
                    session.execute_function_bypass_visibility(
                        &module.self_id(),
                        init_func_name,
                        vec![],
                        args,
                        gas_meter,
                    )?;
                } else {
                    return Err(PartialVMError::new(StatusCode::CONSTRAINT_NOT_SATISFIED)
                        .finish(Location::Undefined));
                }
            }
        }
        Ok(())
    }

    /// Deserialize a module bundle.
    fn deserialize_module_bundle(&self, modules: &ModuleBundle) -> VMResult<Vec<CompiledModule>> {
        let max_version = get_max_binary_format_version(self.features(), None);
        let max_identifier_size = get_max_identifier_size(self.features());
        let config = DeserializerConfig::new(max_version, max_identifier_size);
        let mut result = vec![];
        for module_blob in modules.iter() {
            match CompiledModule::deserialize_with_config(module_blob.code(), &config) {
                Ok(module) => {
                    result.push(module);
                },
                Err(_err) => {
                    return Err(PartialVMError::new(StatusCode::CODE_DESERIALIZATION_ERROR)
                        .finish(Location::Undefined))
                },
            }
        }
        Ok(result)
    }

    /// Resolve a pending code publish request registered via the NativeCodeContext.
    fn resolve_pending_code_publish(
        &self,
        session: &mut SessionExt,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext,
        new_published_modules_loaded: &mut bool,
    ) -> VMResult<()> {
        if let Some(PublishRequest {
            destination,
            bundle,
            expected_modules,
            allowed_deps,
            check_compat: _,
        }) = session.extract_publish_request()
        {
            // TODO: unfortunately we need to deserialize the entire bundle here to handle
            // `init_module` and verify some deployment conditions, while the VM need to do
            // the deserialization again. Consider adding an API to MoveVM which allows to
            // directly pass CompiledModule.
            let modules = self.deserialize_module_bundle(&bundle)?;
            let modules: &Vec<CompiledModule> =
                traversal_context.referenced_module_bundles.alloc(modules);

            // Note: Feature gating is needed here because the traversal of the dependencies could
            //       result in shallow-loading of the modules and therefore subtle changes in
            //       the error semantics.
            if self.gas_feature_version >= 15 {
                // Charge old versions of the modules, in case of upgrades.
                session.check_dependencies_and_charge_gas_non_recursive_optional(
                    gas_meter,
                    traversal_context,
                    modules
                        .iter()
                        .map(|module| (module.self_addr(), module.self_name())),
                )?;

                // Charge all modules in the bundle that is about to be published.
                for (module, blob) in modules.iter().zip(bundle.iter()) {
                    let module_id = &module.self_id();
                    gas_meter
                        .charge_dependency(
                            true,
                            module_id.address(),
                            module_id.name(),
                            NumBytes::new(blob.code().len() as u64),
                        )
                        .map_err(|err| err.finish(Location::Undefined))?;
                }

                // Charge all dependencies.
                //
                // Must exclude the ones that are in the current bundle because they have not
                // been published yet.
                let module_ids_in_bundle = modules
                    .iter()
                    .map(|module| (module.self_addr(), module.self_name()))
                    .collect::<BTreeSet<_>>();

                session.check_dependencies_and_charge_gas(
                    gas_meter,
                    traversal_context,
                    modules
                        .iter()
                        .flat_map(|module| {
                            module
                                .immediate_dependencies_iter()
                                .chain(module.immediate_friends_iter())
                        })
                        .filter(|addr_and_name| !module_ids_in_bundle.contains(addr_and_name)),
                )?;

                // TODO: Revisit the order of traversal. Consider switching to alphabetical order.
            }

            // Validate the module bundle
            self.validate_publish_request(session, modules, expected_modules, allowed_deps)?;

            // Check what modules exist before publishing.
            let mut exists = BTreeSet::new();
            for m in modules {
                let id = m.self_id();
                if session.exists_module(&id)? {
                    exists.insert(id);
                }
            }

            // Publish the bundle and execute initializers
            // publish_module_bundle doesn't actually load the published module into
            // the loader cache. It only puts the module data in the data cache.
            return_on_failure!(session.publish_module_bundle_with_compat_config(
                bundle.into_inner(),
                destination,
                gas_meter,
                Compatibility::new(
                    true,
                    true,
                    !self
                        .features()
                        .is_enabled(FeatureFlag::TREAT_FRIEND_AS_PRIVATE),
                ),
            ));

            self.execute_module_initialization(
                session,
                gas_meter,
                modules,
                exists,
                &[destination],
                new_published_modules_loaded,
            )
        } else {
            Ok(())
        }
    }

    /// Validate a publish request.
    fn validate_publish_request(
        &self,
        session: &mut SessionExt,
        modules: &[CompiledModule],
        mut expected_modules: BTreeSet<String>,
        allowed_deps: Option<BTreeMap<AccountAddress, BTreeSet<String>>>,
    ) -> VMResult<()> {
        for m in modules {
            if !expected_modules.remove(m.self_id().name().as_str()) {
                return Err(Self::metadata_validation_error(&format!(
                    "unregistered module: '{}'",
                    m.self_id().name()
                )));
            }
            if let Some(allowed) = &allowed_deps {
                for dep in m.immediate_dependencies() {
                    if !allowed
                        .get(dep.address())
                        .map(|modules| {
                            modules.contains("") || modules.contains(dep.name().as_str())
                        })
                        .unwrap_or(false)
                    {
                        return Err(Self::metadata_validation_error(&format!(
                            "unregistered dependency: '{}'",
                            dep
                        )));
                    }
                }
            }
            aptos_framework::verify_module_metadata(m, self.features(), &self.timed_features)
                .map_err(|err| Self::metadata_validation_error(&err.to_string()))?;
        }
        verifier::resource_groups::validate_resource_groups(
            session,
            modules,
            self.features()
                .is_enabled(FeatureFlag::SAFER_RESOURCE_GROUPS),
        )?;
        verifier::event_validation::validate_module_events(session, modules)?;

        if !expected_modules.is_empty() {
            return Err(Self::metadata_validation_error(
                "not all registered modules published",
            ));
        }
        Ok(())
    }

    fn metadata_validation_error(msg: &str) -> VMError {
        PartialVMError::new(StatusCode::CONSTRAINT_NOT_SATISFIED)
            .with_message(format!("metadata and code bundle mismatch: {}", msg))
            .finish(Location::Undefined)
    }

    pub(crate) fn make_standard_gas_meter(
        &self,
        balance: Gas,
        log_context: &AdapterLogSchema,
    ) -> Result<MemoryTrackedGasMeter<StandardGasMeter<StandardGasAlgebra>>, VMStatus> {
        Ok(MemoryTrackedGasMeter::new(StandardGasMeter::new(
            StandardGasAlgebra::new(
                self.gas_feature_version,
                get_or_vm_startup_failure(&self.gas_params, log_context)?
                    .vm
                    .clone(),
                get_or_vm_startup_failure(&self.storage_gas_params, log_context)?.clone(),
                balance,
            ),
        )))
    }

    fn validate_signed_transaction(
        &self,
        session: &mut SessionExt,
        resolver: &impl AptosMoveResolver,
        transaction: &SignedTransaction,
        transaction_data: &TransactionMetadata,
        log_context: &AdapterLogSchema,
    ) -> Result<(), VMStatus> {
        // Check transaction format.
        if transaction.contains_duplicate_signers() {
            return Err(VMStatus::error(
                StatusCode::SIGNERS_CONTAIN_DUPLICATES,
                None,
            ));
        }

        let authenticators = aptos_types::keyless::get_authenticators(transaction)
            .map_err(|_| VMStatus::error(StatusCode::INVALID_SIGNATURE, None))?;

        // If there are keyless TXN authenticators, validate them all.
        if !authenticators.is_empty() {
            keyless_validation::validate_authenticators(
                &authenticators,
                self.features(),
                resolver,
            )?;
        }

        // The prologue MUST be run AFTER any validation. Otherwise you may run prologue and hit
        // SEQUENCE_NUMBER_TOO_NEW if there is more than one transaction from the same sender and
        // end up skipping validation.
        self.run_prologue_with_payload(
            session,
            resolver,
            transaction.payload(),
            transaction_data,
            log_context,
        )
    }

    // Called when the execution of the user transaction fails, in order to discard the
    // transaction, or clean up the failed state.
    fn on_user_transaction_execution_failure(
        &self,
        prologue_change_set: VMChangeSet,
        err: VMStatus,
        resolver: &impl AptosMoveResolver,
        txn_data: &TransactionMetadata,
        log_context: &AdapterLogSchema,
        gas_meter: &mut impl AptosGasMeter,
        change_set_configs: &ChangeSetConfigs,
        new_published_modules_loaded: bool,
    ) -> (VMStatus, VMOutput) {
        // Invalidate the loader cache in case there was a new module loaded from a module
        // publish request that failed.
        // This ensures the loader cache is flushed later to align storage with the cache.
        // None of the modules in the bundle will be committed to storage,
        // but some of them may have ended up in the cache.
        if new_published_modules_loaded {
            self.move_vm.mark_loader_cache_as_invalid();
        };

        self.failed_transaction_cleanup(
            prologue_change_set,
            err,
            gas_meter,
            txn_data,
            resolver,
            log_context,
            change_set_configs,
        )
    }

    fn execute_user_transaction_impl<'a>(
        &self,
        resolver: &impl AptosMoveResolver,
        txn: &'a SignedTransaction,
        log_context: &AdapterLogSchema,
        gas_meter: &mut impl AptosGasMeter,
        traversal_context: &mut TraversalContext<'a>,
    ) -> (VMStatus, VMOutput) {
        let mut txn_data = TransactionMetadata::new(txn);

        // Revalidate the transaction.
        let mut prologue_session =
            unwrap_or_discard!(PrologueSession::new(self, &txn_data, resolver));
        unwrap_or_discard!(prologue_session.execute(|session| {
            let required_deposit = self.get_required_deposit(
                session,
                resolver,
                &gas_meter.vm_gas_params().txn,
                &txn_data,
                txn.payload(),
            );
            txn_data.set_required_deposit(required_deposit);
            self.validate_signed_transaction(session, resolver, txn, &txn_data, log_context)
        }));

        let storage_gas_params = unwrap_or_discard!(get_or_vm_startup_failure(
            &self.storage_gas_params,
            log_context
        ));
        let change_set_configs = &storage_gas_params.change_set_configs;
        let (prologue_change_set, mut user_session) = unwrap_or_discard!(prologue_session
            .into_user_session(
                self,
                &txn_data,
                resolver,
                self.gas_feature_version,
                change_set_configs,
            ));

        let is_account_init_for_sponsored_transaction = unwrap_or_discard!(
            is_account_init_for_sponsored_transaction(&txn_data, self.features(), resolver)
        );
        if is_account_init_for_sponsored_transaction {
            unwrap_or_discard!(
                user_session.execute(|session| create_account_if_does_not_exist(
                    session,
                    gas_meter,
                    txn.sender()
                ))
            );
        }

        // We keep track of whether any newly published modules are loaded into the Vm's loader
        // cache as part of executing transactions. This would allow us to decide whether the cache
        // should be flushed later.
        let mut new_published_modules_loaded = false;
        let result = match txn.payload() {
            payload @ TransactionPayload::Script(_)
            | payload @ TransactionPayload::EntryFunction(_) => self
                .execute_script_or_entry_function(
                    resolver,
                    user_session,
                    gas_meter,
                    traversal_context,
                    &txn_data,
                    payload,
                    log_context,
                    &mut new_published_modules_loaded,
                    change_set_configs,
                ),
            TransactionPayload::Multisig(payload) => self.execute_or_simulate_multisig_transaction(
                resolver,
                user_session,
                &prologue_change_set,
                gas_meter,
                traversal_context,
                &txn_data,
                payload,
                log_context,
                &mut new_published_modules_loaded,
                change_set_configs,
            ),

            // Deprecated. We cannot make this `unreachable!` because a malicious
            // validator can craft this transaction and cause the node to panic.
            TransactionPayload::ModuleBundle(_) => {
                unwrap_or_discard!(Err(deprecated_module_bundle!()))
            },
        };

        let gas_usage = txn_data
            .max_gas_amount()
            .checked_sub(gas_meter.balance())
            .expect("Balance should always be less than or equal to max gas amount set");
        TXN_GAS_USAGE.observe(u64::from(gas_usage) as f64);

        result.unwrap_or_else(|err| {
            self.on_user_transaction_execution_failure(
                prologue_change_set,
                err,
                resolver,
                &txn_data,
                log_context,
                gas_meter,
                change_set_configs,
                new_published_modules_loaded,
            )
        })
    }

    fn execute_user_transaction(
        &self,
        resolver: &impl AptosMoveResolver,
        txn: &SignedTransaction,
        log_context: &AdapterLogSchema,
    ) -> (VMStatus, VMOutput) {
        let balance = txn.max_gas_amount().into();
        // TODO: would we end up having a diverging behavior by creating the gas meter at an earlier time?
        let mut gas_meter = unwrap_or_discard!(self.make_standard_gas_meter(balance, log_context));

        let traversal_storage = TraversalStorage::new();
        let mut traversal_context = TraversalContext::new(&traversal_storage);

        self.execute_user_transaction_impl(
            resolver,
            txn,
            log_context,
            &mut gas_meter,
            &mut traversal_context,
        )
    }

    pub fn execute_user_transaction_with_custom_gas_meter<G, F>(
        &self,
        resolver: &impl AptosMoveResolver,
        txn: &SignatureCheckedTransaction,
        log_context: &AdapterLogSchema,
        make_gas_meter: F,
    ) -> Result<(VMStatus, VMOutput, G), VMStatus>
    where
        G: AptosGasMeter,
        F: FnOnce(u64, VMGasParameters, StorageGasParameters, Gas) -> Result<G, VMStatus>,
    {
        let balance = txn.max_gas_amount().into();
        let mut gas_meter = make_gas_meter(
            self.gas_feature_version,
            get_or_vm_startup_failure(&self.gas_params, log_context)?
                .vm
                .clone(),
            get_or_vm_startup_failure(&self.storage_gas_params, log_context)?.clone(),
            balance,
        )?;
        let traversal_storage = TraversalStorage::new();
        let mut traversal_context = TraversalContext::new(&traversal_storage);
        let (status, output) = self.execute_user_transaction_impl(
            resolver,
            txn,
            log_context,
            &mut gas_meter,
            &mut traversal_context,
        );

        Ok((status, output, gas_meter))
    }

    fn execute_write_set(
        &self,
        resolver: &impl AptosMoveResolver,
        write_set_payload: &WriteSetPayload,
        txn_sender: Option<AccountAddress>,
        session_id: SessionId,
    ) -> Result<VMChangeSet, VMStatus> {
        let change_set_configs =
            ChangeSetConfigs::unlimited_at_gas_feature_version(self.gas_feature_version);

        match write_set_payload {
            WriteSetPayload::Direct(change_set) => {
                // this transaction is never delayed field capable.
                // it requires restarting execution afterwards,
                // which allows it to be used as last transaction in delayed_field_enabled context.
                let change = VMChangeSet::try_from_storage_change_set_with_delayed_field_optimization_disabled(
                    change_set.clone(),
                    &change_set_configs,
                )
                .map_err(|e| e.into_vm_status())?;

                // validate_waypoint_change_set checks that this is true, so we only log here.
                if !Self::should_restart_execution(&change) {
                    // This invariant needs to hold irrespectively, so we log error always.
                    // but if we are in delayed_field_optimization_capable context, we cannot execute any transaction after this.
                    // as transaction afterwards would be executed assuming delayed fields are exchanged and
                    // resource groups are split, but WriteSetPayload::Direct has materialized writes,
                    // and so after executing this transaction versioned state is inconsistent.
                    error!(
                        "[aptos_vm] direct write set finished without requiring should_restart_execution");
                }

                Ok(change)
            },
            WriteSetPayload::Script { script, execute_as } => {
                let mut tmp_session = self.new_session(resolver, session_id);
                let senders = match txn_sender {
                    None => vec![*execute_as],
                    Some(sender) => vec![sender, *execute_as],
                };

                let traversal_storage = TraversalStorage::new();
                let mut traversal_context = TraversalContext::new(&traversal_storage);

                self.validate_and_execute_script(
                    &mut tmp_session,
                    &mut UnmeteredGasMeter,
                    &mut traversal_context,
                    senders,
                    script,
                )?;
                Ok(tmp_session.finish(&change_set_configs)?)
            },
        }
    }

    fn read_change_set(
        &self,
        executor_view: &dyn ExecutorView,
        resource_group_view: &dyn ResourceGroupView,
        change_set: &VMChangeSet,
    ) -> PartialVMResult<()> {
        assert!(
            change_set.aggregator_v1_write_set().is_empty(),
            "Waypoint change set should not have any aggregator writes."
        );

        // All Move executions satisfy the read-before-write property. Thus we need to read each
        // access path that the write set is going to update.
        for state_key in change_set.module_write_set().keys() {
            executor_view.get_module_state_value(state_key)?;
        }
        for (state_key, write_op) in change_set.resource_write_set().iter() {
            executor_view.get_resource_state_value(state_key, None)?;
            if let AbstractResourceWriteOp::WriteResourceGroup(group_write) = write_op {
                for (tag, (_, maybe_layout)) in group_write.inner_ops() {
                    resource_group_view.get_resource_from_group(
                        state_key,
                        tag,
                        maybe_layout.as_deref(),
                    )?;
                }
            }
        }

        Ok(())
    }

    fn validate_waypoint_change_set(
        change_set: &VMChangeSet,
        log_context: &AdapterLogSchema,
    ) -> Result<(), VMStatus> {
        let has_new_block_event = change_set
            .events()
            .iter()
            .any(|(e, _)| e.event_key() == Some(&new_block_event_key()));
        let has_new_epoch_event = change_set
            .events()
            .iter()
            .any(|(e, _)| e.event_key() == Some(&new_epoch_event_key()));
        if has_new_block_event && has_new_epoch_event {
            Ok(())
        } else {
            error!(
                *log_context,
                "[aptos_vm] waypoint txn needs to emit new epoch and block"
            );
            Err(VMStatus::error(StatusCode::INVALID_WRITE_SET, None))
        }
    }

    pub(crate) fn process_waypoint_change_set(
        &self,
        resolver: &impl AptosMoveResolver,
        write_set_payload: WriteSetPayload,
        log_context: &AdapterLogSchema,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        // TODO: user specified genesis id to distinguish different genesis write sets
        let genesis_id = HashValue::zero();
        let change_set = self.execute_write_set(
            resolver,
            &write_set_payload,
            Some(aptos_types::account_config::reserved_vm_address()),
            SessionId::genesis(genesis_id),
        )?;

        Self::validate_waypoint_change_set(&change_set, log_context)?;
        self.read_change_set(
            resolver.as_executor_view(),
            resolver.as_resource_group_view(),
            &change_set,
        )
        .map_err(|e| e.finish(Location::Undefined).into_vm_status())?;

        SYSTEM_TRANSACTIONS_EXECUTED.inc();

        let output = VMOutput::new(
            change_set,
            FeeStatement::zero(),
            TransactionStatus::from_executed_vm_status(VMStatus::Executed),
            TransactionAuxiliaryData::default(),
        );
        Ok((VMStatus::Executed, output))
    }

    fn process_block_prologue(
        &self,
        resolver: &impl AptosMoveResolver,
        block_metadata: BlockMetadata,
        log_context: &AdapterLogSchema,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        fail_point!("move_adapter::process_block_prologue", |_| {
            Err(VMStatus::error(
                StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
                None,
            ))
        });

        let mut gas_meter = UnmeteredGasMeter;
        let mut session = self.new_session(resolver, SessionId::block_meta(&block_metadata));

        let args = serialize_values(
            &block_metadata.get_prologue_move_args(account_config::reserved_vm_address()),
        );
        session
            .execute_function_bypass_visibility(
                &BLOCK_MODULE,
                BLOCK_PROLOGUE,
                vec![],
                args,
                &mut gas_meter,
            )
            .map(|_return_vals| ())
            .or_else(|e| {
                expect_only_successful_execution(e, BLOCK_PROLOGUE.as_str(), log_context)
            })?;
        SYSTEM_TRANSACTIONS_EXECUTED.inc();

        let output = get_system_transaction_output(
            session,
            FeeStatement::zero(),
            ExecutionStatus::Success,
            &get_or_vm_startup_failure(&self.storage_gas_params, log_context)?.change_set_configs,
        )?;
        Ok((VMStatus::Executed, output))
    }

    fn process_block_prologue_ext(
        &self,
        resolver: &impl AptosMoveResolver,
        block_metadata_ext: BlockMetadataExt,
        log_context: &AdapterLogSchema,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        fail_point!("move_adapter::process_block_prologue_ext", |_| {
            Err(VMStatus::error(
                StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
                None,
            ))
        });

        let mut gas_meter = UnmeteredGasMeter;
        let mut session =
            self.new_session(resolver, SessionId::block_meta_ext(&block_metadata_ext));

        let block_metadata_with_randomness = match block_metadata_ext {
            BlockMetadataExt::V0(_) => unreachable!(),
            BlockMetadataExt::V1(v1) => v1,
        };

        let BlockMetadataWithRandomness {
            id,
            epoch,
            round,
            proposer,
            previous_block_votes_bitvec,
            failed_proposer_indices,
            timestamp_usecs,
            randomness,
        } = block_metadata_with_randomness;

        let args = vec![
            MoveValue::Signer(AccountAddress::ZERO), // Run as 0x0
            MoveValue::Address(AccountAddress::from_bytes(id.to_vec()).unwrap()),
            MoveValue::U64(epoch),
            MoveValue::U64(round),
            MoveValue::Address(proposer),
            failed_proposer_indices
                .into_iter()
                .map(|i| i as u64)
                .collect::<Vec<_>>()
                .as_move_value(),
            previous_block_votes_bitvec.as_move_value(),
            MoveValue::U64(timestamp_usecs),
            randomness
                .as_ref()
                .map(Randomness::randomness_cloned)
                .as_move_value(),
        ];

        session
            .execute_function_bypass_visibility(
                &BLOCK_MODULE,
                BLOCK_PROLOGUE_EXT,
                vec![],
                serialize_values(&args),
                &mut gas_meter,
            )
            .map(|_return_vals| ())
            .or_else(|e| {
                expect_only_successful_execution(e, BLOCK_PROLOGUE_EXT.as_str(), log_context)
            })?;
        SYSTEM_TRANSACTIONS_EXECUTED.inc();

        let output = get_system_transaction_output(
            session,
            FeeStatement::zero(),
            ExecutionStatus::Success,
            &get_or_vm_startup_failure(&self.storage_gas_params, log_context)?.change_set_configs,
        )?;
        Ok((VMStatus::Executed, output))
    }

    fn extract_module_metadata(&self, module: &ModuleId) -> Option<Arc<RuntimeModuleMetadataV1>> {
        if self.features().is_enabled(FeatureFlag::VM_BINARY_FORMAT_V6) {
            aptos_framework::get_vm_metadata(&self.move_vm, module)
        } else {
            aptos_framework::get_vm_metadata_v0(&self.move_vm, module)
        }
    }

    pub fn execute_view_function(
        state_view: &impl StateView,
        module_id: ModuleId,
        func_name: Identifier,
        type_args: Vec<TypeTag>,
        arguments: Vec<Vec<u8>>,
        max_gas_amount: u64,
    ) -> ViewFunctionOutput {
        let resolver = state_view.as_move_resolver();
        let vm = AptosVM::new(
            &resolver,
            /*override_is_delayed_field_optimization_capable=*/ Some(false),
        );
        let log_context = AdapterLogSchema::new(state_view.id(), 0);
        let mut gas_meter = match vm.make_standard_gas_meter(max_gas_amount.into(), &log_context) {
            Ok(gas_meter) => gas_meter,
            Err(e) => return ViewFunctionOutput::new(Err(anyhow::Error::msg(format!("{}", e))), 0),
        };

        let mut session = vm.new_session(&resolver, SessionId::Void);
        let execution_result = Self::execute_view_function_in_vm(
            &mut session,
            &vm,
            module_id,
            func_name,
            type_args,
            arguments,
            &mut gas_meter,
        );
        let gas_used = Self::gas_used(max_gas_amount.into(), &gas_meter);
        match execution_result {
            Ok(result) => ViewFunctionOutput::new(Ok(result), gas_used),
            Err(e) => ViewFunctionOutput::new(Err(e), gas_used),
        }
    }

    fn gas_used(max_gas_amount: Gas, gas_meter: &impl AptosGasMeter) -> u64 {
        max_gas_amount
            .checked_sub(gas_meter.balance())
            .expect("Balance should always be less than or equal to max gas amount")
            .into()
    }

    fn execute_view_function_in_vm(
        session: &mut SessionExt,
        vm: &AptosVM,
        module_id: ModuleId,
        func_name: Identifier,
        type_args: Vec<TypeTag>,
        arguments: Vec<Vec<u8>>,
        gas_meter: &mut impl AptosGasMeter,
    ) -> anyhow::Result<Vec<Vec<u8>>> {
        let func_inst = session.load_function(&module_id, &func_name, &type_args)?;
        let metadata = vm.extract_module_metadata(&module_id);
        let arguments = verifier::view_function::validate_view_function(
            session,
            arguments,
            func_name.as_ident_str(),
            &func_inst,
            metadata.as_ref().map(Arc::as_ref),
            vm.features().is_enabled(FeatureFlag::STRUCT_CONSTRUCTORS),
        )?;

        Ok(session
            .execute_function_bypass_visibility(
                &module_id,
                func_name.as_ident_str(),
                type_args,
                arguments,
                gas_meter,
            )
            .map_err(|err| anyhow!("Failed to execute function: {:?}", err))?
            .return_values
            .into_iter()
            .map(|(bytes, _ty)| bytes)
            .collect::<Vec<_>>())
    }

    fn run_prologue_with_payload(
        &self,
        session: &mut SessionExt,
        resolver: &impl AptosMoveResolver,
        payload: &TransactionPayload,
        txn_data: &TransactionMetadata,
        log_context: &AdapterLogSchema,
    ) -> Result<(), VMStatus> {
        check_gas(
            get_or_vm_startup_failure(&self.gas_params, log_context)?,
            self.gas_feature_version,
            resolver,
            txn_data,
            self.features(),
            log_context,
        )?;

        match payload {
            TransactionPayload::Script(_) | TransactionPayload::EntryFunction(_) => {
                transaction_validation::run_script_prologue(session, txn_data, log_context)
            },
            TransactionPayload::Multisig(multisig_payload) => {
                // Still run script prologue for multisig transaction to ensure the same tx
                // validations are still run for this multisig execution tx, which is submitted by
                // one of the owners.
                transaction_validation::run_script_prologue(session, txn_data, log_context)?;
                // Skip validation if this is part of tx simulation.
                // This allows simulating multisig txs without having to first create the multisig
                // tx.
                if !self.is_simulation {
                    transaction_validation::run_multisig_prologue(
                        session,
                        txn_data,
                        multisig_payload,
                        log_context,
                    )
                } else {
                    Ok(())
                }
            },

            // Deprecated.
            TransactionPayload::ModuleBundle(_) => Err(deprecated_module_bundle!()),
        }
    }

    pub fn should_restart_execution(vm_change_set: &VMChangeSet) -> bool {
        let new_epoch_event_key = new_epoch_event_key();
        vm_change_set
            .events()
            .iter()
            .any(|(event, _)| event.event_key() == Some(&new_epoch_event_key))
    }

    /// Executes a single transaction (including user transactions, block
    /// metadata and state checkpoint, etc.).
    /// *Precondition:* VM has to be instantiated in execution mode.
    pub fn execute_single_transaction(
        &self,
        txn: &SignatureVerifiedTransaction,
        resolver: &impl AptosMoveResolver,
        log_context: &AdapterLogSchema,
    ) -> Result<(VMStatus, VMOutput), VMStatus> {
        assert!(!self.is_simulation, "VM has to be created for execution");

        if let SignatureVerifiedTransaction::Invalid(_) = txn {
            let vm_status = VMStatus::error(StatusCode::INVALID_SIGNATURE, None);
            let discarded_output = discarded_output(vm_status.status_code());
            return Ok((vm_status, discarded_output));
        }

        Ok(match txn.expect_valid() {
            Transaction::BlockMetadata(block_metadata) => {
                fail_point!("aptos_vm::execution::block_metadata");
                let (vm_status, output) =
                    self.process_block_prologue(resolver, block_metadata.clone(), log_context)?;
                (vm_status, output)
            },
            Transaction::BlockMetadataExt(block_metadata_ext) => {
                fail_point!("aptos_vm::execution::block_metadata_ext");
                let (vm_status, output) = self.process_block_prologue_ext(
                    resolver,
                    block_metadata_ext.clone(),
                    log_context,
                )?;
                (vm_status, output)
            },
            Transaction::GenesisTransaction(write_set_payload) => {
                let (vm_status, output) = self.process_waypoint_change_set(
                    resolver,
                    write_set_payload.clone(),
                    log_context,
                )?;
                (vm_status, output)
            },
            Transaction::UserTransaction(txn) => {
                fail_point!("aptos_vm::execution::user_transaction");
                let _timer = TXN_TOTAL_SECONDS.start_timer();
                let (vm_status, output) = self.execute_user_transaction(resolver, txn, log_context);

                if let StatusType::InvariantViolation = vm_status.status_type() {
                    match vm_status.status_code() {
                        // Type resolution failure can be triggered by user input when providing a bad type argument, skip this case.
                        StatusCode::TYPE_RESOLUTION_FAILURE
                        if vm_status.sub_status()
                            == Some(move_core_types::vm_status::sub_status::type_resolution_failure::EUSER_TYPE_LOADING_FAILURE) => {},
                        // The known Move function failure and type resolution failure could be a result of speculative execution. Use speculative logger.
                        StatusCode::UNEXPECTED_ERROR_FROM_KNOWN_MOVE_FUNCTION
                        | StatusCode::TYPE_RESOLUTION_FAILURE => {
                            speculative_error!(
                                log_context,
                                format!(
                                    "[aptos_vm] Transaction breaking invariant violation. txn: {:?}, status: {:?}",
                                    bcs::to_bytes::<SignedTransaction>(txn),
                                    vm_status
                                ),
                            );
                        },
                        // Paranoid mode failure. We need to be alerted about this ASAP.
                        StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR
                        if vm_status.sub_status()
                            == Some(move_core_types::vm_status::sub_status::unknown_invariant_violation::EPARANOID_FAILURE) =>
                            {
                                error!(
                                *log_context,
                                "[aptos_vm] Transaction breaking paranoid mode. txn: {:?}, status: {:?}",
                                bcs::to_bytes::<SignedTransaction>(txn),
                                vm_status,
                            );
                            },
                        // Paranoid mode failure but with reference counting
                        StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR
                        if vm_status.sub_status()
                            == Some(move_core_types::vm_status::sub_status::unknown_invariant_violation::EREFERENCE_COUNTING_FAILURE) =>
                            {
                                error!(
                                *log_context,
                                "[aptos_vm] Transaction breaking paranoid mode. txn: {:?}, status: {:?}",
                                bcs::to_bytes::<SignedTransaction>(txn),
                                vm_status,
                            );
                            },
                        // Ignore DelayedFields speculative errors as it can be intentionally triggered by parallel execution.
                        StatusCode::SPECULATIVE_EXECUTION_ABORT_ERROR => (),
                        // We will log the rest of invariant violation directly with regular logger as they shouldn't happen.
                        //
                        // TODO: Add different counters for the error categories here.
                        _ => {
                            error!(
                                *log_context,
                                "[aptos_vm] Transaction breaking invariant violation. txn: {:?}, status: {:?}",
                                bcs::to_bytes::<SignedTransaction>(txn),
                                vm_status,
                            );
                        },
                    }
                }

                // Increment the counter for user transactions executed.
                let counter_label = match output.status() {
                    TransactionStatus::Keep(_) => Some("success"),
                    TransactionStatus::Discard(_) => Some("discarded"),
                    TransactionStatus::Retry => None,
                };
                if let Some(label) = counter_label {
                    USER_TRANSACTIONS_EXECUTED.with_label_values(&[label]).inc();
                }
                (vm_status, output)
            },
            Transaction::StateCheckpoint(_) => {
                let status = TransactionStatus::Keep(ExecutionStatus::Success);
                let output = VMOutput::empty_with_status(status);
                (VMStatus::Executed, output)
            },
            Transaction::ValidatorTransaction(txn) => {
                let (vm_status, output) =
                    self.process_validator_transaction(resolver, txn.clone(), log_context)?;
                (vm_status, output)
            },
        })
    }

    pub fn get_required_deposit(
        &self,
        session: &mut SessionExt,
        resolver: &impl AptosMoveResolver,
        txn_gas_params: &TransactionGasParameters,
        txn_metadata: &TransactionMetadata,
        payload: &TransactionPayload,
    ) -> Option<u64> {
        match payload {
            TransactionPayload::EntryFunction(entry_func) => {
                if !self.randomness_enabled {
                    return None;
                }
                if has_randomness_attribute(resolver, session, entry_func).unwrap_or(false) {
                    let max_execution_gas: Gas = txn_gas_params
                        .max_execution_gas
                        .to_unit_round_up_with_params(txn_gas_params);
                    let max_io_gas: Gas = txn_gas_params
                        .max_io_gas
                        .to_unit_round_up_with_params(txn_gas_params);
                    let required_fee_deposit = min(
                        txn_metadata.gas_unit_price * (max_execution_gas + max_io_gas)
                            + txn_gas_params.max_storage_fee,
                        txn_metadata.gas_unit_price * txn_gas_params.maximum_number_of_gas_units,
                    );
                    Some(u64::from(required_fee_deposit))
                } else {
                    None
                }
            },
            TransactionPayload::Script(_)
            | TransactionPayload::ModuleBundle(_)
            | TransactionPayload::Multisig(_) => None,
        }
    }
}

// Executor external API
impl VMExecutor for AptosVM {
    /// Execute a block of `transactions`. The output vector will have the exact same length as the
    /// input vector. The discarded transactions will be marked as `TransactionStatus::Discard` and
    /// have an empty `WriteSet`. Also `state_view` is immutable, and does not have interior
    /// mutability. Writes to be applied to the data view are encoded in the write set part of a
    /// transaction output.
    fn execute_block(
        transactions: &[SignatureVerifiedTransaction],
        state_view: &(impl StateView + Sync),
        onchain_config: BlockExecutorConfigFromOnchain,
    ) -> Result<BlockOutput<TransactionOutput>, VMStatus> {
        fail_point!("move_adapter::execute_block", |_| {
            Err(VMStatus::error(
                StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR,
                None,
            ))
        });
        let log_context = AdapterLogSchema::new(state_view.id(), 0);
        info!(
            log_context,
            "Executing block, transaction count: {}",
            transactions.len()
        );

        let count = transactions.len();
        let ret = BlockAptosVM::execute_block::<
            _,
            NoOpTransactionCommitHook<AptosTransactionOutput, VMStatus>,
        >(
            Arc::clone(&RAYON_EXEC_POOL),
            transactions,
            state_view,
            BlockExecutorConfig {
                local: BlockExecutorLocalConfig {
                    concurrency_level: Self::get_concurrency_level(),
                    allow_fallback: true,
                    discard_failed_blocks: Self::get_discard_failed_blocks(),
                },
                onchain: onchain_config,
            },
            None,
        );
        if ret.is_ok() {
            // Record the histogram count for transactions per block.
            BLOCK_TRANSACTION_COUNT.observe(count as f64);
        }
        ret
    }

    fn execute_block_sharded<S: StateView + Sync + Send + 'static, C: ExecutorClient<S>>(
        sharded_block_executor: &ShardedBlockExecutor<S, C>,
        transactions: PartitionedTransactions,
        state_view: Arc<S>,
        onchain_config: BlockExecutorConfigFromOnchain,
    ) -> Result<Vec<TransactionOutput>, VMStatus> {
        let log_context = AdapterLogSchema::new(state_view.id(), 0);
        info!(
            log_context,
            "Executing block, transaction count: {}",
            transactions.num_txns()
        );

        let count = transactions.num_txns();
        let ret = sharded_block_executor.execute_block(
            state_view,
            transactions,
            AptosVM::get_concurrency_level(),
            onchain_config,
        );
        if ret.is_ok() {
            // Record the histogram count for transactions per block.
            BLOCK_TRANSACTION_COUNT.observe(count as f64);
        }
        ret
    }
}

// VMValidator external API
impl VMValidator for AptosVM {
    /// Determine if a transaction is valid. Will return `None` if the transaction is accepted,
    /// `Some(Err)` if the VM rejects it, with `Err` as an error code. Verification performs the
    /// following steps:
    /// 1. The signature on the `SignedTransaction` matches the public key included in the
    ///    transaction
    /// 2. The script to be executed is under given specific configuration.
    /// 3. Invokes `Account.prologue`, which checks properties such as the transaction has the
    /// right sequence number and the sender has enough balance to pay for the gas.
    /// TBD:
    /// 1. Transaction arguments matches the main function's type signature.
    ///    We don't check this item for now and would execute the check at execution time.
    fn validate_transaction(
        &self,
        transaction: SignedTransaction,
        state_view: &impl StateView,
    ) -> VMValidatorResult {
        let _timer = TXN_VALIDATION_SECONDS.start_timer();
        let log_context = AdapterLogSchema::new(state_view.id(), 0);

        if !self
            .features()
            .is_enabled(FeatureFlag::SINGLE_SENDER_AUTHENTICATOR)
        {
            if let aptos_types::transaction::authenticator::TransactionAuthenticator::SingleSender{ .. } = transaction.authenticator_ref() {
                return VMValidatorResult::error(StatusCode::FEATURE_UNDER_GATING);
            }
        }

        if !self.features().is_enabled(FeatureFlag::WEBAUTHN_SIGNATURE) {
            if let Ok(sk_authenticators) = transaction
                .authenticator_ref()
                .to_single_key_authenticators()
            {
                for authenticator in sk_authenticators {
                    if let AnySignature::WebAuthn { .. } = authenticator.signature() {
                        return VMValidatorResult::error(StatusCode::FEATURE_UNDER_GATING);
                    }
                }
            } else {
                return VMValidatorResult::error(StatusCode::INVALID_SIGNATURE);
            }
        }

        let txn = match transaction.check_signature() {
            Ok(t) => t,
            _ => {
                return VMValidatorResult::error(StatusCode::INVALID_SIGNATURE);
            },
        };
        let mut txn_data = TransactionMetadata::new(&txn);

        let resolver = self.as_move_resolver(&state_view);
        let mut session = self.new_session(&resolver, SessionId::prologue_meta(&txn_data));
        let gas_meter =
            match self.make_standard_gas_meter(txn.max_gas_amount().into(), &log_context) {
                Ok(gas_meter) => gas_meter,
                Err(_) => {
                    return VMValidatorResult::error(StatusCode::UNKNOWN_VALIDATION_STATUS);
                    //TODO: what's a better code?
                },
            };

        // Increment the counter for transactions verified.
        let required_deposit = self.get_required_deposit(
            &mut session,
            &resolver,
            &gas_meter.vm_gas_params().txn,
            &txn_data,
            txn.payload(),
        );
        txn_data.set_required_deposit(required_deposit);
        let (counter_label, result) = match self.validate_signed_transaction(
            &mut session,
            &resolver,
            &txn,
            &txn_data,
            &log_context,
        ) {
            Err(err) if err.status_code() != StatusCode::SEQUENCE_NUMBER_TOO_NEW => (
                "failure",
                VMValidatorResult::new(Some(err.status_code()), 0),
            ),
            _ => (
                "success",
                VMValidatorResult::new(None, txn.gas_unit_price()),
            ),
        };

        TRANSACTIONS_VALIDATED
            .with_label_values(&[counter_label])
            .inc();

        result
    }
}

// Ensure encapsulation of AptosVM APIs by using a wrapper.
pub struct AptosSimulationVM(AptosVM);

impl AptosSimulationVM {
    pub fn new(resolver: &impl AptosMoveResolver) -> Self {
        let mut vm = AptosVM::new(
            resolver,
            /*override_is_delayed_field_optimization_capable=*/ Some(false),
        );
        vm.is_simulation = true;
        Self(vm)
    }

    /// Simulates a signed transaction (i.e., executes it without performing
    /// signature verification) on a newly created VM instance.
    /// *Precondition:* the transaction must **not** have a valid signature.
    pub fn create_vm_and_simulate_signed_transaction(
        transaction: &SignedTransaction,
        state_view: &impl StateView,
    ) -> (VMStatus, TransactionOutput) {
        assert_err!(
            transaction.verify_signature(),
            "Simulated transaction should not have a valid signature"
        );

        let resolver = state_view.as_move_resolver();
        let vm = Self::new(&resolver);
        let log_context = AdapterLogSchema::new(state_view.id(), 0);

        let (vm_status, vm_output) =
            vm.0.execute_user_transaction(&resolver, transaction, &log_context);
        let txn_output = vm_output
            .try_materialize_into_transaction_output(&resolver)
            .expect("Materializing aggregator V1 deltas should never fail");
        (vm_status, txn_output)
    }
}

fn create_account_if_does_not_exist(
    session: &mut SessionExt,
    gas_meter: &mut impl GasMeter,
    account: AccountAddress,
) -> VMResult<()> {
    session
        .execute_function_bypass_visibility(
            &ACCOUNT_MODULE,
            CREATE_ACCOUNT_IF_DOES_NOT_EXIST,
            vec![],
            serialize_values(&vec![MoveValue::Address(account)]),
            gas_meter,
        )
        .map(|_return_vals| ())
}

/// Signals that the transaction should trigger the flow for creating an account as part of a
/// sponsored transaction. This occurs when:
/// * The feature gate is enabled SPONSORED_AUTOMATIC_ACCOUNT_V1_CREATION
/// * There is fee payer
/// * The sequence number is 0
/// * There is no account resource for the account
pub(crate) fn is_account_init_for_sponsored_transaction(
    txn_data: &TransactionMetadata,
    features: &Features,
    resolver: &impl AptosMoveResolver,
) -> VMResult<bool> {
    Ok(
        features.is_enabled(FeatureFlag::SPONSORED_AUTOMATIC_ACCOUNT_V1_CREATION)
            && txn_data.fee_payer.is_some()
            && txn_data.sequence_number == 0
            && resolver
                .get_resource(&txn_data.sender(), &AccountResource::struct_tag())
                .map(|data| data.is_none())
                .map_err(|e| {
                    PartialVMError::new(StatusCode::UNKNOWN_INVARIANT_VIOLATION_ERROR)
                        .with_message(format!("{}", e))
                        .finish(Location::Undefined)
                })?,
    )
}

#[test]
fn vm_thread_safe() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<AptosVM>();
    assert_sync::<AptosVM>();
    assert_send::<MoveVmExt>();
    assert_sync::<MoveVmExt>();
}
