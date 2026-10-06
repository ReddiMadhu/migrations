"""Workbench composition — create_workbench wires all services in dependency order."""

from __future__ import annotations

import os
from typing import Any

from sas2pyspark.core.errors import WorkbenchError, WorkbenchErrorCode
from sas2pyspark.services.clock import Clock, SystemClock, FixedClock
from sas2pyspark.services.evidence import EvidenceService
from sas2pyspark.services.policy import PolicyService, ExecutionPolicy, DEFAULT_EXECUTION_POLICY
from sas2pyspark.services.scopes import ScopeService
from sas2pyspark.services.artifacts import FileSystemArtifactService
from sas2pyspark.services.approvals import ApprovalService
from sas2pyspark.services.guards import GuardService
from sas2pyspark.services.baseline_guards import register_baseline_guards
from sas2pyspark.services.execution import ExecutionService
from sas2pyspark.services.reconciliation import ReconciliationService
from sas2pyspark.services.reconciler import InMemoryReconciler
from sas2pyspark.services.work_units import WorkUnitService
from sas2pyspark.services.workspace_discovery import WorkspaceDiscoveryService
from sas2pyspark.services.semantic_ir import SemanticIrService
from sas2pyspark.services.macro_resolution import MacroResolutionService
from sas2pyspark.services.model_context import ModelContextService
from sas2pyspark.services.executors import DummySparkExecutor, ReferenceCsvSasExecutor
from sas2pyspark.session.persistence import JsonlSessionPersistence
from sas2pyspark.session.service import MigrationSessionService
from sas2pyspark.core.logging import TokenBudgetPolicy, TokenMeter, configure_logging
from sas2pyspark.graph.checkpointer import resolve_checkpointer


class WorkbenchConfig:
    __slots__ = ("root_dir", "clock", "base_policy", "token_budget_policy", "dummy_mode", "translator", "spark_executor", "reconciler", "repairer", "checkpointer", "checkpointer_mode", "checkpoint_path", "environment", "model_provider", "model_name", "model_kwargs", "llm_cache", "file_edit_seam", "tool_registry", "agent_loop", "observation_cache", "projection_cache", "prompt_assembler", "sandbox_policy", "persistence_backend")

    def __init__(self, *, root_dir: str, clock: Clock | None = None, base_policy: ExecutionPolicy | None = None, token_budget_policy: TokenBudgetPolicy | None = None, dummy_mode: str = "success", translator: Any = None, spark_executor: Any = None, reconciler: Any = None, repairer: Any = None, checkpointer: Any = None, checkpointer_mode: str = "development", checkpoint_path: str | None = None, environment: str = "development", model_provider: str | None = None, model_name: str | None = None, model_kwargs: dict[str, Any] | None = None, llm_cache: Any = None, file_edit_seam: Any = None, tool_registry: Any = None, agent_loop: Any = None, observation_cache: Any = None, projection_cache: Any = None, prompt_assembler: Any = None, sandbox_policy: Any = None, persistence_backend: str | None = None) -> None:
        self.root_dir = root_dir
        self.clock = clock
        self.base_policy = base_policy
        self.token_budget_policy = token_budget_policy
        self.dummy_mode = dummy_mode
        self.translator = translator
        self.spark_executor = spark_executor
        self.reconciler = reconciler
        self.repairer = repairer
        self.checkpointer = checkpointer
        self.checkpointer_mode = checkpointer_mode
        self.checkpoint_path = checkpoint_path
        self.environment = environment
        self.model_provider = model_provider
        self.model_name = model_name
        self.model_kwargs = model_kwargs
        self.llm_cache = llm_cache
        self.file_edit_seam = file_edit_seam
        self.tool_registry = tool_registry
        self.agent_loop = agent_loop
        self.observation_cache = observation_cache
        self.projection_cache = projection_cache
        self.prompt_assembler = prompt_assembler
        self.sandbox_policy = sandbox_policy
        self.persistence_backend = persistence_backend


class WorkbenchRuntime:
    """Composed workbench runtime — all services wired and ready."""

    __slots__ = (
        "root_dir", "clock", "evidence", "policy", "scopes", "artifacts",
        "approvals", "guards", "execution", "reconciliation", "work_units",
        "workspace_discovery", "semantic_ir", "macro_resolution", "model_context",
        "sessions", "persistence", "dummy_spark", "reference_sas",
        "translator",
        "spark_executor",
        "token_budget_policy",
        "reconciler",
        "repairer",
        "checkpointer",
        "checkpointer_mode",
        "checkpoint_path",
        "environment",
        "token_meter",
        "model_provider",
        "model_name",
        "model_kwargs",
        "llm_cache",
        "file_edit_seam",
        "tool_registry",
        "agent_loop",
        "observation_cache",
        "projection_cache",
        "prompt_assembler",
        "sandbox_policy",
        "_disposed",
    )

    def __init__(self) -> None:
        self._disposed = False

    @property
    def disposed(self) -> bool:
        return self._disposed

    async def dispose(self) -> None:
        if self._disposed:
            return
        self._disposed = True
        if hasattr(self, "scopes"):
            self.scopes.dispose_all()
        if hasattr(self, "persistence"):
            self.persistence.dispose()
            prov = getattr(self.persistence, "_provider", None)
            if prov is not None and hasattr(prov, "adispose"):
                try:
                    await prov.adispose()
                except Exception:
                    pass
        # Clean up Cosmos client provider if the checkpointer is Cosmos-backed
        if hasattr(self, "checkpointer"):
            cp = self.checkpointer
            if hasattr(cp, "_provider") and hasattr(cp._provider, "adispose") and getattr(cp, "_provider", None) != getattr(getattr(self, "persistence", None), "_provider", None):
                try:
                    await cp._provider.adispose()
                except Exception:
                    pass


def _local_policy(root_dir: str) -> ExecutionPolicy:
    return ExecutionPolicy(
        version="local-milestone-v1",
        network="none",
        allowedHosts=(),
        allowedVolumePrefixes=(root_dir,),
        maximumRuntimeMs=15 * 60 * 1000,
        maximumCostUnits=10,
        maximumDataClassification="internal",
        approvalRequired=False,
        allowedActions=("spark.submit", "sas.execute"),
    )


async def create_workbench(config: WorkbenchConfig) -> WorkbenchRuntime:
    """Compose the workbench runtime with services in dependency order."""
    configure_logging()
    root_dir = os.path.abspath(config.root_dir)
    os.makedirs(root_dir, exist_ok=True)
    os.makedirs(os.path.join(root_dir, "sessions"), exist_ok=True)
    os.makedirs(os.path.join(root_dir, "tenants"), exist_ok=True)

    runtime = WorkbenchRuntime()
    runtime.root_dir = root_dir
    if config.environment == "production":
        if isinstance(config.clock, FixedClock):
            raise WorkbenchError(WorkbenchErrorCode.CONFIG_ERROR, "production requires SystemClock; FixedClock is non-deterministic")
        runtime.clock = config.clock or SystemClock()
    else:
        runtime.clock = config.clock or SystemClock()
    runtime.evidence = EvidenceService()
    runtime.policy = PolicyService()
    runtime.scopes = ScopeService(root_dir, config.base_policy or _local_policy(root_dir))
    runtime.artifacts = FileSystemArtifactService(root_dir, runtime.clock, runtime.scopes)
    runtime.approvals = ApprovalService(runtime.clock)
    runtime.guards = GuardService()
    runtime.work_units = WorkUnitService(runtime.clock, runtime.scopes)
    runtime.workspace_discovery = WorkspaceDiscoveryService()
    runtime.semantic_ir = SemanticIrService()
    runtime.macro_resolution = MacroResolutionService()
    persistence_backend = (config.persistence_backend or os.environ.get("SAS2PYSPARK_PERSISTENCE_BACKEND", "jsonl")).lower()
    if persistence_backend in ("cosmos", "azure_cosmos"):
        from sas2pyspark.graph.cosmos_client_provider import CosmosClientProvider
        from sas2pyspark.session.cosmos_persistence import CosmosDbSessionPersistence

        auto_create = config.environment != "production"
        cosmos_provider = CosmosClientProvider(auto_create=auto_create)
        runtime.persistence = CosmosDbSessionPersistence(cosmos_provider)
    else:
        runtime.persistence = JsonlSessionPersistence(root_dir)
    runtime.sessions = MigrationSessionService(runtime.persistence, runtime.evidence, runtime.scopes, runtime.clock, runtime.artifacts)
    runtime.execution = ExecutionService(runtime.scopes, runtime.policy, runtime.approvals, runtime.guards)
    runtime.reconciliation = ReconciliationService(runtime.artifacts, runtime.clock)
    register_baseline_guards(runtime.guards, runtime.artifacts)
    runtime.model_context = ModelContextService(runtime.artifacts, runtime.scopes, runtime.clock, runtime.work_units)
    runtime.dummy_spark = DummySparkExecutor(runtime.artifacts, mode=config.dummy_mode)
    runtime.reference_sas = ReferenceCsvSasExecutor(runtime.artifacts)
    runtime.translator = config.translator
    runtime.spark_executor = config.spark_executor
    if runtime.spark_executor is None:
        databricks_host = os.environ.get("DATABRICKS_HOST")
        databricks_token = os.environ.get("DATABRICKS_TOKEN")
        databricks_job_id = os.environ.get("DATABRICKS_JOB_ID")
        if databricks_host and databricks_token and databricks_job_id:
            from sas2pyspark.providers.databricks import DatabricksSparkExecutor
            from sas2pyspark.providers.secret_resolver import EnvSecretResolver

            catalog = os.environ.get("DATABRICKS_CATALOG", "main")
            schema = os.environ.get("DATABRICKS_SCHEMA", "migration")
            volume = os.environ.get("DATABRICKS_VOLUME", "artifacts")

            def _artifact_uri(artifact: Any) -> str:
                return f"/Volumes/{catalog}/{schema}/{volume}/{artifact.artifact_id}"

            def _input_uri(snapshot_id: str) -> str:
                return f"/Volumes/{catalog}/{schema}/{volume}/inputs/{snapshot_id}"

            def _output_materializer(response: Any, scope: Any) -> list:
                from sas2pyspark.providers.databricks_materializer import make_output_materializer
                materializer = make_output_materializer(
                    workspace_client=runtime.spark_executor._client,
                    artifacts=runtime.artifacts,
                    output_volume_path=f"/Volumes/{catalog}/{schema}/{volume}/outputs",
                )
                return materializer(response, scope)

            runtime.spark_executor = DatabricksSparkExecutor.from_secret_resolver(
                secret_resolver=EnvSecretResolver(),
                credential_ref="DATABRICKS_TOKEN",
                host=databricks_host,
                job_id=int(databricks_job_id),
                artifact_uri=_artifact_uri,
                input_uri=_input_uri,
                output_materializer=_output_materializer,
            )
    runtime.reconciler = config.reconciler or InMemoryReconciler(runtime.reconciliation, runtime.scopes)
    runtime.repairer = config.repairer
    runtime.checkpointer_mode = config.checkpointer_mode
    runtime.checkpoint_path = config.checkpoint_path or (
        os.path.join(root_dir, "sessions", "migration-checkpoints.sqlite")
        if config.checkpointer_mode.lower() == "production"
        else None
    )
    runtime.environment = config.environment
    runtime.checkpointer = config.checkpointer or resolve_checkpointer(
        mode=runtime.checkpointer_mode,
        checkpoint_path=runtime.checkpoint_path,
        environment=config.environment,
        blob_store=runtime.artifacts.blob_store if hasattr(runtime, "artifacts") else None,
    )
    runtime.token_budget_policy = config.token_budget_policy or TokenBudgetPolicy()
    runtime.token_meter = TokenMeter()
    runtime.model_provider = config.model_provider
    runtime.model_name = config.model_name
    runtime.model_kwargs = config.model_kwargs

    if config.translator is not None:
        runtime.translator = config.translator
    elif config.model_provider is not None and config.model_name is not None:
        from sas2pyspark.llm.translation import create_sas_translator
        runtime.translator = create_sas_translator(
            model_provider=config.model_provider,
            model_name=config.model_name,
            runtime=runtime,
            model_kwargs=config.model_kwargs,
        )
    else:
        runtime.translator = None

    runtime.observation_cache = config.observation_cache
    if runtime.observation_cache is None:
        from sas2pyspark.llm.cache.observation_cache import ObservationCache
        runtime.observation_cache = ObservationCache(capacity=5)

    runtime.projection_cache = config.projection_cache
    if runtime.projection_cache is None:
        from sas2pyspark.llm.cache.projection_cache import ProjectionCache
        runtime.projection_cache = ProjectionCache(
            root_dir=os.path.join(root_dir, "projection-cache"),
            clock=runtime.clock,
            format_version=1,
        )

    runtime.llm_cache = config.llm_cache
    if runtime.llm_cache is None:
        from sas2pyspark.llm.cache.example_cache import ContentAddressedExampleCache
        runtime.llm_cache = ContentAddressedExampleCache(
            root_dir=os.path.join(root_dir, "llm-cache"),
            clock=runtime.clock,
        )

    runtime.sandbox_policy = config.sandbox_policy

    runtime.file_edit_seam = config.file_edit_seam

    runtime.tool_registry = config.tool_registry
    if runtime.tool_registry is None:
        from sas2pyspark.agent.registry import ScopedToolRegistry
        runtime.tool_registry = ScopedToolRegistry()

    runtime.prompt_assembler = config.prompt_assembler
    if runtime.prompt_assembler is None:
        from sas2pyspark.agent.prompt_assembler import PromptAssembler
        runtime.prompt_assembler = PromptAssembler()

    runtime.agent_loop = config.agent_loop
    if runtime.agent_loop is None:
        from sas2pyspark.agent.driver import AgentDriver, StepRunner, TurnRunner
        from sas2pyspark.agent.dispatch import ToolDispatchPipeline
        dispatch = ToolDispatchPipeline(
            registry=runtime.tool_registry,
            guards=runtime.guards,
            approvals=runtime.approvals,
            sessions=runtime.sessions,
            clock=runtime.clock,
        )
        step_runner = StepRunner(dispatch=dispatch)
        turn_runner = TurnRunner(step_runner=step_runner)
        runtime.agent_loop = AgentDriver(
            turn_runner=turn_runner,
            sessions=runtime.sessions,
            clock=runtime.clock,
            token_meter=runtime.token_meter,
            budget_policy=runtime.token_budget_policy,
        )

    if config.repairer is not None:
        runtime.repairer = config.repairer
    elif runtime.repairer is None:
        from sas2pyspark.agent.repairer import AgentLoopRepairer
        runtime.repairer = AgentLoopRepairer(
            driver=runtime.agent_loop,
            prompt_assembler=runtime.prompt_assembler,
            translator=runtime.translator,
        )

    return runtime
