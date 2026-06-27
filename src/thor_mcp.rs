//! MCP bridge exposed to the ACP host running Thor.

use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::time::Duration;

use agent_client_protocol::schema::v1::{
    EnvVariable, McpServer, McpServerStdio, PermissionOptionKind, SessionUpdate, StopReason,
    ToolCall, ToolCallStatus, ToolCallUpdate, ToolKind, Usage, UsageUpdate,
};
use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use tokio::sync::mpsc;

use crate::acp::{self, AcpRuntimeConfig};
use crate::config::{self, Config, SelectedAgent};
use crate::event::{PermissionDecision, UiCommand, UiEvent, content_block_text};
use crate::thor;
use crate::thor_catalog::{self, CatalogRequest};
use crate::thor_probe::{self, AgentValidation, QuotaSnapshot};

const DEFAULT_WORKER_TIMEOUT: Duration = Duration::from_secs(900);
const MAX_WORKER_TIMEOUT_SECONDS: u64 = 7_200;
const THOR_PROGRESS_PATH_ENV: &str = "MJ_THOR_PROGRESS";

#[derive(Debug, Deserialize)]
struct RpcRequest {
    id: Option<Value>,
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Serialize)]
struct RpcResponse<'a> {
    jsonrpc: &'static str,
    id: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<RpcError<'a>>,
}

#[derive(Debug, Serialize)]
struct RpcError<'a> {
    code: i64,
    message: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    data: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ToolCallParams {
    name: String,
    #[serde(default)]
    arguments: Value,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListAgentsArgs {
    #[serde(default)]
    refresh_quota: bool,
    #[serde(default)]
    validate: bool,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowState {
    facts: WorkflowFacts,
    plan: Option<WorkflowPlan>,
    completed_phases: Vec<WorkflowPhase>,
}

#[derive(Debug, Default, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowFacts {
    listed_workers: bool,
    refreshed_quota: bool,
    validated_workers: bool,
    loaded_model_catalog: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum WorkflowPhase {
    Implementation,
    Review,
    Correction,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum ThorTaskComplexity {
    Simple,
    Hard,
    Uncertain,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkflowPlan {
    complexity: ThorTaskComplexity,
    strategy: String,
    rationale: String,
    implementation_jobs: Vec<PlannedJob>,
    review_jobs: Vec<PlannedJob>,
    correction_jobs: Vec<PlannedJob>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PlannedJob {
    id: String,
    source_id: String,
    prompt: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model: Option<String>,
    purpose: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunAgentArgs {
    job_id: String,
    phase: WorkflowPhase,
    source_id: String,
    prompt: String,
    cwd: Option<PathBuf>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    permission_mode: BridgePermissionMode,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunAgentBatchArgs {
    jobs: Vec<RunAgentJob>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RunAgentJob {
    id: String,
    phase: WorkflowPhase,
    source_id: String,
    prompt: String,
    cwd: Option<PathBuf>,
    #[serde(default)]
    timeout_seconds: Option<u64>,
    #[serde(default)]
    permission_mode: BridgePermissionMode,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BridgePermissionMode {
    #[default]
    Reject,
    AcceptEdits,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct AgentSummary {
    source_id: String,
    name: String,
    command: String,
    args: Vec<String>,
    quota_backend: config::ThorQuotaBackend,
    quota: Vec<QuotaSnapshot>,
    validation: Option<AgentValidation>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DelegatedRunResult {
    job_id: Option<String>,
    source_id: String,
    final_text: String,
    text: String,
    stop_reason: String,
    usage: Option<Usage>,
    context_usage: Option<ContextUsageSummary>,
    quota: Vec<QuotaSnapshot>,
    tool_calls: Vec<ToolSummary>,
    progress: Vec<ProgressEvent>,
    permissions: Vec<String>,
    error: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchRunResult {
    jobs: Vec<DelegatedRunResult>,
    aggregate_usage: UsageAggregate,
    progress: Vec<BatchProgressEvent>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct UsageAggregate {
    jobs: usize,
    jobs_with_usage: usize,
    total_tokens: u64,
    input_tokens: u64,
    output_tokens: u64,
    thought_tokens: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ContextUsageSummary {
    used: u64,
    size: u64,
    cost: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ToolSummary {
    id: String,
    title: String,
    kind: Option<String>,
    status: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProgressEvent {
    sequence: usize,
    kind: String,
    detail: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchProgressEvent {
    sequence: usize,
    job_id: Option<String>,
    source_id: String,
    kind: String,
    detail: String,
}

impl WorkflowState {
    fn facts_ready(&self) -> bool {
        self.facts.listed_workers
            && self.facts.refreshed_quota
            && self.facts.validated_workers
            && self.facts.loaded_model_catalog
    }

    fn next_phase(&self) -> Option<WorkflowPhase> {
        [
            WorkflowPhase::Implementation,
            WorkflowPhase::Review,
            WorkflowPhase::Correction,
        ]
        .into_iter()
        .find(|phase| !self.completed_phases.contains(phase))
    }

    fn planned_jobs(&self, phase: WorkflowPhase) -> Result<&[PlannedJob]> {
        let plan = self
            .plan
            .as_ref()
            .ok_or_else(|| anyhow!("submit a valid Thor plan before running workers"))?;
        Ok(match phase {
            WorkflowPhase::Implementation => &plan.implementation_jobs,
            WorkflowPhase::Review => &plan.review_jobs,
            WorkflowPhase::Correction => &plan.correction_jobs,
        })
    }

    fn complete_phase(&mut self, phase: WorkflowPhase) {
        if !self.completed_phases.contains(&phase) {
            self.completed_phases.push(phase);
        }
    }
}

pub fn mcp_servers(config_path: PathBuf) -> Result<Vec<McpServer>> {
    mcp_servers_with_progress(config_path, None)
}

pub fn mcp_servers_with_progress(
    config_path: PathBuf,
    progress_path: Option<PathBuf>,
) -> Result<Vec<McpServer>> {
    Ok(vec![stdio_mcp_server(
        std::env::current_exe().context("resolve current mj executable")?,
        config_path,
        progress_path,
    )])
}

fn stdio_mcp_server(
    command: PathBuf,
    config_path: PathBuf,
    progress_path: Option<PathBuf>,
) -> McpServer {
    let mut env = vec![EnvVariable::new(
        "MJ_THOR_CONFIG",
        config_path.to_string_lossy().into_owned(),
    )];
    if let Some(progress_path) = progress_path {
        env.push(EnvVariable::new(
            THOR_PROGRESS_PATH_ENV,
            progress_path.to_string_lossy().into_owned(),
        ));
    }
    McpServer::Stdio(
        McpServerStdio::new(thor::THOR_MCP_SERVER_NAME, command)
            .args(vec!["thor-mcp".to_string()])
            .env(env),
    )
}

pub async fn run_stdio() -> Result<()> {
    let stdin = std::io::stdin();
    let mut reader = std::io::BufReader::new(stdin.lock());
    let stdout = std::io::stdout();
    let mut writer = stdout.lock();
    let mut workflow = WorkflowState::default();

    while let Some(message) = read_message(&mut reader)? {
        let request: RpcRequest = match serde_json::from_slice(&message) {
            Ok(request) => request,
            Err(error) => {
                let response = RpcResponse {
                    jsonrpc: "2.0",
                    id: Value::Null,
                    result: None,
                    error: Some(RpcError {
                        code: -32700,
                        message: "parse error",
                        data: Some(Value::String(error.to_string())),
                    }),
                };
                write_message(&mut writer, &serde_json::to_vec(&response)?)?;
                continue;
            }
        };
        let Some(id) = request.id.clone() else {
            continue;
        };
        let response = match handle_request_with_config(request, None, &mut workflow).await {
            Ok(result) => RpcResponse {
                jsonrpc: "2.0",
                id,
                result: Some(result),
                error: None,
            },
            Err(error) => RpcResponse {
                jsonrpc: "2.0",
                id,
                result: None,
                error: Some(RpcError {
                    code: -32000,
                    message: "thor MCP bridge error",
                    data: Some(Value::String(error.to_string())),
                }),
            },
        };
        write_message(&mut writer, &serde_json::to_vec(&response)?)?;
    }
    Ok(())
}

async fn handle_request_with_config(
    request: RpcRequest,
    config_path: Option<PathBuf>,
    workflow: &mut WorkflowState,
) -> Result<Value> {
    match request.method.as_str() {
        "initialize" => Ok(json!({
            "protocolVersion": "2024-11-05",
            "capabilities": { "tools": {} },
            "serverInfo": {
                "name": thor::THOR_MCP_SERVER_NAME,
                "version": env!("CARGO_PKG_VERSION"),
            },
        })),
        "tools/list" => Ok(json!({ "tools": tool_definitions() })),
        "tools/call" => {
            let params: ToolCallParams = serde_json::from_value(request.params)?;
            call_tool(params, config_path, workflow).await
        }
        method => bail!("unsupported MCP method {method}"),
    }
}

fn tool_definitions() -> Vec<Value> {
    vec![
        json!({
            "name": "thor_get_workflow_state",
            "description": "Return the Rust-enforced Thor workflow state: gathered facts, accepted plan, completed phase list, and next allowed phase.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_list_acp_agents",
            "description": "List ACP agents mj can launch as Thor workers, including cached direct provider quota signals when available.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "refreshQuota": {
                        "type": "boolean",
                        "description": "When true, actively refresh quota through direct Claude Code /usage and Codex appserver account/rateLimits/read queries before returning workers."
                    },
                    "validate": {
                        "type": "boolean",
                        "description": "When true, launch each worker and verify it completes ACP initialize plus session startup before returning workers."
                    }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_refresh_quota",
            "description": "Actively refresh quota/capacity hints for configured workers through direct provider queries: Claude Code /usage and Codex appserver account/rateLimits/read.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_validate_acp_agents",
            "description": "Launch configured ACP workers and report which ones initialize and open a session successfully.",
            "inputSchema": {
                "type": "object",
                "properties": {},
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_submit_plan",
            "description": "Submit Thor's structured plan after gathering facts. Rust validates worker IDs and requires implementation, adversarial review, and correction phases before worker execution is allowed.",
            "inputSchema": {
                "type": "object",
                "required": ["complexity", "strategy", "rationale", "implementationJobs", "reviewJobs", "correctionJobs"],
                "properties": {
                    "complexity": {
                        "type": "string",
                        "enum": ["simple", "hard", "uncertain"],
                        "description": "Thor's judgment of task complexity."
                    },
                    "strategy": { "type": "string" },
                    "rationale": { "type": "string" },
                    "implementationJobs": {
                        "type": "array",
                        "minItems": 1,
                        "items": { "$ref": "#/$defs/plannedJob" }
                    },
                    "reviewJobs": {
                        "type": "array",
                        "minItems": 1,
                        "items": { "$ref": "#/$defs/plannedJob" }
                    },
                    "correctionJobs": {
                        "type": "array",
                        "minItems": 1,
                        "items": { "$ref": "#/$defs/plannedJob" }
                    }
                },
                "$defs": {
                    "plannedJob": {
                        "type": "object",
                        "required": ["id", "sourceId", "prompt", "purpose"],
                        "properties": {
                            "id": { "type": "string" },
                            "sourceId": { "type": "string" },
                            "prompt": { "type": "string" },
                            "model": { "type": "string" },
                            "purpose": { "type": "string" }
                        },
                        "additionalProperties": false
                    }
                },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "thor_run_acp_agent",
            "description": "Run a prompt on one configured ACP worker and return final text, structured progress, tool calls, usage, and permission summary.",
            "inputSchema": {
                "type": "object",
                "required": ["jobId", "phase", "sourceId", "prompt"],
                "properties": {
                    "jobId": { "type": "string" },
                    "phase": {
                        "type": "string",
                        "enum": ["implementation", "review", "correction"]
                    },
                    "sourceId": { "type": "string" },
                    "prompt": { "type": "string" },
                    "cwd": { "type": "string" },
                    "timeoutSeconds": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 7200,
                        "description": "Overall deadline for the delegated worker run. Defaults to 900 seconds."
                    },
                    "permissionMode": {
                        "type": "string",
                        "enum": ["reject", "accept_edits"]
                    }
                }
            }
        }),
        json!({
            "name": "thor_run_acp_agents",
            "description": "Run multiple configured ACP worker prompts concurrently and return per-worker results plus aggregate usage/progress.",
            "inputSchema": {
                "type": "object",
                "required": ["jobs"],
                "properties": {
                    "jobs": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 8,
                        "items": {
                            "type": "object",
                            "required": ["id", "phase", "sourceId", "prompt"],
                            "properties": {
                                "id": { "type": "string" },
                                "phase": {
                                    "type": "string",
                                    "enum": ["implementation", "review", "correction"]
                                },
                                "sourceId": { "type": "string" },
                                "prompt": { "type": "string" },
                                "cwd": { "type": "string" },
                                "timeoutSeconds": {
                                    "type": "integer",
                                    "minimum": 1,
                                    "maximum": 7200,
                                    "description": "Overall deadline for this delegated worker run. Defaults to 900 seconds."
                                },
                                "permissionMode": {
                                    "type": "string",
                                    "enum": ["reject", "accept_edits"]
                                }
                            }
                        }
                    }
                }
            }
        }),
        json!({
            "name": "thor_get_model_catalog",
            "description": "Return Thor's cached model strength/pricing catalog, refreshing LM Arena/OpenRouter metadata when requested.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "refresh": { "type": "boolean" },
                    "maxAgeSeconds": { "type": "integer", "minimum": 0 }
                }
            }
        }),
    ]
}

async fn call_tool(
    params: ToolCallParams,
    config_path: Option<PathBuf>,
    workflow: &mut WorkflowState,
) -> Result<Value> {
    match params.name.as_str() {
        "thor_get_workflow_state" => Ok(tool_text_result(&serde_json::to_string_pretty(
            &workflow_status(workflow),
        )?)),
        "thor_list_acp_agents" => {
            let config = load_config(config_path.as_ref())?;
            let configured_workers = thor::configured_acp_servers(&config);
            let workers = thor::worker_catalog(&config);
            let args: ListAgentsArgs = if params.arguments.is_null() {
                ListAgentsArgs::default()
            } else {
                serde_json::from_value(params.arguments)?
            };
            let mut quota = thor_probe::load_quota_snapshots().unwrap_or_default();
            if args.refresh_quota {
                let refreshed =
                    thor_probe::refresh_configured_quota_snapshots(&configured_workers).await;
                if !refreshed.is_empty() {
                    quota = refreshed;
                }
            }
            let validations = if args.validate {
                let cwd = std::env::current_dir().context("current dir")?;
                thor_probe::validate_agents(&workers, cwd).await
            } else {
                Vec::new()
            };
            let agents = workers
                .into_iter()
                .map(|agent| AgentSummary {
                    name: configured_workers
                        .iter()
                        .find(|server| server.source_id == agent.source_id)
                        .map(|server| server.name.clone())
                        .unwrap_or_else(|| agent.source_id.clone()),
                    quota_backend: configured_workers
                        .iter()
                        .find(|server| server.source_id == agent.source_id)
                        .map(|server| server.quota_backend)
                        .unwrap_or(config::ThorQuotaBackend::None),
                    quota: quota
                        .iter()
                        .filter(|snapshot| snapshot.source_id == agent.source_id)
                        .cloned()
                        .collect(),
                    validation: validations
                        .iter()
                        .find(|validation| validation.source_id == agent.source_id)
                        .cloned(),
                    source_id: agent.source_id,
                    command: agent.program.to_string_lossy().into_owned(),
                    args: agent.args,
                })
                .collect::<Vec<_>>();
            workflow.facts.listed_workers = true;
            if args.refresh_quota {
                workflow.facts.refreshed_quota = true;
            }
            if args.validate {
                workflow.facts.validated_workers = true;
            }
            Ok(tool_text_result(&serde_json::to_string_pretty(&agents)?))
        }
        "thor_refresh_quota" => {
            let config = load_config(config_path.as_ref())?;
            let snapshots = thor_probe::refresh_configured_quota_snapshots(
                &thor::configured_acp_servers(&config),
            )
            .await;
            workflow.facts.refreshed_quota = true;
            Ok(tool_text_result(&serde_json::to_string_pretty(&snapshots)?))
        }
        "thor_validate_acp_agents" => {
            let config = load_config(config_path.as_ref())?;
            let cwd = std::env::current_dir().context("current dir")?;
            let validations =
                thor_probe::validate_agents(&thor::worker_catalog(&config), cwd).await;
            workflow.facts.validated_workers = true;
            Ok(tool_text_result(&serde_json::to_string_pretty(
                &validations,
            )?))
        }
        "thor_submit_plan" => {
            let config = load_config(config_path.as_ref())?;
            let plan: WorkflowPlan = serde_json::from_value(params.arguments)?;
            submit_plan(plan, &config, workflow)
        }
        "thor_run_acp_agent" => {
            let args: RunAgentArgs = serde_json::from_value(params.arguments)?;
            let phase = authorize_single_run(&args, workflow)?;
            let result = run_agent(args, config_path.as_ref()).await?;
            workflow.complete_phase(phase);
            Ok(tool_text_result(&serde_json::to_string_pretty(&result)?))
        }
        "thor_run_acp_agents" => {
            let args: RunAgentBatchArgs = serde_json::from_value(params.arguments)?;
            let phase = authorize_batch_run(&args, workflow)?;
            let result = run_agent_batch(args, config_path.as_ref()).await?;
            workflow.complete_phase(phase);
            Ok(tool_text_result(&serde_json::to_string_pretty(&result)?))
        }
        "thor_get_model_catalog" => {
            let config = load_config(config_path.as_ref())?;
            let request: CatalogRequest = if params.arguments.is_null() {
                CatalogRequest::default()
            } else {
                serde_json::from_value(params.arguments)?
            };
            let catalog = thor_catalog::load_or_refresh_catalog(&config.thor, request).await?;
            workflow.facts.loaded_model_catalog = true;
            Ok(tool_text_result(&serde_json::to_string_pretty(&catalog)?))
        }
        name => bail!("unknown Thor MCP tool {name}"),
    }
}

fn workflow_status(workflow: &WorkflowState) -> Value {
    json!({
        "facts": &workflow.facts,
        "planAccepted": workflow.plan.is_some(),
        "plan": &workflow.plan,
        "completedPhases": &workflow.completed_phases,
        "nextPhase": workflow.next_phase(),
    })
}

fn submit_plan(plan: WorkflowPlan, config: &Config, workflow: &mut WorkflowState) -> Result<Value> {
    if !workflow.facts_ready() {
        bail!(
            "gather Thor facts before submitting a plan: call thor_list_acp_agents with refreshQuota=true and validate=true, then thor_get_model_catalog"
        );
    }
    validate_plan(&plan, config)?;
    workflow.plan = Some(plan);
    workflow.completed_phases.clear();
    Ok(tool_text_result(&serde_json::to_string_pretty(
        &workflow_status(workflow),
    )?))
}

fn validate_plan(plan: &WorkflowPlan, config: &Config) -> Result<()> {
    if plan.strategy.trim().is_empty() {
        bail!("Thor plan strategy is required");
    }
    if plan.rationale.trim().is_empty() {
        bail!("Thor plan rationale is required");
    }

    let worker_sources = thor::worker_catalog(config)
        .into_iter()
        .map(|agent| agent.source_id)
        .collect::<HashSet<_>>();
    if worker_sources.is_empty() {
        bail!("no configured ACP workers are available for Thor");
    }

    let mut job_ids = HashSet::new();
    validate_phase_jobs(
        "implementation",
        &plan.implementation_jobs,
        &worker_sources,
        &mut job_ids,
    )?;
    validate_phase_jobs("review", &plan.review_jobs, &worker_sources, &mut job_ids)?;
    validate_phase_jobs(
        "correction",
        &plan.correction_jobs,
        &worker_sources,
        &mut job_ids,
    )?;

    let implementation_sources = plan
        .implementation_jobs
        .iter()
        .map(|job| job.source_id.as_str())
        .collect::<HashSet<_>>();
    let review_uses_different_worker = plan
        .review_jobs
        .iter()
        .any(|job| !implementation_sources.contains(job.source_id.as_str()));
    if worker_sources.len() > 1 && !review_uses_different_worker {
        bail!(
            "review phase must use a different ACP worker when more than one worker is available"
        );
    }

    Ok(())
}

fn validate_phase_jobs(
    phase: &str,
    jobs: &[PlannedJob],
    worker_sources: &HashSet<String>,
    job_ids: &mut HashSet<String>,
) -> Result<()> {
    if jobs.is_empty() {
        bail!("Thor plan must include at least one {phase} job");
    }
    for job in jobs {
        if job.id.trim().is_empty() {
            bail!("{phase} job id is required");
        }
        if !job_ids.insert(job.id.clone()) {
            bail!("duplicate Thor plan job id {}", job.id);
        }
        if !worker_sources.contains(&job.source_id) {
            bail!(
                "unknown ACP worker {} in {phase} job {}",
                job.source_id,
                job.id
            );
        }
        if job.prompt.trim().is_empty() {
            bail!("{phase} job {} prompt is required", job.id);
        }
        if job.purpose.trim().is_empty() {
            bail!("{phase} job {} purpose is required", job.id);
        }
    }
    Ok(())
}

fn authorize_single_run(args: &RunAgentArgs, workflow: &WorkflowState) -> Result<WorkflowPhase> {
    let phase = workflow.next_phase().ok_or_else(|| {
        anyhow!("Thor workflow is complete; submit a new plan before more worker runs")
    })?;
    if args.phase != phase {
        bail!(
            "next Thor phase is {:?}, but run requested {:?}",
            phase,
            args.phase
        );
    }
    let planned = workflow.planned_jobs(phase)?;
    if planned.len() != 1 {
        bail!(
            "phase {:?} has {} planned jobs; use thor_run_acp_agents for the whole phase",
            phase,
            planned.len()
        );
    }
    validate_run_matches_plan(&args.job_id, &args.source_id, &args.prompt, planned)?;
    Ok(phase)
}

fn authorize_batch_run(
    args: &RunAgentBatchArgs,
    workflow: &WorkflowState,
) -> Result<WorkflowPhase> {
    let phase = workflow.next_phase().ok_or_else(|| {
        anyhow!("Thor workflow is complete; submit a new plan before more worker runs")
    })?;
    if args.jobs.iter().any(|job| job.phase != phase) {
        bail!("all worker jobs must use next Thor phase {:?}", phase);
    }
    let planned = workflow.planned_jobs(phase)?;
    if args.jobs.len() != planned.len() {
        bail!(
            "phase {:?} requires exactly {} planned jobs, got {}",
            phase,
            planned.len(),
            args.jobs.len()
        );
    }
    for job in &args.jobs {
        validate_run_matches_plan(&job.id, &job.source_id, &job.prompt, planned)?;
    }
    Ok(phase)
}

fn validate_run_matches_plan(
    job_id: &str,
    source_id: &str,
    prompt: &str,
    planned: &[PlannedJob],
) -> Result<()> {
    let planned = planned
        .iter()
        .find(|job| job.id == job_id)
        .ok_or_else(|| anyhow!("worker job {job_id} is not in the accepted Thor plan"))?;
    if planned.source_id != source_id {
        bail!(
            "worker job {job_id} planned source {} but requested {}",
            planned.source_id,
            source_id
        );
    }
    if planned.prompt != prompt {
        bail!("worker job {job_id} prompt does not match the accepted Thor plan");
    }
    Ok(())
}

fn tool_text_result(text: &str) -> Value {
    json!({
        "content": [{ "type": "text", "text": text }],
        "isError": false
    })
}

fn load_config(config_path: Option<&PathBuf>) -> Result<Config> {
    let path = config_path.cloned().unwrap_or_else(|| {
        std::env::var_os("MJ_THOR_CONFIG")
            .map(PathBuf::from)
            .unwrap_or_else(config::default_config_path)
    });
    Config::load(&path).with_context(|| format!("load {}", path.display()))
}

async fn run_agent(
    args: RunAgentArgs,
    config_path: Option<&PathBuf>,
) -> Result<DelegatedRunResult> {
    if args.prompt.trim().is_empty() {
        bail!("empty delegated prompt");
    }
    let config = load_config(config_path)?;
    let agent = thor::worker_catalog(&config)
        .into_iter()
        .find(|agent| agent.source_id == args.source_id)
        .ok_or_else(|| anyhow!("unknown ACP agent {}", args.source_id))?;
    run_agent_prompt(agent, args).await
}

async fn run_agent_batch(
    args: RunAgentBatchArgs,
    config_path: Option<&PathBuf>,
) -> Result<BatchRunResult> {
    if args.jobs.is_empty() {
        bail!("empty delegated job list");
    }
    if args.jobs.len() > 8 {
        bail!("too many delegated jobs: max 8");
    }

    let config_path = config_path.cloned();
    let futures = args.jobs.into_iter().map(|job| {
        let config_path = config_path.clone();
        async move {
            let job_id = job.id.clone();
            let source_id = job.source_id.clone();
            let result = run_agent(job.into(), config_path.as_ref()).await;
            (job_id, source_id, result)
        }
    });
    let completed = futures::future::join_all(futures).await;

    let mut progress = Vec::new();
    let mut jobs = Vec::new();
    for (idx, (job_id, source_id, result)) in completed.into_iter().enumerate() {
        progress.push(BatchProgressEvent {
            sequence: idx + 1,
            job_id: Some(job_id.clone()),
            source_id: source_id.clone(),
            kind: "worker_finished".to_string(),
            detail: match &result {
                Ok(result) => format!("{} stopped with {}", result.source_id, result.stop_reason),
                Err(error) => format!("{source_id} failed: {error}"),
            },
        });
        match result {
            Ok(result) => jobs.push(result),
            Err(error) => jobs.push(DelegatedRunResult {
                job_id: Some(job_id),
                source_id,
                final_text: String::new(),
                text: String::new(),
                stop_reason: "error".to_string(),
                usage: None,
                context_usage: None,
                quota: Vec::new(),
                tool_calls: Vec::new(),
                progress: vec![ProgressEvent {
                    sequence: 1,
                    kind: "error".to_string(),
                    detail: error.to_string(),
                }],
                permissions: Vec::new(),
                error: Some(error.to_string()),
            }),
        }
    }

    Ok(BatchRunResult {
        aggregate_usage: aggregate_usage(&jobs),
        jobs,
        progress,
    })
}

async fn run_agent_prompt(agent: SelectedAgent, args: RunAgentArgs) -> Result<DelegatedRunResult> {
    let cwd = delegated_cwd(args.cwd.as_ref())?;
    let worker_timeout = worker_timeout(args.timeout_seconds);
    let deadline = tokio::time::Instant::now() + worker_timeout;
    let (event_tx, mut event_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();
    let source_id = agent.source_id.clone();
    let runtime_cfg = AcpRuntimeConfig {
        command: agent.program,
        args: agent.args,
        cwd,
        additional_directories: Vec::new(),
        mcp_servers: Vec::new(),
        resume_session: None,
        env: agent.env,
        agent_stderr: None,
        fs_max_text_bytes: acp::DEFAULT_FS_TEXT_BYTES,
    };
    let runtime = tokio::spawn(acp::run(runtime_cfg, event_tx, cmd_rx));

    let mut final_text = String::new();
    let mut collecting_turn_output = false;
    let mut prompt_sent = false;
    let mut stop_reason = None;
    let mut stop_reason_text = None;
    let mut usage = None;
    let mut context_usage = None;
    let quota = Vec::<QuotaSnapshot>::new();
    let mut error = None;
    let mut permissions = Vec::new();
    let mut tool_calls = Vec::<ToolSummary>::new();
    let mut progress = Vec::<ProgressEvent>::new();

    loop {
        let event = match tokio::time::timeout_at(deadline, event_rx.recv()).await {
            Ok(Some(event)) => event,
            Ok(None) => {
                if error.is_none() && stop_reason.is_none() {
                    stop_reason_text = Some("worker_closed".to_string());
                    push_progress(
                        &mut progress,
                        "worker_closed",
                        "worker event channel closed",
                    );
                }
                break;
            }
            Err(_) => {
                let message = format!(
                    "delegated worker timed out after {}s",
                    worker_timeout.as_secs()
                );
                stop_reason_text = Some("timeout".to_string());
                push_progress(&mut progress, "timeout", message.clone());
                error = Some(message);
                break;
            }
        };
        match event {
            UiEvent::SessionStarted { .. } if !prompt_sent => {
                prompt_sent = true;
                push_progress(&mut progress, "session_started", "worker session ready");
                cmd_tx
                    .send(UiCommand::SendPrompt {
                        text: args.prompt.clone(),
                        images: Vec::new(),
                    })
                    .context("send delegated prompt")?;
                push_progress(&mut progress, "prompt_sent", "delegated prompt sent");
            }
            UiEvent::SessionUpdate(SessionUpdate::UserMessageChunk(_)) if prompt_sent => {
                collecting_turn_output = true;
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentThoughtChunk(chunk)) if prompt_sent => {
                collecting_turn_output = true;
                push_progress(
                    &mut progress,
                    "agent_thought",
                    preview(&content_block_text(&chunk.content), 160),
                );
            }
            UiEvent::SessionUpdate(SessionUpdate::AgentMessageChunk(chunk))
                if collecting_turn_output =>
            {
                let text = content_block_text(&chunk.content);
                final_text.push_str(&text);
                push_progress(&mut progress, "agent_message", preview(&text, 160));
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCall(tool_call)) => {
                push_progress(
                    &mut progress,
                    "tool_call",
                    format!("{} ({})", tool_call.title, tool_kind_label(tool_call.kind)),
                );
                upsert_tool_call_summary(&mut tool_calls, &tool_call);
                if prompt_sent {
                    collecting_turn_output = true;
                }
            }
            UiEvent::SessionUpdate(SessionUpdate::ToolCallUpdate(update)) => {
                upsert_tool_update_summary(&mut tool_calls, &update);
                if let Some(title) = update.fields.title {
                    push_progress(&mut progress, "tool_update", title);
                }
                if prompt_sent {
                    collecting_turn_output = true;
                }
            }
            UiEvent::SessionUpdate(SessionUpdate::UsageUpdate(update)) => {
                context_usage = Some(context_usage_summary(update));
            }
            UiEvent::PermissionRequest(prompt) => {
                let decision =
                    permission_decision(args.permission_mode, &prompt.tool_call, &prompt.options);
                permissions.push(format!(
                    "permission {}: {}",
                    prompt.tool_call.tool_call_id,
                    if decision.is_some() {
                        "selected"
                    } else {
                        "cancelled"
                    }
                ));
                push_progress(
                    &mut progress,
                    "permission",
                    format!(
                        "{} {}",
                        prompt.tool_call.tool_call_id,
                        if decision.is_some() {
                            "accepted"
                        } else {
                            "rejected"
                        }
                    ),
                );
                let _ = prompt.responder.send(match decision {
                    Some(option_id) => PermissionDecision::Selected(option_id),
                    None => PermissionDecision::Cancelled,
                });
            }
            UiEvent::PromptDone {
                stop_reason: reason,
                usage: prompt_usage,
            } => {
                stop_reason = Some(reason);
                usage = prompt_usage;
                push_progress(
                    &mut progress,
                    "prompt_done",
                    stop_reason_label(reason).to_string(),
                );
                break;
            }
            UiEvent::PromptFailed { message }
            | UiEvent::SessionForkFailed { message }
            | UiEvent::Fatal(message) => {
                push_progress(&mut progress, "error", message.clone());
                error = Some(message);
                break;
            }
            UiEvent::Connected { .. }
            | UiEvent::SessionStarted { .. }
            | UiEvent::TerminalOutput(_)
            | UiEvent::SessionConfigOptions
            | UiEvent::CancelPendingPermissions
            | UiEvent::RemotePermissionDecision { .. }
            | UiEvent::Warning(_)
            | UiEvent::Info(_) => {}
            UiEvent::SessionUpdate(_) => {}
        }
    }

    let _ = cmd_tx.send(UiCommand::Shutdown);
    let _ = tokio::time::timeout(Duration::from_secs(2), runtime).await;
    let reason = stop_reason_text.unwrap_or_else(|| {
        stop_reason
            .map(stop_reason_label)
            .unwrap_or("worker_closed")
            .to_string()
    });
    let text = final_text.clone();
    Ok(DelegatedRunResult {
        job_id: Some(args.job_id),
        source_id,
        final_text,
        text,
        stop_reason: reason,
        usage,
        context_usage,
        quota,
        tool_calls,
        progress,
        permissions,
        error,
    })
}

impl From<RunAgentJob> for RunAgentArgs {
    fn from(job: RunAgentJob) -> Self {
        Self {
            job_id: job.id,
            phase: job.phase,
            source_id: job.source_id,
            prompt: job.prompt,
            cwd: job.cwd,
            timeout_seconds: job.timeout_seconds,
            permission_mode: job.permission_mode,
        }
    }
}

fn delegated_cwd(requested: Option<&PathBuf>) -> Result<PathBuf> {
    let workspace = std::env::current_dir()
        .context("current dir")?
        .canonicalize()
        .context("canonicalize current dir")?;
    let requested = match requested {
        Some(path) if path.is_absolute() => path.clone(),
        Some(path) => workspace.join(path),
        None => workspace.clone(),
    };
    let cwd = requested
        .canonicalize()
        .with_context(|| format!("canonicalize delegated cwd {}", requested.display()))?;
    if !cwd.starts_with(&workspace) {
        bail!(
            "delegated cwd {} is outside workspace {}",
            cwd.display(),
            workspace.display()
        );
    }
    Ok(cwd)
}

fn worker_timeout(timeout_seconds: Option<u64>) -> Duration {
    timeout_seconds
        .map(|seconds| seconds.clamp(1, MAX_WORKER_TIMEOUT_SECONDS))
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_WORKER_TIMEOUT)
}

fn push_progress(
    progress: &mut Vec<ProgressEvent>,
    kind: impl Into<String>,
    detail: impl Into<String>,
) {
    let kind = kind.into();
    let detail = detail.into();
    emit_progress_if_visible(&kind, &detail);
    progress.push(ProgressEvent {
        sequence: progress.len() + 1,
        kind,
        detail,
    });
}

fn emit_progress_if_visible(kind: &str, detail: &str) {
    if !progress_kind_is_user_visible(kind) {
        return;
    }
    let Some(path) = std::env::var_os(THOR_PROGRESS_PATH_ENV).map(PathBuf::from) else {
        return;
    };
    let record = json!({
        "kind": kind,
        "detail": detail,
    });
    if let Ok(mut file) = OpenOptions::new().create(true).append(true).open(path) {
        let _ = writeln!(file, "{record}");
    }
}

fn progress_kind_is_user_visible(kind: &str) -> bool {
    matches!(
        kind,
        "session_started"
            | "prompt_sent"
            | "tool_call"
            | "tool_update"
            | "permission"
            | "prompt_done"
            | "timeout"
            | "error"
            | "worker_closed"
    )
}

fn context_usage_summary(update: UsageUpdate) -> ContextUsageSummary {
    ContextUsageSummary {
        used: update.used,
        size: update.size,
        cost: update
            .cost
            .map(|cost| format!("{:.4} {}", cost.amount, cost.currency)),
    }
}

fn aggregate_usage(jobs: &[DelegatedRunResult]) -> UsageAggregate {
    let mut aggregate = UsageAggregate {
        jobs: jobs.len(),
        ..UsageAggregate::default()
    };
    for usage in jobs.iter().filter_map(|job| job.usage.as_ref()) {
        aggregate.jobs_with_usage += 1;
        aggregate.total_tokens = aggregate.total_tokens.saturating_add(usage.total_tokens);
        aggregate.input_tokens = aggregate.input_tokens.saturating_add(usage.input_tokens);
        aggregate.output_tokens = aggregate.output_tokens.saturating_add(usage.output_tokens);
        aggregate.thought_tokens = aggregate
            .thought_tokens
            .saturating_add(usage.thought_tokens.unwrap_or_default());
    }
    aggregate
}

fn upsert_tool_call_summary(tool_calls: &mut Vec<ToolSummary>, tool_call: &ToolCall) {
    let id = tool_call.tool_call_id.to_string();
    if let Some(existing) = tool_calls.iter_mut().find(|tool| tool.id == id) {
        existing.title = tool_call.title.clone();
        existing.kind = Some(tool_kind_label(tool_call.kind).to_string());
        existing.status = Some(tool_status_label(tool_call.status).to_string());
        return;
    }
    tool_calls.push(ToolSummary {
        id,
        title: tool_call.title.clone(),
        kind: Some(tool_kind_label(tool_call.kind).to_string()),
        status: Some(tool_status_label(tool_call.status).to_string()),
    });
}

fn upsert_tool_update_summary(tool_calls: &mut Vec<ToolSummary>, update: &ToolCallUpdate) {
    let id = update.tool_call_id.to_string();
    if let Some(existing) = tool_calls.iter_mut().find(|tool| tool.id == id) {
        if let Some(title) = update.fields.title.as_ref() {
            existing.title = title.clone();
        }
        if let Some(kind) = update.fields.kind {
            existing.kind = Some(tool_kind_label(kind).to_string());
        }
        if let Some(status) = update.fields.status {
            existing.status = Some(tool_status_label(status).to_string());
        }
        return;
    }
    tool_calls.push(ToolSummary {
        id,
        title: update
            .fields
            .title
            .clone()
            .unwrap_or_else(|| "tool call".to_string()),
        kind: update
            .fields
            .kind
            .map(|kind| tool_kind_label(kind).to_string()),
        status: update
            .fields
            .status
            .map(|status| tool_status_label(status).to_string()),
    });
}

fn preview(text: &str, max_chars: usize) -> String {
    let mut preview = text.replace('\n', " ");
    if preview.chars().count() > max_chars {
        preview = preview.chars().take(max_chars).collect::<String>();
        preview.push_str("...");
    }
    preview
}

fn permission_decision(
    mode: BridgePermissionMode,
    tool_call: &ToolCallUpdate,
    options: &[agent_client_protocol::schema::v1::PermissionOption],
) -> Option<String> {
    let allow = match mode {
        BridgePermissionMode::Reject => false,
        BridgePermissionMode::AcceptEdits => matches!(
            tool_call.fields.kind,
            Some(ToolKind::Edit | ToolKind::Delete | ToolKind::Move)
        ),
    };
    if !allow {
        return None;
    }
    options
        .iter()
        .find(|option| option.kind == PermissionOptionKind::AllowAlways)
        .or_else(|| {
            options
                .iter()
                .find(|option| option.kind == PermissionOptionKind::AllowOnce)
        })
        .map(|option| option.option_id.to_string())
}

fn stop_reason_label(reason: StopReason) -> &'static str {
    match reason {
        StopReason::EndTurn => "end_turn",
        StopReason::MaxTokens => "max_tokens",
        StopReason::MaxTurnRequests => "max_turn_requests",
        StopReason::Refusal => "refusal",
        StopReason::Cancelled => "cancelled",
        _ => "other",
    }
}

fn tool_kind_label(kind: ToolKind) -> &'static str {
    match kind {
        ToolKind::Read => "read",
        ToolKind::Edit => "edit",
        ToolKind::Delete => "delete",
        ToolKind::Move => "move",
        ToolKind::Search => "search",
        ToolKind::Execute => "execute",
        ToolKind::Think => "think",
        ToolKind::Fetch => "fetch",
        ToolKind::SwitchMode => "switch_mode",
        ToolKind::Other => "other",
        _ => "other",
    }
}

fn tool_status_label(status: ToolCallStatus) -> &'static str {
    match status {
        ToolCallStatus::Pending => "pending",
        ToolCallStatus::InProgress => "in_progress",
        ToolCallStatus::Completed => "completed",
        ToolCallStatus::Failed => "failed",
        _ => "other",
    }
}

fn read_message(reader: &mut impl BufRead) -> Result<Option<Vec<u8>>> {
    loop {
        let mut line = Vec::new();
        let read = reader.read_until(b'\n', &mut line)?;
        if read == 0 {
            return Ok(None);
        }
        while matches!(line.last(), Some(b'\n' | b'\r')) {
            line.pop();
        }
        if line.is_empty() {
            continue;
        }
        return Ok(Some(line));
    }
}

fn write_message(writer: &mut impl Write, body: &[u8]) -> Result<()> {
    writer.write_all(body)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stdio_mcp_server_uses_current_binary_entrypoint_shape() {
        let server = stdio_mcp_server(
            PathBuf::from("/tmp/mj"),
            PathBuf::from("/tmp/config.toml"),
            Some(PathBuf::from("/tmp/progress.jsonl")),
        );
        let McpServer::Stdio(stdio) = server else {
            panic!("expected stdio MCP server");
        };
        assert_eq!(stdio.name, thor::THOR_MCP_SERVER_NAME);
        assert_eq!(stdio.command, PathBuf::from("/tmp/mj"));
        assert_eq!(stdio.args, vec!["thor-mcp"]);
        assert_eq!(stdio.env[0].name, "MJ_THOR_CONFIG");
        assert_eq!(stdio.env[0].value, "/tmp/config.toml");
        assert_eq!(stdio.env[1].name, THOR_PROGRESS_PATH_ENV);
        assert_eq!(stdio.env[1].value, "/tmp/progress.jsonl");
    }

    #[test]
    fn progress_visibility_filter_omits_chatty_text_chunks() {
        assert!(progress_kind_is_user_visible("tool_call"));
        assert!(progress_kind_is_user_visible("prompt_done"));
        assert!(!progress_kind_is_user_visible("agent_message"));
        assert!(!progress_kind_is_user_visible("agent_thought"));
    }

    #[test]
    fn tool_definitions_include_catalog_and_batch_runner() {
        let tools = tool_definitions();
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str).map(str::to_string))
            .collect::<Vec<_>>();

        assert!(names.iter().any(|name| name == "thor_get_model_catalog"));
        assert!(names.iter().any(|name| name == "thor_get_workflow_state"));
        assert!(names.iter().any(|name| name == "thor_submit_plan"));
        assert!(names.iter().any(|name| name == "thor_validate_acp_agents"));
        assert!(names.iter().any(|name| name == "thor_refresh_quota"));
        assert!(names.iter().any(|name| name == "thor_run_acp_agents"));
        let list_tool = tools
            .iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some("thor_list_acp_agents"))
            .expect("list tool");
        assert!(
            list_tool
                .pointer("/inputSchema/properties/validate")
                .is_some()
        );
        let run_tool = tools
            .iter()
            .find(|tool| tool.get("name").and_then(Value::as_str) == Some("thor_run_acp_agent"))
            .expect("run tool");
        assert_eq!(
            run_tool.pointer("/inputSchema/properties/permissionMode/enum"),
            Some(&json!(["reject", "accept_edits"]))
        );
        assert!(
            run_tool
                .pointer("/inputSchema/properties/timeoutSeconds")
                .is_some()
        );
        assert_eq!(
            run_tool.pointer("/inputSchema/required"),
            Some(&json!(["jobId", "phase", "sourceId", "prompt"]))
        );
    }

    #[test]
    fn stdio_messages_are_newline_delimited_json() {
        let mut reader = std::io::Cursor::new(
            br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}
"#,
        );

        assert_eq!(
            read_message(&mut reader).expect("read message"),
            Some(br#"{"jsonrpc":"2.0","id":1,"method":"tools/list"}"#.to_vec())
        );

        let mut writer = Vec::new();
        write_message(&mut writer, br#"{"jsonrpc":"2.0","id":1,"result":{}}"#)
            .expect("write message");

        assert_eq!(
            String::from_utf8(writer).expect("utf8"),
            "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}\n"
        );
    }

    #[test]
    fn run_agent_args_reject_bypass_permission_mode() {
        let parsed = serde_json::from_value::<RunAgentArgs>(json!({
            "jobId": "impl",
            "phase": "implementation",
            "sourceId": "codex",
            "prompt": "work",
            "permissionMode": "bypass"
        }));

        assert!(parsed.is_err());
    }

    fn test_server(source_id: &str) -> config::ConfiguredAcpServer {
        config::ConfiguredAcpServer {
            source_id: source_id.to_string(),
            name: source_id.to_string(),
            program: PathBuf::from("agent"),
            args: Vec::new(),
            env: Default::default(),
            description: String::new(),
            setup_hint: String::new(),
            setup_url: String::new(),
            quota_backend: config::ThorQuotaBackend::None,
        }
    }

    fn workflow_test_config() -> Config {
        Config {
            thor: thor::ThorConfig {
                configured_acp_servers: vec![test_server("worker-a"), test_server("worker-b")],
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn valid_plan() -> WorkflowPlan {
        WorkflowPlan {
            complexity: ThorTaskComplexity::Hard,
            strategy: "architect".to_string(),
            rationale: "hard enough to compare and review".to_string(),
            implementation_jobs: vec![PlannedJob {
                id: "impl".to_string(),
                source_id: "worker-a".to_string(),
                prompt: "implement".to_string(),
                model: None,
                purpose: "implementation".to_string(),
            }],
            review_jobs: vec![PlannedJob {
                id: "review".to_string(),
                source_id: "worker-b".to_string(),
                prompt: "review".to_string(),
                model: None,
                purpose: "adversarial review".to_string(),
            }],
            correction_jobs: vec![PlannedJob {
                id: "fix".to_string(),
                source_id: "worker-a".to_string(),
                prompt: "correct".to_string(),
                model: None,
                purpose: "correction".to_string(),
            }],
        }
    }

    fn ready_workflow() -> WorkflowState {
        WorkflowState {
            facts: WorkflowFacts {
                listed_workers: true,
                refreshed_quota: true,
                validated_workers: true,
                loaded_model_catalog: true,
            },
            ..Default::default()
        }
    }

    fn run_args(job_id: &str, phase: WorkflowPhase, source_id: &str) -> RunAgentArgs {
        let prompt = match job_id {
            "impl" => "implement",
            "review" => "review",
            "fix" => "correct",
            _ => "work",
        };
        RunAgentArgs {
            job_id: job_id.to_string(),
            phase,
            source_id: source_id.to_string(),
            prompt: prompt.to_string(),
            cwd: None,
            timeout_seconds: None,
            permission_mode: BridgePermissionMode::Reject,
        }
    }

    #[test]
    fn thor_plan_requires_facts_before_execution() {
        let config = workflow_test_config();
        let mut workflow = WorkflowState::default();

        assert!(submit_plan(valid_plan(), &config, &mut workflow).is_err());
        assert!(
            authorize_single_run(
                &run_args("impl", WorkflowPhase::Implementation, "worker-a"),
                &workflow
            )
            .is_err()
        );

        workflow = ready_workflow();
        submit_plan(valid_plan(), &config, &mut workflow).expect("accepted plan");

        assert_eq!(
            authorize_single_run(
                &run_args("impl", WorkflowPhase::Implementation, "worker-a"),
                &workflow
            )
            .expect("authorized"),
            WorkflowPhase::Implementation
        );
    }

    #[test]
    fn thor_workflow_enforces_phase_order() {
        let config = workflow_test_config();
        let mut workflow = ready_workflow();
        submit_plan(valid_plan(), &config, &mut workflow).expect("accepted plan");

        assert!(
            authorize_single_run(
                &run_args("fix", WorkflowPhase::Correction, "worker-a"),
                &workflow
            )
            .is_err()
        );
        assert!(
            authorize_single_run(
                &run_args("impl", WorkflowPhase::Implementation, "worker-a"),
                &workflow
            )
            .is_ok()
        );

        workflow.complete_phase(WorkflowPhase::Implementation);
        assert!(
            authorize_single_run(
                &run_args("review", WorkflowPhase::Review, "worker-b"),
                &workflow
            )
            .is_ok()
        );

        workflow.complete_phase(WorkflowPhase::Review);
        assert!(
            authorize_single_run(
                &run_args("fix", WorkflowPhase::Correction, "worker-a"),
                &workflow
            )
            .is_ok()
        );
    }

    #[test]
    fn thor_plan_requires_different_review_worker_when_available() {
        let config = workflow_test_config();
        let mut plan = valid_plan();
        plan.review_jobs[0].source_id = "worker-a".to_string();

        assert!(validate_plan(&plan, &config).is_err());
    }

    #[test]
    fn thor_run_prompt_must_match_accepted_plan() {
        let config = workflow_test_config();
        let mut workflow = ready_workflow();
        submit_plan(valid_plan(), &config, &mut workflow).expect("accepted plan");
        let mut args = run_args("impl", WorkflowPhase::Implementation, "worker-a");
        args.prompt = "different work".to_string();

        assert!(authorize_single_run(&args, &workflow).is_err());
    }

    #[test]
    fn delegated_cwd_must_stay_inside_workspace() {
        let outside = PathBuf::from("..");

        assert!(delegated_cwd(Some(&outside)).is_err());
    }

    #[test]
    fn worker_timeout_defaults_and_clamps() {
        assert_eq!(worker_timeout(None), DEFAULT_WORKER_TIMEOUT);
        assert_eq!(worker_timeout(Some(0)), Duration::from_secs(1));
        assert_eq!(
            worker_timeout(Some(MAX_WORKER_TIMEOUT_SECONDS + 1)),
            Duration::from_secs(MAX_WORKER_TIMEOUT_SECONDS)
        );
    }

    #[test]
    fn aggregate_usage_sums_completed_worker_usage() {
        let jobs = vec![
            DelegatedRunResult {
                job_id: Some("a".to_string()),
                source_id: "claude".to_string(),
                final_text: String::new(),
                text: String::new(),
                stop_reason: "end_turn".to_string(),
                usage: Some(Usage::new(10, 4, 6)),
                context_usage: None,
                quota: Vec::new(),
                tool_calls: Vec::new(),
                progress: Vec::new(),
                permissions: Vec::new(),
                error: None,
            },
            DelegatedRunResult {
                job_id: Some("b".to_string()),
                source_id: "codex".to_string(),
                final_text: String::new(),
                text: String::new(),
                stop_reason: "error".to_string(),
                usage: None,
                context_usage: None,
                quota: Vec::new(),
                tool_calls: Vec::new(),
                progress: Vec::new(),
                permissions: Vec::new(),
                error: Some("failed".to_string()),
            },
        ];

        let aggregate = aggregate_usage(&jobs);
        assert_eq!(aggregate.jobs, 2);
        assert_eq!(aggregate.jobs_with_usage, 1);
        assert_eq!(aggregate.total_tokens, 10);
        assert_eq!(aggregate.input_tokens, 4);
        assert_eq!(aggregate.output_tokens, 6);
    }
}
