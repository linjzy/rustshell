use rmcp::{
    handler::server::wrapper::Parameters, model::CallToolResult, schemars, tool, tool_handler,
    tool_router, ServerHandler, ServiceExt,
};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Mutex,
};

const MAX_ERROR_BYTES: usize = 16 * 1024;
const MAX_EXEC_TIMEOUT_SECONDS: u64 = 300;
const MAX_FILE_RESUME_RECONNECTS: usize = 32;
const MAX_SAME_HIGH_WATER_RECONNECTS: usize = 2;

#[derive(Clone)]
pub struct RustDeskMcp {
    wrapper: Arc<PathBuf>,
    sessions: Arc<Mutex<HashMap<(SessionChannel, String), SessionSlot>>>,
}

type SessionSlot = Arc<Mutex<Option<SessionProcess>>>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum SessionChannel {
    Terminal,
    File,
}

impl SessionChannel {
    fn wrapper_action(self) -> &'static str {
        match self {
            Self::Terminal => "session",
            Self::File => "file-session",
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Terminal => "terminal",
            Self::File => "file",
        }
    }
}

struct SessionProcess {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct RunCommandParams {
    /// Exact RustDesk device ID returned by rustdesk_list_devices.
    pub device_id: String,
    /// One command interpreted by PowerShell on Windows or /bin/sh on macOS/Linux.
    pub command: String,
    /// Hard command timeout in seconds, from 1 through 300. Defaults to 60.
    pub timeout_seconds: Option<u64>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct UploadFileParams {
    /// Exact RustDesk device ID returned by rustdesk_list_devices.
    pub device_id: String,
    /// Existing regular file on the MCP host.
    pub local_path: String,
    /// Exact destination file path on the remote device. Existing files are overwritten.
    pub remote_path: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
pub struct DownloadFileParams {
    /// Exact RustDesk device ID returned by rustdesk_list_devices.
    pub device_id: String,
    /// Exact existing regular-file path on the remote device.
    pub remote_path: String,
    /// Exact destination file path on the MCP host. Existing files are overwritten.
    pub local_path: String,
}

struct ProcessOutput {
    success: bool,
    stdout: String,
    stderr: String,
}

impl RustDeskMcp {
    pub fn new(wrapper: PathBuf) -> Self {
        Self {
            wrapper: Arc::new(wrapper),
            sessions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn run_wrapper(
        &self,
        args: &[String],
        timeout: Duration,
    ) -> Result<ProcessOutput, String> {
        let mut command = Command::new(self.wrapper.as_ref());
        command
            .args(args)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let child = command
            .spawn()
            .map_err(|error| format!("failed to start RustShell wrapper: {error}"))?;
        let output = tokio::time::timeout(timeout, child.wait_with_output())
            .await
            .map_err(|_| format!("RustShell wrapper exceeded {} seconds", timeout.as_secs()))?
            .map_err(|error| format!("failed while waiting for RustShell wrapper: {error}"))?;

        Ok(ProcessOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: truncate_text(String::from_utf8_lossy(&output.stderr).into_owned()),
        })
    }

    async fn session_slot(&self, channel: SessionChannel, device_id: &str) -> SessionSlot {
        let mut sessions = self.sessions.lock().await;
        sessions
            .entry((channel, device_id.to_owned()))
            .or_insert_with(|| Arc::new(Mutex::new(None)))
            .clone()
    }

    async fn start_session(
        &self,
        channel: SessionChannel,
        device_id: &str,
    ) -> Result<SessionProcess, String> {
        let mut command = Command::new(self.wrapper.as_ref());
        command
            .args([channel.wrapper_action(), device_id])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true);

        let mut child = command
            .spawn()
            .map_err(|error| format!("failed to start {} session: {error}", channel.name()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| format!("{} session has no stdin", channel.name()))?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| format!("{} session has no stdout", channel.name()))?;
        let mut stdout = BufReader::new(stdout).lines();
        let ready = tokio::time::timeout(Duration::from_secs(45), stdout.next_line())
            .await
            .map_err(|_| {
                format!(
                    "{} session did not become ready in 45 seconds",
                    channel.name()
                )
            })?
            .map_err(|error| {
                format!(
                    "failed to read {} session readiness: {error}",
                    channel.name()
                )
            })?
            .ok_or_else(|| format!("{} session exited before becoming ready", channel.name()))?;
        let ready: Value = serde_json::from_str(&ready).map_err(|error| {
            format!("invalid {} session readiness JSON: {error}", channel.name())
        })?;
        if ready.get("type").and_then(Value::as_str) != Some("ready")
            || ready.get("channel").and_then(Value::as_str) != Some(channel.name())
            || ready.get("device_id").and_then(Value::as_str) != Some(device_id)
        {
            return Err(format!(
                "{} session readiness did not match device {device_id}",
                channel.name()
            ));
        }

        Ok(SessionProcess {
            child,
            stdin,
            stdout,
        })
    }

    async fn call_session(
        &self,
        channel: SessionChannel,
        device_id: &str,
        request: Value,
        timeout: Option<Duration>,
    ) -> Result<Value, String> {
        let slot = self.session_slot(channel, device_id).await;
        let mut session = slot.lock().await;
        let mut reused = false;

        if let Some(process) = session.as_mut() {
            match process.child.try_wait() {
                Ok(None) => reused = true,
                Ok(Some(_)) => *session = None,
                Err(error) => {
                    *session = None;
                    return Err(format!(
                        "failed to inspect {} session before request: {error}",
                        channel.name()
                    ));
                }
            }
        }
        if session.is_none() {
            *session = Some(self.start_session(channel, device_id).await?);
        }

        let process = session.as_mut().expect("session initialized");
        let mut encoded = serde_json::to_vec(&request)
            .map_err(|error| format!("failed to encode session request: {error}"))?;
        encoded.push(b'\n');
        let exchange = async {
            process.stdin.write_all(&encoded).await.map_err(|error| {
                format!(
                    "failed to write {} session request: {error}",
                    channel.name()
                )
            })?;
            process.stdin.flush().await.map_err(|error| {
                format!(
                    "failed to flush {} session request: {error}",
                    channel.name()
                )
            })?;
            process
                .stdout
                .next_line()
                .await
                .map_err(|error| {
                    format!(
                        "failed to read {} session response: {error}",
                        channel.name()
                    )
                })?
                .ok_or_else(|| {
                    format!(
                        "{} session closed before returning a response",
                        channel.name()
                    )
                })
        };
        let response = match timeout {
            Some(timeout) => match tokio::time::timeout(timeout, exchange).await {
                Ok(response) => response,
                Err(_) => {
                    *session = None;
                    return Err(format!(
                        "{} session response exceeded {} seconds",
                        channel.name(),
                        timeout.as_secs()
                    ));
                }
            },
            None => exchange.await,
        };
        let line = match response {
            Ok(line) => line,
            Err(error) => {
                *session = None;
                return Err(error);
            }
        };
        let mut value: Value = match serde_json::from_str(&line) {
            Ok(value) => value,
            Err(error) => {
                *session = None;
                return Err(format!(
                    "invalid {} session response JSON: {error}",
                    channel.name()
                ));
            }
        };
        let reconnect = response_requires_reconnect(&value);
        if let Some(object) = value.as_object_mut() {
            object.insert("session_reused".to_owned(), Value::Bool(reused));
            object.insert(
                "session_channel".to_owned(),
                Value::String(channel.name().to_owned()),
            );
            object.insert(
                "connection_policy".to_owned(),
                Value::String("reuse_until_disconnect_or_300s_idle".to_owned()),
            );
        }
        if reconnect {
            *session = None;
        }
        Ok(value)
    }

    async fn call_file_session(&self, device_id: &str, request: Value) -> Result<Value, String> {
        let mut resume_reconnects = 0;
        let mut same_high_water_reconnects = 0;
        let mut initial_session_reused = None;
        let mut last_progress = file_request_progress(&request);

        loop {
            let mut value = self
                .call_session(SessionChannel::File, device_id, request.clone(), None)
                .await?;
            if initial_session_reused.is_none() {
                initial_session_reused = value.get("session_reused").and_then(Value::as_bool);
            }

            let can_resume = file_response_can_resume(&value);
            let progress = value
                .get("progress_bytes")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| file_request_progress(&request));
            let made_progress = file_response_made_progress(&value, last_progress);
            if can_resume && file_response_only_replayed_u32_tail(&value, last_progress) {
                same_high_water_reconnects += 1;
            } else if progress > last_progress {
                same_high_water_reconnects = 0;
            }
            let chunk_fallback_required =
                same_high_water_reconnects >= MAX_SAME_HIGH_WATER_RECONNECTS;
            if can_resume
                && made_progress
                && !chunk_fallback_required
                && resume_reconnects < MAX_FILE_RESUME_RECONNECTS
            {
                last_progress = last_progress.max(progress);
                resume_reconnects += 1;
                continue;
            }

            if let Some(object) = value.as_object_mut() {
                object.insert(
                    "resume_reconnects".to_owned(),
                    Value::from(resume_reconnects as u64),
                );
                object.insert(
                    "automatic_resume_limit".to_owned(),
                    Value::from(MAX_FILE_RESUME_RECONNECTS as u64),
                );
                object.insert(
                    "same_high_water_reconnects".to_owned(),
                    Value::from(same_high_water_reconnects as u64),
                );
                object.insert(
                    "same_high_water_limit".to_owned(),
                    Value::from(MAX_SAME_HIGH_WATER_RECONNECTS as u64),
                );
                object.insert(
                    "chunk_fallback_required".to_owned(),
                    Value::Bool(chunk_fallback_required),
                );
                object.insert(
                    "initial_session_reused".to_owned(),
                    Value::Bool(initial_session_reused.unwrap_or(false)),
                );
                object.insert("replayed".to_owned(), Value::Bool(false));
                object.insert(
                    "resume_stalled".to_owned(),
                    Value::Bool(can_resume && (!made_progress || chunk_fallback_required)),
                );
                object.insert(
                    "resume_exhausted".to_owned(),
                    Value::Bool(
                        can_resume
                            && made_progress
                            && resume_reconnects == MAX_FILE_RESUME_RECONNECTS,
                    ),
                );
            }
            return Ok(value);
        }
    }
}

fn file_response_can_resume(value: &Value) -> bool {
    value.get("stage").and_then(Value::as_str) == Some("session_disconnected")
        && value.get("resume_supported").and_then(Value::as_bool) == Some(true)
        && value.get("partial_preserved").and_then(Value::as_bool) == Some(true)
}

fn response_requires_reconnect(value: &Value) -> bool {
    value.get("reconnect_on_next_call").and_then(Value::as_bool) == Some(true)
}

fn file_response_made_progress(value: &Value, last_progress: u64) -> bool {
    value
        .get("progress_bytes")
        .and_then(Value::as_u64)
        .is_some_and(|progress| progress > last_progress)
        || value
            .get("attempt_bytes")
            .and_then(Value::as_u64)
            .is_some_and(|attempt| attempt > 0)
}

fn file_response_only_replayed_u32_tail(value: &Value, last_progress: u64) -> bool {
    last_progress > u64::from(u32::MAX)
        && value
            .get("progress_bytes")
            .and_then(Value::as_u64)
            .is_some_and(|progress| progress <= last_progress)
        && value
            .get("attempt_bytes")
            .and_then(Value::as_u64)
            .is_some_and(|attempt| attempt > 0)
}

fn file_request_progress(request: &Value) -> u64 {
    if request.get("operation").and_then(Value::as_str) != Some("pull") {
        return 0;
    }
    request
        .get("local_path")
        .and_then(Value::as_str)
        .and_then(|path| std::fs::metadata(format!("{path}.download")).ok())
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

#[tool_router]
impl RustDeskMcp {
    #[tool(
        name = "rustdesk_list_devices",
        description = "Read the live RustDesk peer TOML files now and return the current saved-device list without opening a remote connection. Use once when resolving a target for a task; refresh only when the target changes, the user asks, matching is ambiguous, or device/session validation fails."
    )]
    async fn list_devices(&self) -> CallToolResult {
        let args = vec!["devices".to_owned(), "--json".to_owned()];
        let output = match self.run_wrapper(&args, Duration::from_secs(15)).await {
            Ok(output) => output,
            Err(error) => return tool_error("devices", error),
        };
        if !output.success {
            return tool_error("devices", output.stderr);
        }

        let devices: Value = match serde_json::from_str(output.stdout.trim()) {
            Ok(Value::Array(devices)) => Value::Array(devices),
            Ok(_) => return tool_error("devices", "wrapper returned a non-array device list"),
            Err(error) => {
                return tool_error(
                    "devices",
                    format!("invalid device JSON from wrapper: {error}"),
                )
            }
        };
        CallToolResult::structured(json!({
            "ok": true,
            "fresh": true,
            "queried_at_unix_ms": unix_time_millis(),
            "devices": devices
        }))
    }

    #[tool(
        name = "rustdesk_run_command",
        description = "Run one bounded command through the per-device reusable terminal session and return its real exit code, stdout, stderr, authenticated device identity, platform, duration, and whether the connection was reused. Resolve the target once at task start; do not relist before each command on the same authenticated device. The command may change remote state."
    )]
    async fn run_command(
        &self,
        Parameters(params): Parameters<RunCommandParams>,
    ) -> CallToolResult {
        if let Err(error) = validate_device_id(&params.device_id) {
            return tool_error("validate_device", error);
        }
        if params.command.trim().is_empty() {
            return tool_error("validate_command", "command cannot be empty");
        }
        let timeout_seconds = params.timeout_seconds.unwrap_or(60);
        if !(1..=MAX_EXEC_TIMEOUT_SECONDS).contains(&timeout_seconds) {
            return tool_error(
                "validate_timeout",
                format!("timeout_seconds must be between 1 and {MAX_EXEC_TIMEOUT_SECONDS}"),
            );
        }

        let value = match self
            .call_session(
                SessionChannel::Terminal,
                &params.device_id,
                json!({
                    "command": params.command,
                    "timeout_seconds": timeout_seconds
                }),
                Some(Duration::from_secs(timeout_seconds + 15)),
            )
            .await
        {
            Ok(value) => value,
            Err(error) => return session_error("terminal_session", error),
        };
        if value.get("ok").and_then(Value::as_bool) == Some(true) {
            CallToolResult::structured(value)
        } else {
            CallToolResult::structured_error(value)
        }
    }

    #[tool(
        name = "rustdesk_upload_file",
        description = "Upload one local regular file, including large files, through the per-device reusable file-transfer session and return byte count, local SHA-256, resume offset, and connection reuse details. There is no server-side total-duration limit; the transfer stops after 300 seconds without protocol progress. A confirmed mid-transfer disconnect reconnects only when that connection transferred data, up to 32 times. RustDesk's 32-bit resume offset can retransmit the tail after 4 GiB, but never restarts the whole file; two reconnects without a higher persisted byte count return chunk_fallback_required instead of looping. Existing remote files are overwritten. Reuse the target resolved for the current task."
    )]
    async fn upload_file(
        &self,
        Parameters(params): Parameters<UploadFileParams>,
    ) -> CallToolResult {
        if let Err(error) = validate_device_id(&params.device_id) {
            return tool_error("validate_device", error);
        }
        if params.remote_path.is_empty() {
            return tool_error("validate_remote_path", "remote_path cannot be empty");
        }
        let local_path = PathBuf::from(&params.local_path);
        let (bytes, sha256) = match file_fingerprint(&local_path) {
            Ok(value) => value,
            Err(error) => return tool_error("validate_local_file", error),
        };

        let mut value = match self
            .call_file_session(
                &params.device_id,
                json!({
                    "operation": "push",
                    "local_path": params.local_path.clone(),
                    "remote_path": params.remote_path.clone()
                }),
            )
            .await
        {
            Ok(value) => value,
            Err(error) => return session_error("file_session", error),
        };
        if value.get("ok").and_then(Value::as_bool) != Some(true) {
            return CallToolResult::structured_error(value);
        }
        if let Some(object) = value.as_object_mut() {
            object.insert("bytes".to_owned(), Value::from(bytes));
            object.insert("sha256".to_owned(), Value::String(sha256));
            object.insert("stage".to_owned(), Value::String("remote_done".to_owned()));
        }
        CallToolResult::structured(value)
    }

    #[tool(
        name = "rustdesk_download_file",
        description = "Download one remote regular file, including large files, through the per-device reusable file-transfer session and return byte count, SHA-256, resume offset, and connection reuse details. There is no server-side total-duration limit; the transfer stops after 300 seconds without protocol progress. A confirmed mid-transfer disconnect reconnects only when that connection transferred data, up to 32 times; if stalled or exhausted, the partial file remains resumable on the next call. RustDesk's 32-bit resume offset can retransmit the tail after 4 GiB, but never restarts the whole file; two reconnects without a higher persisted byte count return chunk_fallback_required instead of looping. Existing local files are overwritten. Reuse the target resolved for the current task."
    )]
    async fn download_file(
        &self,
        Parameters(params): Parameters<DownloadFileParams>,
    ) -> CallToolResult {
        if let Err(error) = validate_device_id(&params.device_id) {
            return tool_error("validate_device", error);
        }
        if params.remote_path.is_empty() {
            return tool_error("validate_remote_path", "remote_path cannot be empty");
        }
        if params.local_path.is_empty() {
            return tool_error("validate_local_path", "local_path cannot be empty");
        }

        let mut value = match self
            .call_file_session(
                &params.device_id,
                json!({
                    "operation": "pull",
                    "local_path": params.local_path.clone(),
                    "remote_path": params.remote_path.clone()
                }),
            )
            .await
        {
            Ok(value) => value,
            Err(error) => return session_error("file_session", error),
        };
        if value.get("ok").and_then(Value::as_bool) != Some(true) {
            return CallToolResult::structured_error(value);
        }

        let (bytes, sha256) = match file_fingerprint(Path::new(&params.local_path)) {
            Ok(value) => value,
            Err(error) => return tool_error("verify_local_file", error),
        };
        if let Some(object) = value.as_object_mut() {
            object.insert("bytes".to_owned(), Value::from(bytes));
            object.insert("sha256".to_owned(), Value::String(sha256));
            object.insert(
                "stage".to_owned(),
                Value::String("local_verified".to_owned()),
            );
        }
        CallToolResult::structured(value)
    }
}

#[tool_handler(
    name = "rustdesk",
    version = "0.5.4",
    instructions = "Call rustdesk_list_devices once when a task first resolves a RustDesk target, then reuse that exact device_id and its authenticated sessions for subsequent operations on the same target without relisting. Refresh only when the target changes, the user requests it, matching is ambiguous, a new unrelated task starts, or device/session validation fails. Device listing reads live local peer files and does not connect. Commands reuse one terminal session per device; uploads and downloads reuse a separate file-transfer session. File transfers have no server-side total-duration limit and fail after 300 seconds without protocol progress; configure the MCP client timeout high enough for the file size. A dead or idle session reconnects on the next call. A confirmed file-transfer disconnect may reconnect only after measurable transferred data, up to 32 times. RustDesk's 32-bit offset can retransmit the tail after 4 GiB but must not restart the whole file; after two reconnects without a higher persisted byte count, return chunk_fallback_required so the client can use terminal plus file channels for verified chunks. Never replay a terminal command, guess a menu index, request or log credentials, silently retry non-connection errors or zero-progress transfers, or fall back to SSH."
)]
impl ServerHandler for RustDeskMcp {}

pub async fn serve(wrapper: PathBuf) -> anyhow::Result<()> {
    if !wrapper.is_file() {
        anyhow::bail!("RustShell wrapper is not a file: {}", wrapper.display());
    }
    let service = RustDeskMcp::new(wrapper)
        .serve(rmcp::transport::stdio())
        .await?;
    service.waiting().await?;
    Ok(())
}

fn validate_device_id(device_id: &str) -> Result<(), String> {
    if device_id.is_empty() || !device_id.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("device_id must contain ASCII digits only".to_owned());
    }
    Ok(())
}

fn file_fingerprint(path: &Path) -> Result<(u64, String), String> {
    let metadata = path
        .metadata()
        .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("not a regular file: {}", path.display()));
    }

    let mut file =
        File::open(path).map_err(|error| format!("cannot open {}: {error}", path.display()))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|error| format!("cannot hash {}: {error}", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok((metadata.len(), format!("{:x}", hasher.finalize())))
}

fn tool_error(stage: &str, message: impl Into<String>) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "ok": false,
        "stage": stage,
        "error": truncate_text(message.into())
    }))
}

fn session_error(stage: &str, message: impl Into<String>) -> CallToolResult {
    CallToolResult::structured_error(json!({
        "ok": false,
        "stage": stage,
        "error": truncate_text(message.into()),
        "replayed": false,
        "reconnect_on_next_call": true
    }))
}

fn truncate_text(mut value: String) -> String {
    if value.len() > MAX_ERROR_BYTES {
        value.truncate(MAX_ERROR_BYTES);
        value.push_str("...<truncated>");
    }
    value
}

fn unix_time_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn device_id_requires_digits() {
        assert!(validate_device_id("134222114").is_ok());
        assert!(validate_device_id("").is_err());
        assert!(validate_device_id("134-222").is_err());
    }

    #[test]
    fn truncates_large_errors() {
        let value = truncate_text("x".repeat(MAX_ERROR_BYTES + 10));
        assert!(value.ends_with("...<truncated>"));
        assert!(value.len() < MAX_ERROR_BYTES + 32);
    }

    #[test]
    fn reconnect_policy_uses_the_explicit_response_flag() {
        assert!(response_requires_reconnect(&json!({
            "stage": "command_timeout",
            "reconnect_on_next_call": true
        })));
        assert!(!response_requires_reconnect(&json!({
            "stage": "command_completed",
            "reconnect_on_next_call": false
        })));
    }

    #[test]
    fn resumes_only_confirmed_disconnect_with_partial_data() {
        assert!(file_response_can_resume(&json!({
            "stage": "session_disconnected",
            "resume_supported": true,
            "partial_preserved": true,
            "progress_bytes": 42
        })));
        assert!(!file_response_can_resume(&json!({
            "stage": "transfer_failed",
            "resume_supported": true,
            "partial_preserved": true
        })));
        assert!(!file_response_can_resume(&json!({
            "stage": "session_disconnected",
            "resume_supported": true,
            "partial_preserved": false
        })));
    }

    #[test]
    fn attempt_bytes_prove_progress_beyond_u32_resume_offset() {
        let previous_size = u64::from(u32::MAX) + 1024;
        let response = json!({
            "progress_bytes": previous_size,
            "attempt_bytes": 512
        });

        assert!(file_response_made_progress(&response, previous_size));
        assert!(!file_response_made_progress(
            &json!({"progress_bytes": previous_size, "attempt_bytes": 0}),
            previous_size
        ));
        assert!(file_response_only_replayed_u32_tail(
            &response,
            previous_size
        ));
        assert!(!file_response_only_replayed_u32_tail(
            &json!({"progress_bytes": previous_size + 1, "attempt_bytes": 512}),
            previous_size
        ));
    }
}
