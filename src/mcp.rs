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
const FILE_TOOL_TIMEOUT: Duration = Duration::from_secs(15 * 60);

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
        timeout: Duration,
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
        let line = match tokio::time::timeout(timeout, exchange).await {
            Ok(Ok(line)) => line,
            Ok(Err(error)) => {
                *session = None;
                return Err(error);
            }
            Err(_) => {
                *session = None;
                return Err(format!(
                    "{} session response exceeded {} seconds",
                    channel.name(),
                    timeout.as_secs()
                ));
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
        let disconnected =
            value.get("stage").and_then(Value::as_str) == Some("session_disconnected");
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
        if disconnected {
            *session = None;
        }
        Ok(value)
    }
}

#[tool_router]
impl RustDeskMcp {
    #[tool(
        name = "rustdesk_list_devices",
        description = "Read the live RustDesk peer TOML files now and return the current saved-device list. This tool never caches results and must be called immediately before every command or transfer."
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
        description = "Run one bounded command through the per-device reusable terminal session and return its real exit code, stdout, stderr, authenticated device identity, platform, duration, and whether the connection was reused. Call rustdesk_list_devices immediately first. The command may change remote state."
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
                Duration::from_secs(timeout_seconds + 15),
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
        description = "Upload one local regular file through the per-device reusable file-transfer session and return byte count, local SHA-256, and whether the connection was reused. Existing remote files are overwritten. Call rustdesk_list_devices immediately first."
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
            .call_session(
                SessionChannel::File,
                &params.device_id,
                json!({
                    "operation": "push",
                    "local_path": params.local_path.clone(),
                    "remote_path": params.remote_path.clone()
                }),
                FILE_TOOL_TIMEOUT,
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
        description = "Download one remote regular file through the per-device reusable file-transfer session and return byte count, downloaded SHA-256, and whether the connection was reused. Existing local files are overwritten. Call rustdesk_list_devices immediately first."
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
            .call_session(
                SessionChannel::File,
                &params.device_id,
                json!({
                    "operation": "pull",
                    "local_path": params.local_path.clone(),
                    "remote_path": params.remote_path.clone()
                }),
                FILE_TOOL_TIMEOUT,
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
    version = "0.4.0",
    instructions = "Always call rustdesk_list_devices immediately before every other RustDesk tool and use the exact device_id returned by that fresh call. Device listing reads local peer files and does not connect. Commands reuse one terminal session per device; uploads and downloads reuse a separate file-transfer session per device. A dead or idle session reconnects on the next call. Never replay an in-flight operation after a disconnect. Never reuse a cached device list, guess a menu index, request or log credentials, silently retry, or fall back to SSH."
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
}
