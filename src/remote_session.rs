use crate::{
    peer_from_login_response, pull_file, push_file, recv_raw, respond_to_test_delay, send_msg,
};
use anyhow::{bail, Context, Result};
use base64::Engine;
use hbb_common::{
    config::CONNECT_TIMEOUT,
    message_proto::*,
    protobuf::Message as ProtoMessage,
    tokio::{self, time},
    Stream,
};
use serde::{Deserialize, Serialize};
use std::{io::Write, path::PathBuf, time::Duration};

const MAX_EXEC_TIMEOUT: Duration = Duration::from_secs(300);
const MAX_EXEC_CAPTURE_BYTES: usize = 16 * 1024 * 1024;
const SESSION_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Serialize)]
pub(crate) struct ExecResult {
    pub(crate) ok: bool,
    pub(crate) operation: &'static str,
    pub(crate) device_id: String,
    pub(crate) hostname: String,
    pub(crate) platform: String,
    pub(crate) exit_code: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
    pub(crate) duration_ms: u128,
    pub(crate) stage: &'static str,
}

pub(crate) struct CapturedCommand {
    pub(crate) exit_code: i32,
    pub(crate) stdout: String,
    pub(crate) stderr: String,
    pub(crate) duration_ms: u128,
}

#[derive(Debug, Deserialize)]
struct CommandSessionRequest {
    command: String,
    timeout_seconds: u64,
}

#[derive(Debug, Deserialize)]
struct FileSessionRequest {
    operation: String,
    local_path: String,
    remote_path: String,
}

pub(crate) async fn execute_command(
    conn: &mut Stream,
    remote_platform: &str,
    command: &str,
    timeout: Duration,
) -> Result<CapturedCommand> {
    validate_exec_request(command, timeout)?;
    let terminal_id = 0;
    open_command_terminal(conn, terminal_id).await?;
    let result =
        execute_command_in_open_terminal(conn, remote_platform, terminal_id, command, timeout)
            .await;
    close_terminal(conn, terminal_id).await.ok();
    result
}

async fn execute_command_in_open_terminal(
    conn: &mut Stream,
    remote_platform: &str,
    terminal_id: i32,
    command: &str,
    timeout: Duration,
) -> Result<CapturedCommand> {
    validate_exec_request(command, timeout)?;

    let marker = format!("__RUSTSHELL_RESULT_{}__", uuid::Uuid::new_v4().simple());
    let remote_script = if remote_platform.to_ascii_lowercase().contains("windows") {
        windows_command_script(command, &marker)
    } else {
        posix_command_script(command, &marker)
    };
    let started = std::time::Instant::now();
    let deadline = time::Instant::now() + timeout;
    let mut keepalive = time::interval_at(
        time::Instant::now() + Duration::from_secs(15),
        Duration::from_secs(15),
    );
    let mut output = Vec::new();
    send_terminal_data(conn, terminal_id, remote_script.as_bytes()).await?;

    loop {
        tokio::select! {
            _ = time::sleep_until(deadline) => {
                bail!("[exec_wait_result] timeout after {} seconds", timeout.as_secs());
            }
            _ = keepalive.tick() => {
                conn.send(&Message::new())
                    .await
                    .context("[exec_keepalive] failed to keep session alive")?;
            }
            incoming = conn.next() => {
                let bytes = match incoming {
                    Some(Ok(bytes)) => bytes,
                    Some(Err(error)) => bail!("[exec_wait_result] stream error: {error}"),
                    None => bail!("[exec_wait_result] connection closed by peer"),
                };
                let message = Message::parse_from_bytes(&bytes)
                    .context("[exec_wait_result] invalid message")?;
                match message.union {
                    Some(message::Union::TerminalResponse(response)) => {
                        use terminal_response::Union;
                        match response.union {
                            Some(Union::Opened(opened)) => {
                                if !opened.success {
                                    bail!("Terminal open failed: {}", opened.message);
                                }
                            }
                            Some(Union::Data(data)) => {
                                let data = if data.compressed {
                                    hbb_common::compress::decompress(&data.data)
                                } else {
                                    data.data.to_vec()
                                };
                                if output.len().saturating_add(data.len()) > MAX_EXEC_CAPTURE_BYTES {
                                    bail!(
                                        "[exec_capture] terminal output exceeded {} bytes",
                                        MAX_EXEC_CAPTURE_BYTES
                                    );
                                }
                                output.extend_from_slice(&data);
                                if let Some((exit_code, stdout, stderr)) =
                                    parse_command_frame(&output, &marker)?
                                {
                                    return Ok(CapturedCommand {
                                        exit_code,
                                        stdout,
                                        stderr,
                                        duration_ms: started.elapsed().as_millis(),
                                    });
                                }
                            }
                            Some(Union::Closed(closed)) => bail!(
                                "[exec_wait_result] terminal closed before result (session exit code: {})",
                                closed.exit_code
                            ),
                            Some(Union::Error(error)) => {
                                bail!("Terminal error: {}", error.message);
                            }
                            _ => {}
                        }
                    }
                    Some(message::Union::TestDelay(delay)) => {
                        respond_to_test_delay(conn, delay).await?;
                    }
                    Some(message::Union::LoginResponse(response)) => {
                        peer_from_login_response(response)?;
                    }
                    Some(message::Union::MessageBox(message)) if message.msgtype == "error" => {
                        bail!("Remote command failed: {}", message.text);
                    }
                    _ => {}
                }
            }
        }
    }
}

fn validate_exec_request(command: &str, timeout: Duration) -> Result<()> {
    if timeout.is_zero() || timeout > MAX_EXEC_TIMEOUT {
        bail!(
            "Exec timeout must be between 1 and {} seconds",
            MAX_EXEC_TIMEOUT.as_secs()
        );
    }
    if command.trim().is_empty() {
        bail!("Exec command cannot be empty");
    }
    if command.len() > 16 * 1024 {
        bail!("Exec command exceeds the 16384-byte limit");
    }
    Ok(())
}

async fn open_command_terminal(conn: &mut Stream, terminal_id: i32) -> Result<()> {
    let mut action = TerminalAction::new();
    action.set_open(OpenTerminal {
        terminal_id,
        rows: 40,
        cols: 4096,
        ..Default::default()
    });
    let mut message = Message::new();
    message.set_terminal_action(action);
    send_msg(conn, &message, "exec_open_terminal").await?;

    let deadline = time::Instant::now() + Duration::from_millis(CONNECT_TIMEOUT);
    loop {
        let bytes = time::timeout_at(deadline, recv_raw(conn, "exec_wait_terminal_open"))
            .await
            .context("[exec_wait_terminal_open] timeout")??;
        let message = Message::parse_from_bytes(&bytes)
            .context("[exec_wait_terminal_open] invalid message")?;
        match message.union {
            Some(message::Union::TerminalResponse(response)) => {
                use terminal_response::Union;
                match response.union {
                    Some(Union::Opened(opened)) if opened.success => return Ok(()),
                    Some(Union::Opened(opened)) => {
                        bail!("Terminal open failed: {}", opened.message)
                    }
                    Some(Union::Closed(closed)) => bail!(
                        "Terminal closed while opening (session exit code: {})",
                        closed.exit_code
                    ),
                    Some(Union::Error(error)) => bail!("Terminal error: {}", error.message),
                    _ => {}
                }
            }
            Some(message::Union::TestDelay(delay)) => {
                respond_to_test_delay(conn, delay).await?;
            }
            Some(message::Union::LoginResponse(response)) => {
                peer_from_login_response(response)?;
            }
            Some(message::Union::MessageBox(message)) if message.msgtype == "error" => {
                bail!("Remote terminal failed: {}", message.text);
            }
            _ => {}
        }
    }
}

pub(crate) async fn command_session_loop(
    conn: &mut Stream,
    device_id: &str,
    peer: &PeerInfo,
) -> Result<()> {
    use tokio::io::AsyncBufReadExt;

    let terminal_id = 0;
    open_command_terminal(conn, terminal_id).await?;
    write_json_line(&serde_json::json!({
        "type": "ready",
        "protocol": 1,
        "channel": "terminal",
        "device_id": device_id,
        "hostname": peer.hostname,
        "platform": peer.platform,
        "idle_timeout_seconds": SESSION_IDLE_TIMEOUT.as_secs()
    }))?;

    let mut requests = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut last_command = time::Instant::now();
    let mut keepalive = time::interval_at(
        time::Instant::now() + Duration::from_secs(15),
        Duration::from_secs(15),
    );

    loop {
        tokio::select! {
            request = requests.next_line() => {
                let Some(request) = request.context("[session_stdin] failed to read request")? else {
                    close_terminal(conn, terminal_id).await.ok();
                    return Ok(());
                };
                let request: CommandSessionRequest = match serde_json::from_str(&request) {
                    Ok(request) => request,
                    Err(error) => {
                        write_json_line(&serde_json::json!({
                            "ok": false,
                            "stage": "validate_request",
                            "error": format!("invalid session request JSON: {error}")
                        }))?;
                        continue;
                    }
                };
                let timeout = Duration::from_secs(request.timeout_seconds);
                if let Err(error) = validate_exec_request(&request.command, timeout) {
                    write_json_line(&serde_json::json!({
                        "ok": false,
                        "stage": "validate_command",
                        "error": error.to_string()
                    }))?;
                    continue;
                }

                let result = execute_command_in_open_terminal(
                    conn,
                    &peer.platform,
                    terminal_id,
                    &request.command,
                    timeout,
                ).await;
                last_command = time::Instant::now();
                match result {
                    Ok(captured) => {
                        write_json_line(&ExecResult {
                            ok: captured.exit_code == 0,
                            operation: "exec",
                            device_id: device_id.to_owned(),
                            hostname: peer.hostname.clone(),
                            platform: peer.platform.clone(),
                            exit_code: captured.exit_code,
                            stdout: captured.stdout,
                            stderr: captured.stderr,
                            duration_ms: captured.duration_ms,
                            stage: "command_completed",
                        })?;
                    }
                    Err(error) => {
                        close_terminal(conn, terminal_id).await.ok();
                        write_json_line(&serde_json::json!({
                            "ok": false,
                            "operation": "exec",
                            "device_id": device_id,
                            "hostname": peer.hostname,
                            "platform": peer.platform,
                            "exit_code": -1,
                            "stdout": "",
                            "stderr": format!("{error:#}"),
                            "duration_ms": 0,
                            "stage": "session_disconnected",
                            "replayed": false,
                            "reconnect_on_next_call": true
                        }))?;
                        return Err(error);
                    }
                }
            }
            incoming = conn.next() => {
                let bytes = match incoming {
                    Some(Ok(bytes)) => bytes,
                    Some(Err(error)) => bail!("[session_idle] stream error: {error}"),
                    None => bail!("[session_idle] connection closed by peer"),
                };
                let message = Message::parse_from_bytes(&bytes)
                    .context("[session_idle] invalid message")?;
                match message.union {
                    Some(message::Union::TerminalResponse(response)) => {
                        use terminal_response::Union;
                        match response.union {
                            Some(Union::Closed(closed)) => bail!(
                                "[session_idle] terminal closed (session exit code: {})",
                                closed.exit_code
                            ),
                            Some(Union::Error(error)) => bail!("Terminal error: {}", error.message),
                            _ => {}
                        }
                    }
                    Some(message::Union::TestDelay(delay)) => {
                        respond_to_test_delay(conn, delay).await?;
                    }
                    Some(message::Union::LoginResponse(response)) => {
                        peer_from_login_response(response)?;
                    }
                    Some(message::Union::MessageBox(message)) if message.msgtype == "error" => {
                        bail!("Remote session failed: {}", message.text);
                    }
                    _ => {}
                }
            }
            _ = keepalive.tick() => {
                conn.send(&Message::new())
                    .await
                    .context("[session_keepalive] failed to keep session alive")?;
            }
            _ = time::sleep_until(last_command + SESSION_IDLE_TIMEOUT) => {
                close_terminal(conn, terminal_id).await.ok();
                return Ok(());
            }
        }
    }
}

async fn send_terminal_data(conn: &mut Stream, terminal_id: i32, script: &[u8]) -> Result<()> {
    let mut data = script.to_vec();
    data.push(b'\r');
    let mut action = TerminalAction::new();
    action.set_data(TerminalData {
        terminal_id,
        data: data.into(),
        compressed: false,
        ..Default::default()
    });
    let mut message = Message::new();
    message.set_terminal_action(action);
    send_msg(conn, &message, "exec_send_command").await
}

async fn close_terminal(conn: &mut Stream, terminal_id: i32) -> Result<()> {
    let mut action = TerminalAction::new();
    action.set_close(CloseTerminal {
        terminal_id,
        ..Default::default()
    });
    let mut message = Message::new();
    message.set_terminal_action(action);
    send_msg(conn, &message, "exec_close_terminal").await
}

fn write_json_line(value: &impl Serialize) -> Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, value).context("serialize session response")?;
    stdout.write_all(b"\n").context("write session response")?;
    stdout.flush().context("flush session response")
}

pub(crate) async fn file_session_loop(
    conn: &mut Stream,
    device_id: &str,
    peer: &PeerInfo,
) -> Result<()> {
    use tokio::io::AsyncBufReadExt;

    write_json_line(&serde_json::json!({
        "type": "ready",
        "protocol": 1,
        "channel": "file",
        "device_id": device_id,
        "hostname": peer.hostname,
        "platform": peer.platform,
        "idle_timeout_seconds": SESSION_IDLE_TIMEOUT.as_secs()
    }))?;

    let mut requests = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut last_operation = time::Instant::now();
    let mut keepalive = time::interval_at(
        time::Instant::now() + Duration::from_secs(15),
        Duration::from_secs(15),
    );

    loop {
        tokio::select! {
            request = requests.next_line() => {
                let Some(request) = request.context("[file_session_stdin] failed to read request")? else {
                    return Ok(());
                };
                let request: FileSessionRequest = match serde_json::from_str(&request) {
                    Ok(request) => request,
                    Err(error) => {
                        write_json_line(&serde_json::json!({
                            "ok": false,
                            "stage": "validate_request",
                            "error": format!("invalid file-session request JSON: {error}")
                        }))?;
                        continue;
                    }
                };
                if request.local_path.is_empty() || request.remote_path.is_empty() {
                    write_json_line(&serde_json::json!({
                        "ok": false,
                        "stage": "validate_path",
                        "error": "local_path and remote_path cannot be empty"
                    }))?;
                    continue;
                }

                let result = match request.operation.as_str() {
                    "push" => push_file(
                        conn,
                        PathBuf::from(&request.local_path),
                        request.remote_path.clone(),
                        &peer.version,
                    ).await,
                    "pull" => pull_file(
                        conn,
                        request.remote_path.clone(),
                        PathBuf::from(&request.local_path),
                        &peer.version,
                    ).await,
                    _ => {
                        write_json_line(&serde_json::json!({
                            "ok": false,
                            "stage": "validate_operation",
                            "error": "operation must be push or pull"
                        }))?;
                        continue;
                    }
                };
                last_operation = time::Instant::now();
                match result {
                    Ok(()) => {
                        write_json_line(&serde_json::json!({
                            "ok": true,
                            "operation": request.operation,
                            "device_id": device_id,
                            "hostname": peer.hostname,
                            "platform": peer.platform,
                            "local_path": request.local_path,
                            "remote_path": request.remote_path,
                            "stage": "transfer_completed"
                        }))?;
                    }
                    Err(error) => {
                        write_json_line(&serde_json::json!({
                            "ok": false,
                            "operation": request.operation,
                            "device_id": device_id,
                            "hostname": peer.hostname,
                            "platform": peer.platform,
                            "local_path": request.local_path,
                            "remote_path": request.remote_path,
                            "stage": "session_disconnected",
                            "error": format!("{error:#}"),
                            "replayed": false,
                            "reconnect_on_next_call": true
                        }))?;
                        return Err(error);
                    }
                }
            }
            incoming = conn.next() => {
                let bytes = match incoming {
                    Some(Ok(bytes)) => bytes,
                    Some(Err(error)) => bail!("[file_session_idle] stream error: {error}"),
                    None => bail!("[file_session_idle] connection closed by peer"),
                };
                let message = Message::parse_from_bytes(&bytes)
                    .context("[file_session_idle] invalid message")?;
                match message.union {
                    Some(message::Union::TestDelay(delay)) => {
                        respond_to_test_delay(conn, delay).await?;
                    }
                    Some(message::Union::LoginResponse(response)) => {
                        peer_from_login_response(response)?;
                    }
                    Some(message::Union::MessageBox(message)) if message.msgtype == "error" => {
                        bail!("Remote file session failed: {}", message.text);
                    }
                    _ => {}
                }
            }
            _ = keepalive.tick() => {
                conn.send(&Message::new())
                    .await
                    .context("[file_session_keepalive] failed to keep session alive")?;
            }
            _ = time::sleep_until(last_operation + SESSION_IDLE_TIMEOUT) => {
                return Ok(());
            }
        }
    }
}

fn posix_command_script(command: &str, marker: &str) -> String {
    format!(
        "unset HISTFILE; set +e; \
         __rs_o=$(mktemp \"${{TMPDIR:-/tmp}}/rustshell-out.XXXXXX\"); \
         __rs_e=$(mktemp \"${{TMPDIR:-/tmp}}/rustshell-err.XXXXXX\"); \
         /bin/sh -c {} >\"$__rs_o\" 2>\"$__rs_e\"; __rs_c=$?; \
         printf '\\n{}:%s:' \"$__rs_c\"; \
         base64 <\"$__rs_o\" | tr -d '\\r\\n'; printf ':'; \
         base64 <\"$__rs_e\" | tr -d '\\r\\n'; \
         rm -f \"$__rs_o\" \"$__rs_e\"; printf ':END\\n'",
        posix_single_quote(command),
        marker
    )
}

fn posix_single_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn windows_command_script(command: &str, marker: &str) -> String {
    let command_base64 = base64::engine::general_purpose::STANDARD.encode(command.as_bytes());
    let user_script = format!(
        "$ErrorActionPreference='Continue'; $ProgressPreference='SilentlyContinue'; \
         $__rs_utf8=New-Object Text.UTF8Encoding $false; \
         [Console]::OutputEncoding=$__rs_utf8; [Console]::InputEncoding=$__rs_utf8; \
         $OutputEncoding=$__rs_utf8; \
         $global:LASTEXITCODE=$null; \
         $__rs_cmd=[Text.Encoding]::UTF8.GetString([Convert]::FromBase64String('{command_base64}')); \
         try {{ & ([ScriptBlock]::Create($__rs_cmd)); \
         if ($null -ne $LASTEXITCODE) {{ exit $LASTEXITCODE }}; \
         if ($?) {{ exit 0 }} else {{ exit 1 }} }} \
         catch {{ [Console]::Error.WriteLine($_.ToString()); exit 1 }}"
    );
    let encoded_user_script = encode_powershell(&user_script);
    let outer_script = format!(
        "$ErrorActionPreference='Continue'; \
         $__rs_o=[IO.Path]::GetTempFileName(); $__rs_e=[IO.Path]::GetTempFileName(); \
         $__rs_args=@('-NoLogo','-NoProfile','-NonInteractive','-OutputFormat','Text','-EncodedCommand','{encoded_user_script}'); \
         $__rs_p=Start-Process -FilePath 'powershell.exe' -ArgumentList $__rs_args \
         -RedirectStandardOutput $__rs_o -RedirectStandardError $__rs_e -Wait -PassThru -NoNewWindow; \
         $__rs_c=$__rs_p.ExitCode; \
         $__rs_ot=[IO.File]::ReadAllText($__rs_o); $__rs_et=[IO.File]::ReadAllText($__rs_e); \
         if ($__rs_et.StartsWith('#< CLIXML')) {{ try {{ \
         $__rs_xml=[xml]($__rs_et -replace '^#< CLIXML\\r?\\n',''); \
         $__rs_parts=@($__rs_xml.Objs.ChildNodes | Where-Object {{ $_.Name -eq 'S' -and $_.GetAttribute('S') -eq 'Error' }} | ForEach-Object {{ $_.InnerText }}); \
         if ($__rs_parts.Count -gt 0) {{ $__rs_et=$__rs_parts -join ''; \
         $__rs_et=$__rs_et -replace '_x000D__x000A_',\"`r`n\" -replace '_x000A_',\"`n\" -replace '_x000D_',\"`r\" -replace '_x005F_','_' }} \
         }} catch {{ }} }}; \
         $__rs_ob=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($__rs_ot)); \
         $__rs_eb=[Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($__rs_et)); \
         Remove-Item -Force $__rs_o,$__rs_e; \
         [Console]::Out.WriteLine(\"`n{marker}:{{0}}:{{1}}:{{2}}:END\",$__rs_c,$__rs_ob,$__rs_eb)"
    );
    format!(
        "powershell.exe -NoLogo -NoProfile -NonInteractive -EncodedCommand {}",
        encode_powershell(&outer_script)
    )
}

fn encode_powershell(script: &str) -> String {
    let bytes: Vec<u8> = script
        .encode_utf16()
        .flat_map(|value| value.to_le_bytes())
        .collect();
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn parse_command_frame(data: &[u8], marker: &str) -> Result<Option<(i32, String, String)>> {
    let text = String::from_utf8_lossy(data);
    let prefix = format!("{marker}:");
    for (index, _) in text.rmatch_indices(prefix.as_str()) {
        let payload = &text[index + prefix.len()..];
        let Some(end) = payload.find(":END") else {
            continue;
        };
        let mut fields = payload[..end].splitn(3, ':');
        let Some(exit_code) = fields.next() else {
            continue;
        };
        let Some(stdout) = fields.next() else {
            continue;
        };
        let Some(stderr) = fields.next() else {
            continue;
        };
        let Ok(exit_code) = exit_code.parse::<i32>() else {
            continue;
        };
        let stdout = base64::engine::general_purpose::STANDARD
            .decode(stdout)
            .context("[exec_parse] invalid stdout encoding")?;
        let stderr = base64::engine::general_purpose::STANDARD
            .decode(stderr)
            .context("[exec_parse] invalid stderr encoding")?;
        return Ok(Some((
            exit_code,
            String::from_utf8_lossy(&stdout).into_owned(),
            String::from_utf8_lossy(&stderr).into_owned(),
        )));
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_structured_command_frame_after_echoed_marker() {
        let marker = "__RUSTSHELL_RESULT_test__";
        let frame = format!("echo printf {marker}:%s:%s\r\n{marker}:7:aGVsbG8=:YmFk:END\r\n");
        let parsed = parse_command_frame(frame.as_bytes(), marker)
            .unwrap()
            .expect("frame");

        assert_eq!(parsed.0, 7);
        assert_eq!(parsed.1, "hello");
        assert_eq!(parsed.2, "bad");
    }

    #[test]
    fn posix_quotes_single_quotes() {
        assert_eq!(posix_single_quote("it's"), "'it'\"'\"'s'");
    }

    #[test]
    fn powershell_encoding_is_utf16le_base64() {
        let encoded = encode_powershell("A");
        assert_eq!(encoded, "QQA=");
    }
}
