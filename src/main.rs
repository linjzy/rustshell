use anyhow::{bail, Context, Result};
use clap::{Parser, Subcommand};
use hbb_common::{
    bytes,
    config::{CONNECT_TIMEOUT, RELAY_PORT, RS_PUB_KEY},
    fs, log,
    message_proto::*,
    protobuf::Message as ProtoMessage,
    rendezvous_proto::{
        ConnType, KeyExchange, NatType, PunchHoleRequest, RequestRelay, RendezvousMessage,
    },
    socket_client,
    sodiumoxide::crypto::{box_, secretbox, sign},
    tokio::{self, time},
    Stream,
};
use sha2::{Digest, Sha256};
use std::{fmt, io::Write, path::PathBuf, time::Duration};

mod mcp;
mod remote_session;

use remote_session::{command_session_loop, execute_command, file_session_loop, ExecResult};

const APP_NAME: &str = "RustShell";
const APP_VERSION: &str = env!("CARGO_PKG_VERSION");
const RUSTDESK_PROTOCOL_VERSION: &str = "1.4.9";
const FILE_JOB_ID: i32 = 1;
const FILE_RESUME_MIN_VERSION: &str = "1.4.2";
const TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(5 * 60);

#[derive(Debug)]
struct TransferDisconnected {
    detail: String,
    progress_bytes: u64,
}

impl fmt::Display for TransferDisconnected {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.detail)
    }
}

impl std::error::Error for TransferDisconnected {}

#[derive(Debug)]
pub(crate) struct FileTransferStats {
    pub(crate) bytes: u64,
    pub(crate) resumed_from: u64,
}

fn disconnected(operation: &str, detail: impl fmt::Display) -> anyhow::Error {
    disconnected_at(operation, detail, 0)
}

fn disconnected_at(
    operation: &str,
    detail: impl fmt::Display,
    progress_bytes: u64,
) -> anyhow::Error {
    TransferDisconnected {
        detail: format!("[{operation}] {detail}"),
        progress_bytes,
    }
    .into()
}

pub(crate) fn is_transfer_disconnected(error: &anyhow::Error) -> bool {
    error.downcast_ref::<TransferDisconnected>().is_some()
}

pub(crate) fn transfer_disconnect_progress(error: &anyhow::Error) -> Option<u64> {
    error
        .downcast_ref::<TransferDisconnected>()
        .map(|error| error.progress_bytes)
}

pub(crate) fn file_resume_supported(remote_version: &str) -> bool {
    hbb_common::get_version_number(remote_version)
        >= hbb_common::get_version_number(FILE_RESUME_MIN_VERSION)
}

// ── CLI arguments ──────────────────────────────────────────────────

#[derive(Parser, Debug)]
#[command(
    name = APP_NAME,
    version = APP_VERSION,
    about = "Cross-platform remote shell via RustDesk protocol 1.4.9",
    after_help = "Environment variables (fallback when CLI arg not set):\n  \
                  RUSTSHELL_ID, RUSTSHELL_SERVER, RUSTSHELL_PORT, RUSTSHELL_KEY, \
                  RUSTSHELL_PASSWORD, RUSTSHELL_QUIT_KEY=(a-z), RUSTSHELL_DEBUG=(1|true)\n\n  \
                  With no subcommand, RustShell opens an interactive terminal."
)]
struct Args {
    #[arg(short = 'i', long, default_value = "")] id: String,
    #[arg(short = 's', long, default_value = "")] server: String,
    #[arg(short = 'p', long, default_value = "21116")] port: u16,
    #[arg(short = 'k', long, default_value = "")] key: String,
    #[arg(short = 'w', long, default_value = "")] password: String,
    #[arg(short = 'd', long, default_value = "false")] debug: bool,
    #[arg(short = 'q', long, default_value = "q")] quit_key: char,
    #[command(subcommand)] command: Option<Command>,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Run one bounded command and print a structured JSON result.
    Exec {
        #[arg(long, default_value_t = 60)]
        timeout: u64,
        command: String,
    },
    /// Keep one authenticated terminal connection open and exchange JSON lines on stdio.
    #[command(hide = true)]
    Session,
    /// Keep one authenticated file-transfer connection open and exchange JSON lines on stdio.
    #[command(hide = true)]
    FileSession,
    /// Upload one local file to an exact path on the remote device.
    Push { local: PathBuf, remote: String },
    /// Download one remote file to an exact path on this device.
    Pull { remote: String, local: PathBuf },
    /// Serve RustDesk tools over MCP stdio using the local wrapper for configuration.
    Mcp {
        #[arg(long)]
        wrapper: Option<PathBuf>,
    },
}

enum RunOutcome {
    Completed,
    Command(ExecResult),
}

// ── Crypto helpers ─────────────────────────────────────────────────

fn get_pk(pk: &[u8]) -> Option<[u8; 32]> {
    if pk.len() == 32 {
        let mut tmp = [0u8; 32];
        tmp[..].copy_from_slice(pk);
        Some(tmp)
    } else { None }
}

fn get_rs_pk(str_base64: &str) -> Option<sign::PublicKey> {
    use base64::Engine;
    get_pk(&base64::engine::general_purpose::STANDARD.decode(str_base64).ok()?).map(sign::PublicKey)
}

fn decode_id_pk(signed: &[u8], key: &sign::PublicKey) -> Result<(String, [u8; 32])> {
    let raw = sign::verify(signed, key).map_err(|_| anyhow::anyhow!("Signature mismatch"))?;
    let id_pk = IdPk::parse_from_bytes(&raw)?;
    let pk = get_pk(&id_pk.pk).ok_or_else(|| anyhow::anyhow!("Wrong public key length"))?;
    Ok((id_pk.id, pk))
}

fn create_symmetric_key_msg(their_pk_b: [u8; 32]) -> (Vec<u8>, Vec<u8>, secretbox::Key) {
    let their_pk_b = box_::PublicKey(their_pk_b);
    let (our_pk_b, our_sk_b) = box_::gen_keypair();
    let key = secretbox::gen_key();
    let nonce = box_::Nonce([0u8; box_::NONCEBYTES]);
    let sealed_key = box_::seal(&key.0, &nonce, &their_pk_b, &our_sk_b);
    (our_pk_b.0.to_vec(), sealed_key, key)
}

// ── Key event encoding ─────────────────────────────────────────────

use crossterm::event::{KeyCode, KeyModifiers};

fn key_event_to_bytes(code: KeyCode, modifiers: KeyModifiers) -> Vec<u8> {
    let ctrl = modifiers.contains(KeyModifiers::CONTROL);
    let alt = modifiers.contains(KeyModifiers::ALT);
    match code {
        KeyCode::Char(c) => {
            if ctrl {
                let c_lower = c.to_ascii_lowercase();
                if c_lower.is_ascii_lowercase() { vec![(c_lower as u8) - b'a' + 1] }
                else {
                    match c_lower {
                        '[' => vec![0x1b], ']' => vec![0x1d],
                        '\\' => vec![0x1c], '^' => vec![0x1e],
                        _ => vec![c as u8],
                    }
                }
            } else if alt {
                let mut v = vec![0x1b];
                let mut buf = [0u8; 4];
                let s = c.encode_utf8(&mut buf);
                v.extend_from_slice(s.as_bytes());
                v
            } else {
                let mut buf = [0u8; 4];
                let s = c.encode_utf8(&mut buf);
                s.as_bytes().to_vec()
            }
        }
        KeyCode::Enter => vec![b'\r'],       KeyCode::Backspace => vec![0x7f],
        KeyCode::Tab => vec![b'\t'],          KeyCode::Esc => vec![0x1b],
        KeyCode::Up => vec![0x1b, b'[', b'A'], KeyCode::Down => vec![0x1b, b'[', b'B'],
        KeyCode::Right => vec![0x1b, b'[', b'C'], KeyCode::Left => vec![0x1b, b'[', b'D'],
        KeyCode::Home => vec![0x1b, b'[', b'H'], KeyCode::End => vec![0x1b, b'[', b'F'],
        KeyCode::PageUp => vec![0x1b, b'[', b'5', b'~'], KeyCode::PageDown => vec![0x1b, b'[', b'6', b'~'],
        KeyCode::Delete => vec![0x1b, b'[', b'3', b'~'], KeyCode::Insert => vec![0x1b, b'[', b'2', b'~'],
        KeyCode::F(1) => vec![0x1b, b'O', b'P'], KeyCode::F(2) => vec![0x1b, b'O', b'Q'],
        KeyCode::F(3) => vec![0x1b, b'O', b'R'], KeyCode::F(4) => vec![0x1b, b'O', b'S'],
        KeyCode::F(5) => vec![0x1b, b'[', b'1', b'5', b'~'], KeyCode::F(6) => vec![0x1b, b'[', b'1', b'7', b'~'],
        KeyCode::F(7) => vec![0x1b, b'[', b'1', b'8', b'~'], KeyCode::F(8) => vec![0x1b, b'[', b'1', b'9', b'~'],
        KeyCode::F(9) => vec![0x1b, b'[', b'2', b'0', b'~'], KeyCode::F(10) => vec![0x1b, b'[', b'2', b'1', b'~'],
        KeyCode::F(11) => vec![0x1b, b'[', b'2', b'3', b'~'], KeyCode::F(12) => vec![0x1b, b'[', b'2', b'4', b'~'],
        _ => vec![],
    }
}

// ── Windows console helpers ────────────────────────────────────────

#[cfg(windows)]
mod win_console {
    extern "system" {
        pub fn GetStdHandle(nStdHandle: u32) -> isize;
        pub fn GetConsoleMode(handle: isize, mode: *mut u32) -> i32;
        pub fn SetConsoleMode(handle: isize, mode: u32) -> i32;
        pub fn SetConsoleCP(code_page: u32) -> i32;
        pub fn SetConsoleOutputCP(code_page: u32) -> i32;
    }
    pub const STD_OUTPUT_HANDLE: u32 = (-11i32) as u32;
    pub const ENABLE_VIRTUAL_TERMINAL_PROCESSING: u32 = 0x0004;
    pub const DISABLE_NEWLINE_AUTO_RETURN: u32 = 0x0008;
}

/// Write bytes to stdout.
fn write_stdout(data: &[u8]) {
    let mut stdout = std::io::stdout();
    stdout.write_all(data).ok();
    stdout.flush().ok();
}

// ── Terminal setup ─────────────────────────────────────────────────

struct ConsoleGuard;
impl ConsoleGuard {
    fn enable() -> Result<Self> {
        crossterm::terminal::enable_raw_mode()
            .context("Failed to enable raw mode")?;
        // On Windows, enable VT100 processing on output.
        // This lets WriteFile (stdout) handle UTF-8 + escape sequences
        // natively, matching Unix terminal behavior.
        #[cfg(windows)]
        unsafe {
            let handle = win_console::GetStdHandle(win_console::STD_OUTPUT_HANDLE);
            let mut mode: u32 = 0;
            if win_console::GetConsoleMode(handle, &mut mode) != 0 {
                win_console::SetConsoleMode(handle, mode
                    | win_console::ENABLE_VIRTUAL_TERMINAL_PROCESSING
                    | win_console::DISABLE_NEWLINE_AUTO_RETURN);
            }
        }
        Ok(Self)
    }
}
impl Drop for ConsoleGuard {
    fn drop(&mut self) {
        let _ = crossterm::terminal::disable_raw_mode();
    }
}

/// Poll keyboard input (cross-platform, uses crossterm).
fn poll_key_event() -> Option<Vec<u8>> {
    use crossterm::event::{self, Event, KeyEventKind};
    if !event::poll(std::time::Duration::from_millis(0)).unwrap_or(false) {
        return None;
    }
    match event::read() {
        Ok(Event::Key(key_event)) if key_event.kind != KeyEventKind::Release => {
            Some(key_event_to_bytes(key_event.code, key_event.modifiers))
        }
        _ => None,
    }
}

// ── Stream helpers ─────────────────────────────────────────────────

async fn recv_raw(conn: &mut Stream, step: &str) -> Result<bytes::BytesMut> {
    match conn.next().await {
        Some(Ok(b)) => { log::debug!("[{step}] received {} bytes", b.len()); Ok(b) }
        Some(Err(e)) => bail!("[{step}] stream error: {e}"),
        None => bail!("[{step}] connection closed by peer"),
    }
}

async fn recv_msg(conn: &mut Stream, step: &str) -> Result<Message> {
    let bytes = recv_raw(conn, step).await?;
    Message::parse_from_bytes(&bytes)
        .with_context(|| format!("[{step}] failed to parse Message"))
}

async fn recv_rendezvous_msg(conn: &mut Stream, step: &str) -> Result<RendezvousMessage> {
    let bytes = recv_raw(conn, step).await?;
    RendezvousMessage::parse_from_bytes(&bytes)
        .with_context(|| format!("[{step}] failed to parse RendezvousMessage"))
}

async fn send_msg(conn: &mut Stream, msg: &impl ProtoMessage, step: &str) -> Result<()> {
    hbb_common::timeout(CONNECT_TIMEOUT, conn.send(msg)).await
        .with_context(|| format!("[{step}] timeout sending message"))??;
    log::debug!("[{step}] sent message");
    Ok(())
}

// ── Main ───────────────────────────────────────────────────────────

fn main() {
    // Windows: set console to UTF-8 codepage
    #[cfg(windows)]
    unsafe {
        win_console::SetConsoleCP(65001);
        win_console::SetConsoleOutputCP(65001);
    }

    let mut args = Args::parse();

    if let Some(Command::Mcp { wrapper }) = &args.command {
        let wrapper = wrapper
            .clone()
            .or_else(|| std::env::var_os("RUSTSHELL_WRAPPER").map(PathBuf::from))
            .unwrap_or_else(|| PathBuf::from("rustshell.sh"));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .enable_all().build().expect("tokio runtime");
        if let Err(error) = rt.block_on(mcp::serve(wrapper)) {
            eprintln!("Error: {error:#}");
            std::process::exit(1);
        }
        return;
    }

    // Fill empty fields from RUSTSHELL_* environment variables
    if args.id.is_empty() { args.id = std::env::var("RUSTSHELL_ID").unwrap_or_default(); }
    if args.server.is_empty() { args.server = std::env::var("RUSTSHELL_SERVER").unwrap_or_default(); }
    if args.port == 21116 { if let Ok(v) = std::env::var("RUSTSHELL_PORT") { if let Ok(p) = v.parse() { args.port = p; } } }
    if args.key.is_empty() { args.key = std::env::var("RUSTSHELL_KEY").unwrap_or_default(); }
    if !args.debug { args.debug = std::env::var("RUSTSHELL_DEBUG").map(|v| v == "1" || v.eq_ignore_ascii_case("true")).unwrap_or(false); }
    if args.password.is_empty() { args.password = std::env::var("RUSTSHELL_PASSWORD").unwrap_or_default(); }
    if args.quit_key == 'q' { if let Ok(v) = std::env::var("RUSTSHELL_QUIT_KEY") { if let Some(c) = v.chars().next() { args.quit_key = c; } } }

    if args.id.is_empty() { eprintln!("Error: --id or RUSTSHELL_ID is required"); std::process::exit(1); }
    if args.server.is_empty() { eprintln!("Error: --server or RUSTSHELL_SERVER is required"); std::process::exit(1); }
    if !args.quit_key.is_ascii_alphabetic() { eprintln!("Error: --quit-key must be an ASCII letter a-z"); std::process::exit(1); }

    let log_level = if args.debug { "debug" } else { "info" };
    hbb_common::env_logger::init_from_env(
        hbb_common::env_logger::Env::default()
            .filter_or(hbb_common::env_logger::DEFAULT_FILTER_ENV, log_level),
    );

    let password = if args.password.is_empty() {
        match rpassword::prompt_password("Enter password: ") {
            Ok(p) => p,
            Err(e) => { eprintln!("Failed to read password: {}", e); std::process::exit(1); }
        }
    } else { args.password };

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all().build().expect("tokio runtime");

    match rt.block_on(run(args.id, args.key, args.server, args.port, password, args.quit_key, args.command)) {
        Ok(RunOutcome::Completed) => {}
        Ok(RunOutcome::Command(result)) => {
            println!("{}", serde_json::to_string(&result).expect("serialize command result"));
            if result.exit_code != 0 {
                let exit_code = if (1..=255).contains(&result.exit_code) { result.exit_code } else { 1 };
                std::process::exit(exit_code);
            }
        }
        Err(e) => {
            let _ = crossterm::terminal::disable_raw_mode();
            eprintln!("Error: {:#}", e);
            std::process::exit(1);
        }
    }
}

async fn run(
    device_id: String, licence_key: String,
    server: String, port: u16, password: String,
    quit_key: char,
    command: Option<Command>,
) -> Result<RunOutcome> {
    let file_transfer = matches!(command.as_ref(), Some(Command::Push { .. } | Command::Pull { .. } | Command::FileSession));
    let conn_type = if file_transfer { ConnType::FILE_TRANSFER } else { ConnType::TERMINAL };
    let rendezvous_addr = format!("{}:{}", server, port);
    log::info!("Connecting to rendezvous server {}...", rendezvous_addr);

    // Phase 1: Connect to rendezvous server
    let mut socket = socket_client::connect_tcp(rendezvous_addr.clone(), CONNECT_TIMEOUT).await
        .with_context(|| format!("Failed to connect to {}", rendezvous_addr))?;
    log::info!("TCP connected to rendezvous server");

    let key_str: &str = if licence_key.is_empty() { RS_PUB_KEY } else { &licence_key };
    attempt_secure_tcp(&mut socket, key_str).await?;

    // Send PunchHoleRequest
    let mut msg_out = RendezvousMessage::new();
    msg_out.set_punch_hole_request(PunchHoleRequest {
        id: device_id.clone(), licence_key: licence_key.clone(),
        conn_type: conn_type.into(),
        nat_type: NatType::SYMMETRIC.into(), force_relay: false,
        version: RUSTDESK_PROTOCOL_VERSION.to_owned(), ..Default::default()
    });
    log::info!("Requesting connection to device {}...", device_id);
    send_msg(&mut socket, &msg_out, "punch_hole_request").await?;

    // Wait for response
    let rmsg = recv_rendezvous_msg(&mut socket, "wait_rendezvous_response").await?;
    let (peer_pk_from_server, relay_server, relay_uuid, try_direct) = match rmsg.union {
        Some(hbb_common::rendezvous_proto::rendezvous_message::Union::PunchHoleResponse(ph)) => {
            if !ph.socket_addr.is_empty() {
                let addr = hbb_common::AddrMangle::decode(&ph.socket_addr);
                let relay = if ph.relay_server.is_empty() {
                    socket_client::increase_port(&rendezvous_addr, 1)
                } else { socket_client::check_port(ph.relay_server.clone(), RELAY_PORT) };
                log::info!("Peer address: {} (local: {}), relay fallback: {}", addr, ph.is_local(), relay);
                (ph.pk.to_vec(), relay, String::new(), Some(addr))
            } else {
                use hbb_common::rendezvous_proto::punch_hole_response::Failure;
                let reason = match ph.failure.enum_value() {
                    Ok(Failure::ID_NOT_EXIST) => "ID does not exist",
                    Ok(Failure::OFFLINE) => "Remote device is offline",
                    Ok(Failure::LICENSE_MISMATCH) => "Key mismatch",
                    Ok(Failure::LICENSE_OVERUSE) => "Key overuse",
                    _ => &ph.other_failure,
                };
                bail!("Connection refused: {}", reason);
            }
        }
        Some(hbb_common::rendezvous_proto::rendezvous_message::Union::RelayResponse(rr)) => {
            let relay = if rr.relay_server.is_empty() {
                socket_client::increase_port(&rendezvous_addr, 1)
            } else { socket_client::check_port(rr.relay_server, RELAY_PORT) };
            log::info!("Relay assigned: {} (uuid: {})", relay, rr.uuid);
            let pk = match rr.union {
                Some(hbb_common::rendezvous_proto::relay_response::Union::Pk(pk)) => pk.to_vec(),
                _ => Vec::new(),
            };
            (pk, relay, rr.uuid, None)
        }
        other => bail!("Unexpected response: {:?}", other.map(|_| "unknown")),
    };

    // Phase 2: Connect — try direct first, fall back to relay
    let mut conn = if let Some(addr) = try_direct {
        let direct_addr = format!("{}:{}", addr.ip(), addr.port());
        log::info!("Trying direct connection to {}...", direct_addr);
        match socket_client::connect_tcp(direct_addr, CONNECT_TIMEOUT).await {
            Ok(c) => {
                log::info!("Direct connection established");
                c
            }
            Err(e) => {
                log::info!("Direct failed ({}), falling back to relay {}", e, relay_server);
                let mut c = socket_client::connect_tcp(relay_server.clone(), CONNECT_TIMEOUT).await
                    .with_context(|| format!("Failed to connect to relay {}", relay_server))?;
                // Send RequestRelay for relay
                let mut msg_out = RendezvousMessage::new();
                msg_out.set_request_relay(RequestRelay {
                    id: device_id.clone(), uuid: relay_uuid,
                    licence_key: licence_key.clone(),
                    conn_type: conn_type.into(), ..Default::default()
                });
                send_msg(&mut c, &msg_out, "request_relay").await?;
                c
            }
        }
    } else {
        log::info!("Connecting via relay server {}...", relay_server);
        let mut c = socket_client::connect_tcp(relay_server.clone(), CONNECT_TIMEOUT).await
            .with_context(|| format!("Failed to connect to relay {}", relay_server))?;
        let mut msg_out = RendezvousMessage::new();
        msg_out.set_request_relay(RequestRelay {
            id: device_id.clone(), uuid: relay_uuid,
            licence_key: licence_key.clone(),
            conn_type: conn_type.into(), ..Default::default()
        });
        send_msg(&mut c, &msg_out, "request_relay").await?;
        c
    };

    // Phase 3: E2E key exchange
    let rs_pk = get_rs_pk(key_str).context("Invalid rendezvous server key")?;
    let peer_sign_pk = if !peer_pk_from_server.is_empty() {
        let (vouched_id, pk) = decode_id_pk(&peer_pk_from_server, &rs_pk)
            .context("Failed to verify peer key from rendezvous")?;
        if vouched_id != device_id {
            bail!("Rendezvous vouched for device {vouched_id}, expected {device_id}");
        }
        log::debug!("Peer key vouched: {}", vouched_id);
        Some(sign::PublicKey(pk))
    } else { None };

    let msg_in = recv_msg(&mut conn, "wait_signed_id").await?;
    let signed_id = match msg_in.union {
        Some(message::Union::SignedId(si)) => si,
        other => bail!("Expected SignedId, got: {:?}", other.map(|_| "other")),
    };
    let peer_sign_pk = peer_sign_pk
        .ok_or_else(|| anyhow::anyhow!("No peer public key from rendezvous server"))?;
    let (peer_id, their_pk) = decode_id_pk(&signed_id.id, &peer_sign_pk)?;
    if peer_id != device_id {
        bail!("Connected peer identity is {peer_id}, expected {device_id}");
    }
    log::info!("Peer identity verified: {}", peer_id);

    let (av, sv, enc_key) = create_symmetric_key_msg(their_pk);
    let mut pk_msg = Message::new();
    pk_msg.set_public_key(PublicKey { asymmetric_value: av.into(), symmetric_value: sv.into(), ..Default::default() });
    send_msg(&mut conn, &pk_msg, "public_key").await?;
    conn.set_key(enc_key);
    log::info!("End-to-end encryption established");

    // Phase 4: Password authentication
    let msg_in = recv_msg(&mut conn, "wait_hash").await?;
    let hash = match msg_in.union {
        Some(message::Union::Hash(h)) => h,
        _ => bail!("Expected Hash challenge"),
    };
    let mut h1 = Sha256::new();
    h1.update(password.as_bytes()); h1.update(hash.salt.as_bytes());
    let mut h2 = Sha256::new();
    h2.update(&h1.finalize()[..]); h2.update(hash.challenge.as_bytes());
    let pw_response: Vec<u8> = h2.finalize()[..].into();

    // Phase 5: Login with the requested RustDesk service.
    let mut lr = LoginRequest::new();
    lr.username = device_id.clone();
    lr.password = pw_response.into();
    lr.my_id = format!("RustShell-{}", std::process::id());
    lr.version = RUSTDESK_PROTOCOL_VERSION.to_owned();
    lr.my_platform = std::env::consts::OS.to_owned();
    if file_transfer {
        lr.set_file_transfer(FileTransfer::new());
    } else {
        let mut terminal = Terminal::new();
        terminal.service_id = format!("ts_{}", uuid::Uuid::new_v4());
        lr.set_terminal(terminal);
    }
    let mut lr_msg = Message::new();
    lr_msg.set_login_request(lr);
    send_msg(&mut conn, &lr_msg, "login_request").await?;
    log::info!("Login request sent");

    let peer = wait_for_login(&mut conn).await?;
    log::info!(
        "Connected to {} ({} {})",
        peer.hostname,
        peer.platform,
        peer.version
    );

    match command {
        None => {
            terminal_io_loop(&mut conn, &peer.platform, quit_key).await?;
            Ok(RunOutcome::Completed)
        }
        Some(Command::Exec { timeout, command }) => {
            let captured = execute_command(&mut conn, &peer.platform, &command, Duration::from_secs(timeout)).await?;
            Ok(RunOutcome::Command(ExecResult {
                ok: captured.exit_code == 0,
                operation: "exec",
                device_id,
                hostname: peer.hostname,
                platform: peer.platform,
                exit_code: captured.exit_code,
                stdout: captured.stdout,
                stderr: captured.stderr,
                duration_ms: captured.duration_ms,
                stage: "command_completed",
            }))
        }
        Some(Command::Session) => {
            command_session_loop(&mut conn, &device_id, &peer).await?;
            Ok(RunOutcome::Completed)
        }
        Some(Command::FileSession) => {
            file_session_loop(&mut conn, &device_id, &peer).await?;
            Ok(RunOutcome::Completed)
        }
        Some(Command::Push { local, remote }) => {
            push_file(&mut conn, local, remote, &peer.version).await?;
            Ok(RunOutcome::Completed)
        }
        Some(Command::Pull { remote, local }) => {
            pull_file(&mut conn, remote, local, &peer.version).await?;
            Ok(RunOutcome::Completed)
        }
        Some(Command::Mcp { .. }) => unreachable!("MCP is handled before remote validation"),
    }
}

async fn wait_for_login(conn: &mut Stream) -> Result<PeerInfo> {
    let deadline = time::Instant::now() + Duration::from_millis(CONNECT_TIMEOUT);
    loop {
        let bytes = time::timeout_at(deadline, recv_raw(conn, "wait_login_response"))
            .await
            .context("[wait_login_response] timeout")??;
        log::debug!(
            "Login response raw bytes ({}): {:02x?}",
            bytes.len(),
            bytes.as_ref()
        );

        match Message::parse_from_bytes(&bytes) {
            Ok(message) => match message.union {
                Some(message::Union::LoginResponse(response)) => {
                    if let Some(peer) = peer_from_login_response(response)? {
                        return Ok(peer);
                    }
                }
                Some(message::Union::TestDelay(delay)) => {
                    respond_to_test_delay(conn, delay).await?;
                }
                other => {
                    log::debug!(
                        "Ignoring message before authenticated PeerInfo: {:?}",
                        other.map(|_| ())
                    );
                }
            },
            Err(message_error) => {
                let response = LoginResponse::parse_from_bytes(&bytes)
                    .with_context(|| format!("Invalid login response: {message_error}"))?;
                if let Some(peer) = peer_from_login_response(response)? {
                    return Ok(peer);
                }
            }
        }
    }
}

fn peer_from_login_response(response: LoginResponse) -> Result<Option<PeerInfo>> {
    match response.union {
        Some(login_response::Union::Error(error)) if !error.is_empty() => {
            bail!("Login failed: {error}")
        }
        Some(login_response::Union::PeerInfo(peer)) => Ok(Some(peer)),
        _ => Ok(None),
    }
}

async fn respond_to_test_delay(conn: &mut Stream, delay: TestDelay) -> Result<()> {
    if !delay.from_client {
        let mut response = Message::new();
        response.set_test_delay(delay);
        send_msg(conn, &response, "test_delay_response").await?;
    }
    Ok(())
}

async fn push_file(
    conn: &mut Stream,
    local: PathBuf,
    remote: String,
    remote_version: &str,
) -> Result<FileTransferStats> {
    let metadata = std::fs::metadata(&local)
        .with_context(|| format!("Cannot read local file {}", local.display()))?;
    if !metadata.is_file() {
        bail!("Push source is not a file: {}", local.display());
    }
    if remote.is_empty() {
        bail!("Remote destination path cannot be empty");
    }

    let local_path = local
        .to_str()
        .context("Local source path is not valid UTF-8")?;
    let overwrite_detection =
        fs::can_enable_overwrite_detection(hbb_common::get_version_number(remote_version));
    let mut job = fs::TransferJob::new_read(
        FILE_JOB_ID,
        fs::JobType::Generic,
        remote.clone(),
        fs::DataSource::FilePath(local.clone()),
        0,
        false,
        false,
        overwrite_detection,
    )?;
    if job.files().len() != 1 {
        bail!("Only single-file uploads are supported: {local_path}");
    }
    job.is_resume = file_resume_supported(remote_version);

    let files = job.files().clone();
    let total_size = job.total_size();
    let request = fs::new_receive(FILE_JOB_ID, remote.clone(), 0, files, total_size);
    send_msg(conn, &request, "push_request")
        .await
        .map_err(|error| disconnected("push_request", error))?;

    let mut jobs = vec![job];
    let mut ticker = time::interval(Duration::from_millis(1));
    ticker.set_missed_tick_behavior(time::MissedTickBehavior::Skip);
    let mut transferred = 0;
    let mut finished_size = 0;
    let mut resumed_from = 0;
    let mut last_progress = time::Instant::now();
    let mut local_done = false;
    let mut keepalive = time::interval_at(
        time::Instant::now() + Duration::from_secs(15),
        Duration::from_secs(15),
    );

    loop {
        tokio::select! {
            result = conn.next() => {
                let bytes = match result {
                    Some(Ok(bytes)) => bytes,
                    Some(Err(error)) => return Err(disconnected_at("push", format!("stream error: {error}"), finished_size)),
                    None => return Err(disconnected_at("push", "connection closed by peer", finished_size)),
                };
                let (made_progress, remote_done, confirmed_offset) =
                    handle_push_message(conn, &mut jobs, bytes).await?;
                if let Some(offset) = confirmed_offset {
                    resumed_from = resumed_from.max(offset);
                }
                if made_progress {
                    last_progress = time::Instant::now();
                }
                if remote_done {
                    if !local_done {
                        bail!("Remote completed upload before all local data was sent");
                    }
                    break;
                }
            }
            _ = ticker.tick(), if !local_done => {
                fs::handle_read_jobs(&mut jobs, conn)
                    .await
                    .map_err(|error| disconnected_at("push_send", error, finished_size))?;
                let current = jobs.iter().map(|job| job.transferred()).sum();
                finished_size = if jobs.is_empty() {
                    total_size
                } else {
                    jobs.iter().map(|job| job.finished_size()).sum()
                };
                if current != transferred || jobs.is_empty() {
                    transferred = current;
                    last_progress = time::Instant::now();
                }
                local_done = jobs.is_empty();
            }
            _ = keepalive.tick() => {
                send_msg(conn, &Message::new(), "push_keepalive")
                    .await
                    .map_err(|error| disconnected_at("push_keepalive", error, finished_size))?;
            }
            _ = time::sleep_until(last_progress + TRANSFER_IDLE_TIMEOUT) => {
                return Err(disconnected_at(
                    "push",
                    format!("made no progress for {} seconds", TRANSFER_IDLE_TIMEOUT.as_secs()),
                    finished_size,
                ));
            }
        }
    }

    log::info!(
        "Uploaded {} bytes: {} -> {}",
        total_size,
        local.display(),
        remote
    );
    Ok(FileTransferStats {
        bytes: total_size,
        resumed_from,
    })
}

async fn handle_push_message(
    conn: &mut Stream,
    jobs: &mut [fs::TransferJob],
    bytes: bytes::BytesMut,
) -> Result<(bool, bool, Option<u64>)> {
    let message = Message::parse_from_bytes(&bytes).context("[push] invalid message")?;
    match message.union {
        Some(message::Union::TestDelay(delay)) => {
            let progress = jobs.iter().map(|job| job.finished_size()).sum();
            respond_to_test_delay(conn, delay)
                .await
                .map_err(|error| disconnected_at("push_test_delay", error, progress))?;
            Ok((false, false, None))
        }
        Some(message::Union::FileAction(action)) => {
            if let Some(file_action::Union::SendConfirm(confirm)) = action.union {
                if confirm.id == FILE_JOB_ID {
                    let job = fs::get_job(FILE_JOB_ID, jobs)
                        .context("Push confirmation arrived after the job ended")?;
                    let offset = confirmed_offset(&confirm, job.total_size())?;
                    job.confirm(&confirm).await;
                    return Ok((true, false, Some(offset)));
                }
            }
            Ok((false, false, None))
        }
        Some(message::Union::FileResponse(response)) => match response.union {
            Some(file_response::Union::Digest(digest)) if digest.id == FILE_JOB_ID => {
                let job = fs::get_job(FILE_JOB_ID, jobs)
                    .context("Push digest arrived after the job ended")?;
                let offset = if job.is_resume && digest.is_identical {
                    protocol_resume_offset(digest.transferred_size, job.total_size())?
                } else {
                    0
                };
                let confirm = FileTransferSendConfirmRequest {
                    id: FILE_JOB_ID,
                    file_num: digest.file_num,
                    union: Some(file_transfer_send_confirm_request::Union::OffsetBlk(offset)),
                    ..Default::default()
                };
                if offset > 0 {
                    log::info!("Resuming upload at byte offset {offset}");
                }
                job.confirm(&confirm).await;
                send_msg(
                    conn,
                    &fs::new_send_confirm(confirm),
                    "push_overwrite_confirm",
                )
                .await
                .map_err(|error| {
                    disconnected_at("push_confirm", error, job.finished_size())
                })?;
                Ok((true, false, Some(u64::from(offset))))
            }
            Some(file_response::Union::Done(done)) if done.id == FILE_JOB_ID => {
                Ok((true, true, None))
            }
            Some(file_response::Union::Error(error)) if error.id == FILE_JOB_ID => {
                bail!("Remote upload failed: {}", error.error)
            }
            _ => Ok((false, false, None)),
        },
        Some(message::Union::LoginResponse(response)) => {
            peer_from_login_response(response)?;
            Ok((false, false, None))
        }
        Some(message::Union::MessageBox(message)) if message.msgtype == "error" => {
            bail!("Remote upload failed: {}", message.text)
        }
        _ => Ok((false, false, None)),
    }
}

fn confirmed_offset(confirm: &FileTransferSendConfirmRequest, total_size: u64) -> Result<u64> {
    let offset = match confirm.union.as_ref() {
        Some(file_transfer_send_confirm_request::Union::OffsetBlk(offset)) => u64::from(*offset),
        _ => 0,
    };
    if offset > total_size {
        bail!("Remote resume offset {offset} exceeds upload size {total_size}");
    }
    Ok(offset)
}

fn protocol_resume_offset(transferred_size: u64, total_size: u64) -> Result<u32> {
    if transferred_size > total_size {
        bail!("Resume size {transferred_size} exceeds file size {total_size}");
    }
    Ok(transferred_size.min(u64::from(u32::MAX)) as u32)
}

async fn pull_file(
    conn: &mut Stream,
    remote: String,
    local: PathBuf,
    remote_version: &str,
) -> Result<FileTransferStats> {
    if remote.is_empty() {
        bail!("Remote source path cannot be empty");
    }
    if local.is_dir() {
        bail!("Pull destination is a directory: {}", local.display());
    }
    let local_path = local
        .to_str()
        .context("Local destination path is not valid UTF-8")?;
    let overwrite_detection =
        fs::can_enable_overwrite_detection(hbb_common::get_version_number(remote_version));
    let mut job = fs::TransferJob::new_write(
        FILE_JOB_ID,
        fs::JobType::Generic,
        remote.clone(),
        fs::DataSource::FilePath(local.clone()),
        0,
        false,
        true,
        overwrite_detection,
    );
    job.is_resume = file_resume_supported(remote_version);

    let request = fs::new_send(FILE_JOB_ID, fs::JobType::Generic, remote.clone(), 0, false);
    send_msg(conn, &request, "pull_request")
        .await
        .map_err(|error| disconnected("pull_request", error))?;

    let result = pull_file_loop(conn, &mut job, local_path).await;
    let (expected_size, resumed_from) = match result {
        Ok(result) => result,
        Err(error) => {
            if !job.is_resume {
                job.remove_download_file();
            }
            return Err(error);
        }
    };
    job.modify_time();
    drop(job);

    let metadata = std::fs::metadata(&local)
        .with_context(|| format!("Downloaded file was not finalized at {local_path}"))?;
    if !metadata.is_file() || metadata.len() != expected_size {
        bail!(
            "Downloaded file size mismatch at {}: expected {}, got {}",
            local.display(),
            expected_size,
            metadata.len()
        );
    }
    log::info!(
        "Downloaded {} bytes: {} -> {}",
        expected_size,
        remote,
        local.display()
    );
    Ok(FileTransferStats {
        bytes: expected_size,
        resumed_from,
    })
}

async fn pull_file_loop(
    conn: &mut Stream,
    job: &mut fs::TransferJob,
    local_path: &str,
) -> Result<(u64, u64)> {
    let mut expected_size = None;
    let mut resumed_from = 0;
    let mut last_progress = time::Instant::now();
    let mut keepalive = time::interval_at(
        time::Instant::now() + Duration::from_secs(15),
        Duration::from_secs(15),
    );

    loop {
        tokio::select! {
            result = conn.next() => {
                let bytes = match result {
                    Some(Ok(bytes)) => bytes,
                    Some(Err(error)) => return Err(disconnected_at("pull", format!("stream error: {error}"), job.finished_size())),
                    None => return Err(disconnected_at("pull", "connection closed by peer", job.finished_size())),
                };
                let message = Message::parse_from_bytes(&bytes).context("[pull] invalid message")?;
                match message.union {
                    Some(message::Union::TestDelay(delay)) => {
                        respond_to_test_delay(conn, delay)
                            .await
                            .map_err(|error| disconnected_at("pull_test_delay", error, job.finished_size()))?;
                    }
                    Some(message::Union::FileResponse(response)) => match response.union {
                        Some(file_response::Union::Dir(directory)) if directory.id == FILE_JOB_ID => {
                            if expected_size.is_some() {
                                bail!("Remote sent duplicate file metadata for pull job");
                            }
                            if directory.entries.len() != 1 {
                                bail!(
                                    "Pull supports one file, but the remote path contains {} entries",
                                    directory.entries.len()
                                );
                            }
                            let entry = &directory.entries[0];
                            if entry.entry_type.enum_value() != Ok(FileType::File) {
                                bail!("Remote source is not a regular file");
                            }
                            expected_size = Some(entry.size);
                            job.set_files(directory.entries.to_vec())?;
                            last_progress = time::Instant::now();
                        }
                        Some(file_response::Union::Digest(digest)) if digest.id == FILE_JOB_ID => {
                            job.set_digest(digest.file_size, digest.last_modified);
                            let offset = match fs::is_write_need_confirmation(
                                job.is_resume,
                                local_path,
                                &digest,
                            )? {
                                fs::DigestCheckResult::NeedConfirm(local_digest)
                                    if local_digest.is_identical
                                        && local_digest.transferred_size > 0
                                        && local_digest.transferred_size <= digest.file_size =>
                                {
                                    u64::from(protocol_resume_offset(
                                        local_digest.transferred_size,
                                        digest.file_size,
                                    )?)
                                }
                                _ => 0,
                            };
                            let confirm = FileTransferSendConfirmRequest {
                                id: FILE_JOB_ID,
                                file_num: digest.file_num,
                                union: Some(file_transfer_send_confirm_request::Union::OffsetBlk(
                                    offset as _,
                                )),
                                ..Default::default()
                            };
                            if offset > 0 {
                                log::info!("Resuming download at byte offset {offset}");
                            }
                            job.confirm(&confirm).await;
                            send_msg(conn, &fs::new_send_confirm(confirm), "pull_overwrite_confirm")
                                .await
                                .map_err(|error| disconnected_at("pull_confirm", error, job.finished_size()))?;
                            resumed_from = resumed_from.max(offset);
                            last_progress = time::Instant::now();
                        }
                        Some(file_response::Union::Block(block)) if block.id == FILE_JOB_ID => {
                            if expected_size.is_none() {
                                bail!("Remote sent file data before file metadata");
                            }
                            job.write(block).await?;
                            last_progress = time::Instant::now();
                        }
                        Some(file_response::Union::Done(done)) if done.id == FILE_JOB_ID => {
                            return Ok((
                                expected_size.context("Remote completed pull without file metadata")?,
                                resumed_from,
                            ));
                        }
                        Some(file_response::Union::Error(error)) if error.id == FILE_JOB_ID => {
                            bail!("Remote download failed: {}", error.error)
                        }
                        _ => {}
                    },
                    Some(message::Union::LoginResponse(response)) => {
                        peer_from_login_response(response)?;
                    }
                    Some(message::Union::MessageBox(message)) if message.msgtype == "error" => {
                        bail!("Remote download failed: {}", message.text)
                    }
                    _ => {}
                }
            }
            _ = keepalive.tick() => {
                send_msg(conn, &Message::new(), "pull_keepalive")
                    .await
                    .map_err(|error| disconnected_at("pull_keepalive", error, job.finished_size()))?;
            }
            _ = time::sleep_until(last_progress + TRANSFER_IDLE_TIMEOUT) => {
                return Err(disconnected_at(
                    "pull",
                    format!("made no progress for {} seconds", TRANSFER_IDLE_TIMEOUT.as_secs()),
                    job.finished_size(),
                ));
            }
        }
    }
}

// ── secure_tcp ─────────────────────────────────────────────────────

async fn attempt_secure_tcp(conn: &mut Stream, key: &str) -> Result<()> {
    let rs_pk = match get_rs_pk(key) {
        Some(pk) => pk,
        None => { log::debug!("No valid key, skipping secure_tcp"); return Ok(()); }
    };
    match hbb_common::timeout(3000, conn.next()).await {
        Ok(Some(Ok(bytes))) => {
            let rmsg = match RendezvousMessage::parse_from_bytes(&bytes) {
                Ok(m) => m, Err(_) => { log::debug!("Non-protobuf, skipping"); return Ok(()); }
            };
            let ex = match rmsg.union {
                Some(hbb_common::rendezvous_proto::rendezvous_message::Union::KeyExchange(ex)) => ex,
                _ => { log::debug!("No KeyExchange, proceeding"); return Ok(()); }
            };
            if ex.keys.len() != 1 { log::warn!("Invalid KeyExchange"); return Ok(()); }
            let their_pk_b = match sign::verify(&ex.keys[0], &rs_pk) {
                Ok(pk) => pk, Err(_) => { log::warn!("Sig verify failed"); return Ok(()); }
            };
            let their_pk = match get_pk(&their_pk_b) {
                Some(pk) => pk, None => { log::warn!("Invalid pk len"); return Ok(()); }
            };
            let (av, sv, enc) = create_symmetric_key_msg(their_pk);
            let mut mo = RendezvousMessage::new();
            mo.set_key_exchange(KeyExchange { keys: vec![av.into(), sv.into()], ..Default::default() });
            send_msg(conn, &mo, "key_exchange_response").await?;
            conn.set_key(enc);
            log::info!("Secure channel with rendezvous server");
        }
        Ok(Some(Err(e))) => { log::warn!("Stream err: {e}"); }
        Ok(None) => bail!("Rendezvous server closed connection"),
        Err(_) => { log::debug!("No KeyExchange (timeout), proceeding"); }
    }
    Ok(())
}

// ── Terminal I/O loop ──────────────────────────────────────────────

async fn terminal_io_loop(conn: &mut Stream, remote_platform: &str, quit_key: char) -> Result<()> {
    let _guard = ConsoleGuard::enable()?;
    let (cols, rows) = crossterm::terminal::size().context("Failed to get terminal size")?;
    let terminal_id: i32 = 0;

    {
        let mut action = TerminalAction::new();
        action.set_open(OpenTerminal { terminal_id, rows: rows as u32, cols: cols as u32, ..Default::default() });
        let mut msg = Message::new();
        msg.set_terminal_action(action);
        send_msg(conn, &msg, "open_terminal").await?;
    }
    log::debug!("OpenTerminal sent ({}x{}), waiting for shell...", cols, rows);

    let mut input_timer = time::interval(std::time::Duration::from_millis(20));
    let mut keepalive = time::interval(std::time::Duration::from_secs(15));
    let mut terminal_opened = false;
    let mut locale_injected = false;
    let mut last_cols = cols;
    let mut last_rows = rows;

    loop {
        tokio::select! {
            _ = keepalive.tick() => {
                if terminal_opened { conn.send(&Message::new()).await.ok(); }
            }

            res = conn.next() => {
                let bytes = match res {
                    Some(Ok(b)) => b,
                    Some(Err(e)) => { log::error!("Stream error: {}", e); break; }
                    None => { log::info!("Connection closed by peer"); break; }
                };
                let msg_in = match Message::parse_from_bytes(&bytes) {
                    Ok(m) => m, Err(e) => { log::error!("Parse: {} (raw: {:02x?})", e, bytes.as_ref()); continue; }
                };
                match msg_in.union {
                    Some(message::Union::TerminalResponse(resp)) => {
                        use terminal_response::Union;
                        match resp.union {
                            Some(Union::Opened(o)) => {
                                terminal_opened = o.success;
                                if !o.success { bail!("Terminal open failed: {}", o.message); }
                                log::info!("Shell started (pid: {})", o.pid);
                            }
                            Some(Union::Data(data)) => {
                                let output = if data.compressed {
                                    hbb_common::compress::decompress(&data.data)
                                } else { data.data.to_vec() };
                                write_stdout(&output);
                            }
                            Some(Union::Closed(c)) => {
                                log::info!("Terminal closed (exit code: {})", c.exit_code);
                                return Ok(());
                            }
                            Some(Union::Error(e)) => bail!("Terminal error: {}", e.message),
                            _ => { log::debug!("TerminalResponse with empty union"); }
                        }
                    }
                    Some(message::Union::TestDelay(delay)) => {
                        respond_to_test_delay(conn, delay).await?;
                    }
                    Some(message::Union::Hash(_)) => {}
                    other => { log::debug!("Unhandled message type: {:?}", other.map(|_| ())); }
                }
            }

            _ = input_timer.tick() => {
                // Print locale setup hint after shell starts.
                // The remote PTY may run in C locale, breaking CJK display.
                // Let the user decide whether to run it.
                if terminal_opened && !locale_injected {
                    locale_injected = true;
                    let hint = if remote_platform.eq_ignore_ascii_case("Windows") {
                        "\n  | Tip: If CJK chars display incorrectly, run:\n  |   cmd /c \"chcp 65001 >nul 2>&1\"\n"
                    } else {
                        "\n  | Tip: If CJK chars display incorrectly, run:\n  |   export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8\n"
                    };
                    eprintln!("{hint}");
                }

                if let Ok((nc, nr)) = crossterm::terminal::size() {
                    if (nc != last_cols || nr != last_rows) && terminal_opened {
                        log::debug!("Resize: {}x{}", nc, nr);
                        let mut a = TerminalAction::new();
                        a.set_resize(ResizeTerminal { terminal_id, rows: nr as u32, cols: nc as u32, ..Default::default() });
                        let mut m = Message::new(); m.set_terminal_action(a);
                        conn.send(&m).await.ok();
                        last_cols = nc; last_rows = nr;
                    }
                }
                while let Some(data) = poll_key_event() {
                    if data.is_empty() { continue; }
                    let quit_byte = (quit_key.to_ascii_lowercase() as u8) - b'a' + 1;
                    if data == [quit_byte] {
                        log::info!("Closing terminal (Ctrl+{})...", quit_key.to_ascii_uppercase());
                        if terminal_opened {
                            let mut a = TerminalAction::new();
                            a.set_close(CloseTerminal { terminal_id, ..Default::default() });
                            let mut m = Message::new(); m.set_terminal_action(a);
                            conn.send(&m).await.ok();
                        }
                        return Ok(());
                    }
                    if terminal_opened {
                        let mut a = TerminalAction::new();
                        a.set_data(TerminalData { terminal_id, data: data.into(), compressed: false, ..Default::default() });
                        let mut m = Message::new(); m.set_terminal_action(a);
                        conn.send(&m).await.ok();
                    }
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_subcommand_keeps_terminal_mode() {
        let args =
            Args::try_parse_from(["rustshell", "--id", "123", "--server", "example.test"]).unwrap();

        assert!(args.command.is_none());
    }

    #[test]
    fn parses_push_paths() {
        let args = Args::try_parse_from([
            "rustshell",
            "--id",
            "123",
            "--server",
            "example.test",
            "push",
            "local.txt",
            "C:\\Temp\\remote.txt",
        ])
        .unwrap();

        match args.command {
            Some(Command::Push { local, remote }) => {
                assert_eq!(local, PathBuf::from("local.txt"));
                assert_eq!(remote, "C:\\Temp\\remote.txt");
            }
            _ => panic!("expected push command"),
        }
    }

    #[test]
    fn parses_pull_paths() {
        let args = Args::try_parse_from([
            "rustshell",
            "--id",
            "123",
            "--server",
            "example.test",
            "pull",
            "/tmp/remote.txt",
            "local.txt",
        ])
        .unwrap();

        match args.command {
            Some(Command::Pull { remote, local }) => {
                assert_eq!(remote, "/tmp/remote.txt");
                assert_eq!(local, PathBuf::from("local.txt"));
            }
            _ => panic!("expected pull command"),
        }
    }

    #[test]
    fn parses_exec_command_and_timeout() {
        let args = Args::try_parse_from([
            "rustshell", "--id", "123", "--server", "example.test",
            "exec", "--timeout", "12", "printf 'hello'",
        ]).unwrap();

        match args.command {
            Some(Command::Exec { timeout, command }) => {
                assert_eq!(timeout, 12);
                assert_eq!(command, "printf 'hello'");
            }
            _ => panic!("expected exec command"),
        }
    }

    #[test]
    fn parses_mcp_without_remote_connection_options() {
        let args = Args::try_parse_from([
            "rustshell", "mcp", "--wrapper", "/tmp/rustshell.sh",
        ]).unwrap();

        assert!(matches!(args.command, Some(Command::Mcp { .. })));
    }

    #[test]
    fn file_resume_requires_rustdesk_1_4_2() {
        assert!(!file_resume_supported("1.4.1"));
        assert!(file_resume_supported("1.4.2"));
        assert!(file_resume_supported("1.4.9"));
    }

    #[test]
    fn app_and_rustdesk_protocol_versions_are_independent() {
        assert_ne!(APP_VERSION, RUSTDESK_PROTOCOL_VERSION);
        assert_eq!(RUSTDESK_PROTOCOL_VERSION, "1.4.9");
    }

    #[test]
    fn transfer_disconnect_marker_survives_anyhow() {
        let error = disconnected("pull", "connection closed");
        assert!(is_transfer_disconnected(&error));
        assert_eq!(transfer_disconnect_progress(&error), Some(0));
        let progressed = disconnected_at("pull", "connection closed", 42);
        assert_eq!(transfer_disconnect_progress(&progressed), Some(42));
    }

    #[test]
    fn resume_offset_caps_files_larger_than_four_gib() {
        let five_gib = 5 * 1024 * 1024 * 1024;
        assert_eq!(protocol_resume_offset(five_gib, five_gib).unwrap(), u32::MAX);
        assert!(protocol_resume_offset(five_gib + 1, five_gib).is_err());
    }
}
