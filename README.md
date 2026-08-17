# RustShell

[中文文档](README_zh.md)

Cross-platform remote shell and single-file transfer client. Connects to any
device running RustDesk through the RustDesk relay infrastructure.

Works on **Windows**, **macOS**, and **Linux**.

## Quick Start

```bash
# Build
cargo build --release

# Connect to a remote device
./target/release/rustshell \
  --id <DEVICE_ID> \
  --server <RENDEZVOUS_SERVER> \
  --key <LICENCE_KEY> \
  --password <DEVICE_PASSWORD>
```

## Usage

```
rustshell [OPTIONS] [COMMAND]

Commands:
  exec <COMMAND>          Run one bounded command and print a JSON result
  push <LOCAL> <REMOTE>  Upload one file to an exact remote path
  pull <REMOTE> <LOCAL>  Download one file to an exact local path
  mcp                     Serve RustDesk tools over MCP stdio

Options:
  -i, --id <ID>              Remote device ID (required)
  -s, --server <SERVER>      Rendezvous server host:port or IP (required)
  -p, --port <PORT>          Rendezvous server port [default: 21116]
  -k, --key <KEY>            Licence key [default: built-in public key]
  -w, --password <PASSWORD>  Device password (omit for interactive prompt)
  -q, --quit-key <CHAR>      Quit key letter for Ctrl+key combo [default: q]
  -d, --debug                Enable debug logging
  -h, --help                 Print help
```

## Environment Variables

All CLI arguments can also be set via environment variables (prefixed with `RUSTSHELL_`).
CLI arguments take precedence when both are provided.

| Variable | CLI flag | Description |
|----------|----------|-------------|
| `RUSTSHELL_ID` | `--id` | Remote device ID |
| `RUSTSHELL_SERVER` | `--server` | Rendezvous server address |
| `RUSTSHELL_PORT` | `--port` | Rendezvous server port |
| `RUSTSHELL_KEY` | `--key` | Licence key |
| `RUSTSHELL_PASSWORD` | `--password` | Device password |
| `RUSTSHELL_QUIT_KEY` | `--quit-key` | Quit key letter (a-z) |
| `RUSTSHELL_DEBUG` | `--debug` | Set to `1` or `true` |

```bash
# All configuration via environment variables
export RUSTSHELL_ID=123456789
export RUSTSHELL_SERVER=myserver.example.com
export RUSTSHELL_KEY="MyKeyBase64..."
export RUSTSHELL_PASSWORD="mypassword"
rustshell

# Override specific values with CLI flags
RUSTSHELL_ID=123456789 RUSTSHELL_SERVER=myserver.example.com \
  rustshell -k "MyKey..." -w mypassword
```

## Examples

```bash
# Self-hosted server with custom key
rustshell -i 123456789 -s myserver.example.com -k "MyKeyBase64..." -w mypassword

# Custom port
rustshell -i 123456789 -s 192.168.1.100 -p 61116 -k "MyKey..." -w mypassword

# Interactive password prompt (more secure)
rustshell -i 123456789 -s myserver.example.com -k "MyKey..."

# Debug mode for troubleshooting
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword -d

# Upload to macOS
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  push ./report.txt /Users/name/Desktop/report.txt

# Upload to Windows
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  push ./report.txt 'C:\Users\name\Desktop\report.txt'

# Download from either platform
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  pull /Users/name/Desktop/report.txt ./report.txt

# Run one command with separated output and a real remote exit code
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  exec --timeout 30 "uname -a"
```

`push` and `pull` currently transfer one regular file. Both destination
arguments are full file paths, and an existing destination is overwritten.

### MCP server

RustShell includes a local stdio MCP server with four tools:

- `rustdesk_list_devices`
- `rustdesk_run_command`
- `rustdesk_upload_file`
- `rustdesk_download_file`

Start it with a local controller wrapper. The wrapper owns RustDesk discovery
and credentials; the MCP server never accepts credentials as tool arguments.

```bash
rustshell mcp --wrapper /absolute/path/to/rustshell.sh
```

`rustdesk_list_devices` executes the wrapper's `devices --json` operation on
every call. It reads the live RustDesk peer files without opening a remote
connection, so newly added, removed, or renamed devices are visible without
restarting the MCP server. Clients should resolve a target once when a task
starts and reuse that device ID for consecutive operations; refresh the list
only when the target changes, the user requests it, matching is ambiguous, or
device/session validation fails.

Remote operations use a per-device dual session pool. Consecutive commands
reuse one authenticated terminal connection; consecutive uploads and downloads
reuse a separate authenticated file-transfer connection. The two RustDesk
connection types cannot share one underlying session. Sessions close after 300
seconds idle and reconnect once on the next call. A terminal command interrupted
by a disconnect is never replayed. A file transfer on RustDesk 1.4.2 or newer
keeps its partial data and resumes at the confirmed byte offset after a
mid-transfer disconnect, with at most 32 automatic reconnects per tool call and
only while each connection transfers data. This does not restart the whole file
or loop on zero progress; after the limit, the next explicit call can continue
the preserved partial file. Tool results expose `session_channel`,
`session_reused`, `resumed_from`, `resume_reconnects`, and the exact completion
stage in addition to the authenticated device identity and operation result.
Both active upload and download loops send a protocol keepalive every 15 seconds,
so a receive-only download does not look idle to the relay while data is flowing.

Large file transfers have no server-side total-duration limit. A transfer is
stopped only by an explicit error or 300 seconds without protocol progress.
RustShell reports RustDesk protocol version 1.4.9 independently from its own
application version so that the peer enables digest and resume negotiation.
The protocol offset is 32-bit; for partial files beyond 4 GiB, resume safely
starts at the largest representable offset. That protocol limit can retransmit
and overwrite the tail after 4 GiB, but it preserves the first 4 GiB rather than
restarting the whole file. Per-attempt byte accounting keeps automatic resume
working even while that tail is being overwritten.
The MCP client timeout must still cover the whole transfer; for Codex, use for
example:

```toml
[mcp_servers.rustdesk]
tool_timeout_sec = 86400
```

### Container image

The fork publishes Linux images for amd64 and arm64 to
`ghcr.io/linjzy/rustshell`. Mount the local files that should be transferred
and allocate a TTY for interactive terminal sessions:

```bash
docker run --rm -it \
  -v "$PWD:/data" -w /data \
  -e RUSTSHELL_ID -e RUSTSHELL_SERVER -e RUSTSHELL_KEY -e RUSTSHELL_PASSWORD \
  ghcr.io/linjzy/rustshell:latest \
  push /data/report.txt /Users/name/Desktop/report.txt
```

The container is the RustShell client; the remote RustDesk device can be
Windows, macOS, or Linux.

## How It Works

```
rustshell                         RustDesk infrastructure              Remote device
    │                                    │                                  │
    ├── TCP connect ──────────────────► rendezvous server (:21116)          │
    │   PunchHoleRequest{id, key}        │                                  │
    │   ◄── PunchHoleResponse ──────────┤                                  │
    │   {peer_addr, relay_fallback}      │                                  │
    │                                    │                                  │
    ├── direct TCP ────────────────(try)──┼────────────────────────────►   │
    │   (fallback on failure)                                 │             │
    │   ─── relay TCP ────────────────► relay (:21117)       │             │
    │       RequestRelay{id, uuid}      │                     │             │
    │                                    ├── bridge ────────►│             │
    │                                    │                                    │
    │   ◄══ E2E encrypted channel ═══════════════════════════════════════   │
    │   ◄── SignedId ───────────────────────────────────────────────────   │
    │   ──── PublicKey (NaCl key exchange) ───────────────────────────►   │
    │   ◄── Hash challenge ────────────────────────────────────────────   │
    │   ──── LoginRequest{terminal} ──────────────────────────────────►   │
    │   ◄══ Terminal I/O (stdin/stdout) ═══════════════════════════════   │
    │                                                                      │
    ▼                                                                      ▼
local terminal                                                     remote shell
(raw mode)                                                   (bash/zsh/PowerShell)
```

1. **Rendezvous**: Connects to the ID server, requests connection to target device
2. **Relay**: ID server assigns a relay server; both sides connect to it
3. **Key exchange**: NaCl-based E2E encryption (Curve25519 + XSalsa20-Poly1305)
4. **Authentication**: SHA-256 challenge-response with the device password
5. **Terminal**: Opens a PTY on the remote, enters raw mode locally, bi-directional I/O

## Requirements

- Rust 1.88+
- A running [RustDesk server](https://github.com/rustdesk/rustdesk-server) (hbbs + hbbr)
- RustDesk running on the target device with terminal or file-transfer access enabled

## Keyboard Shortcuts

| Key | Action |
|-----|--------|
| Ctrl+Q | Disconnect and exit (letter customizable via `--quit-key`) |
| Ctrl+C | Sent to remote (stop remote processes) |
| Ctrl+D | Sent to remote (send EOF) |

## Troubleshooting

**Connection closed immediately:**
- Verify the remote device ID is correct and the device is online
- Check that the rendezvous server address and port are correct
- Ensure the licence key matches the server configuration

**Chinese/CJK characters display as garbled text:**
- The remote shell's locale may not be set to UTF-8
- RustShell prints a hint with the appropriate fix command after connecting
- macOS/Linux: copy and run `export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8`
- Windows: copy and run `chcp 65001`

**Connection hangs after typing `exit` on Windows remote:**
- This is a [known bug](https://github.com/rustdesk/rustdesk/blob/caadd72ab2db8cc66e3d237e3e1cb60edbab7bc5/src/server/terminal_service.rs#L1267-L1270) in the RustDesk server: Windows ConPTY does not signal EOF when the shell exits, so the server never detects the session has ended
- **Workaround**: use Ctrl+Q to close the session instead of typing `exit`. This sends an explicit `CloseTerminal` message that the server handles correctly
- This issue only affects Windows remotes; macOS and Linux remotes work correctly with `exit`

**Connection drops after idle:**
- A keepalive heartbeat is sent every 15 seconds; the relay or server may have a shorter timeout
- Check the relay server's timeout configuration

## License

AGPL-3.0
