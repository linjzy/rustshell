# RustShell

[English](README.md)

跨平台远程 Shell 和单文件传输客户端。通过 RustDesk 中继基础设施连接
任意运行 RustDesk 的设备。

支持 **Windows**、**macOS**、**Linux**。

## 快速开始

```bash
# 编译
cargo build --release

# 连接远程设备
./target/release/rustshell \
  --id <设备ID> \
  --server <中继服务器地址> \
  --key <许可证密钥> \
  --password <设备密码>
```

## 用法

```
rustshell [OPTIONS] [COMMAND]

命令:
  exec <COMMAND>          执行一条有超时的命令并输出 JSON 结果
  push <LOCAL> <REMOTE>  上传一个文件到远端完整路径
  pull <REMOTE> <LOCAL>  从远端下载一个文件到本地完整路径
  mcp                     通过 stdio 提供 RustDesk MCP 工具

选项:
  -i, --id <ID>              远程设备 ID
  -s, --server <SERVER>      ID 服务器地址 (host:port 或 IP)
  -p, --port <PORT>          ID 服务器端口 [默认: 21116]
  -k, --key <KEY>            许可证密钥 [默认: 内置公钥]
  -w, --password <PASSWORD>  设备密码 (留空则交互式输入)
  -q, --quit-key <CHAR>      退出组合键字母 [默认: q]
  -d, --debug                启用调试日志
  -h, --help                 打印帮助
```

## 环境变量

所有 CLI 参数也可通过环境变量设置（前缀 `RUSTSHELL_`）。
CLI 参数优先级高于环境变量。

| 变量 | CLI 参数 | 说明 |
|------|----------|------|
| `RUSTSHELL_ID` | `--id` | 远程设备 ID |
| `RUSTSHELL_SERVER` | `--server` | ID 服务器地址 |
| `RUSTSHELL_PORT` | `--port` | ID 服务器端口 |
| `RUSTSHELL_KEY` | `--key` | 许可证密钥 |
| `RUSTSHELL_PASSWORD` | `--password` | 设备密码 |
| `RUSTSHELL_QUIT_KEY` | `--quit-key` | 退出快捷键字母 (a-z) |
| `RUSTSHELL_DEBUG` | `--debug` | 设为 `1` 或 `true` |

```bash
# 全部通过环境变量配置
export RUSTSHELL_ID=123456789
export RUSTSHELL_SERVER=myserver.example.com
export RUSTSHELL_KEY="MyKeyBase64..."
export RUSTSHELL_PASSWORD="mypassword"
rustshell

# 环境变量 + CLI 参数覆盖
RUSTSHELL_ID=123456789 RUSTSHELL_SERVER=myserver.example.com \
  rustshell -k "MyKey..." -w mypassword
```

## 示例

```bash
# 自建服务器 + 自定义密钥
rustshell -i 123456789 -s myserver.example.com -k "MyKeyBase64..." -w mypassword

# 自定义端口
rustshell -i 123456789 -s 192.168.1.100 -p 61116 -k "MyKey..." -w mypassword

# 交互式密码输入（更安全，密码不出现在命令行）
rustshell -i 123456789 -s myserver.example.com -k "MyKey..."

# 调试模式
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword -d

# 上传到 macOS
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  push ./report.txt /Users/name/Desktop/report.txt

# 上传到 Windows
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  push ./report.txt 'C:\Users\name\Desktop\report.txt'

# 从 macOS 或 Windows 下载
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  pull /Users/name/Desktop/report.txt ./report.txt

# 执行一条命令，分别返回输出及真实远端退出码
rustshell -i 123456789 -s myserver.example.com -k "MyKey..." -w mypassword \
  exec --timeout 30 "uname -a"
```

`push` 和 `pull` 当前只传一个普通文件。目标参数必须是完整文件路径；
目标已存在时会覆盖。

### MCP 服务

RustShell 内置本地 stdio MCP 服务，提供四个工具：

- `rustdesk_list_devices`
- `rustdesk_run_command`
- `rustdesk_upload_file`
- `rustdesk_download_file`

启动时指定本机控制 wrapper。RustDesk 设备发现和凭据只由 wrapper 管理，
MCP 工具参数不接受密码或服务器 key。

```bash
rustshell mcp --wrapper /absolute/path/to/rustshell.sh
```

`rustdesk_list_devices` 每次调用都会执行 wrapper 的 `devices --json`，
实时读取本机 RustDesk peer 文件且不连接远端，因此新增、删除或
改名设备后无需重启 MCP。客户端在一个连续任务开始时解析一次目标，
后续操作直接复用该设备 ID；只在更换目标、用户要求刷新、匹配不唯一或
设备/会话校验失败时重新查询。

远程操作按设备使用双通道会话池：连续命令复用一条已认证终端连接，
连续上传和下载复用另一条已认证文件连接。两种 RustDesk 连接类型不能
共用同一条底层会话。空闲 300 秒后会话自动关闭，下一次调用只重连一次；
断线的终端命令绝不自动重放。RustDesk 1.4.2 及以上的文件传输会保留
分片，按已确认字节偏移续传，只在每次连接确有数据传输时重连，单次工具调用最多 32 次；
不会从头重下，也不在零进度时循环。达到上限后保留分片，下一次显式调用可继续。
工具结果会返回 `session_channel`、
`session_reused`、`resumed_from`、`resume_reconnects`、已认证设备身份和准确完成阶段。
上传和下载的活动传输循环都每 15 秒发送一次协议保活，避免只接收数据的下载被 relay 误判为空闲。

有界命令达到截止时间时，结果使用 `stage: "command_timeout"`，返回实测
`duration_ms`，并设置 `timed_out: true` 和 `output_complete: false`，随后关闭该终端会话。
下一次显式调用会重新连接，超时命令绝不重放。stdout 和 stderr 只在命令完成后以一个
已校验帧返回，因此超时时两个字段保持为空，避免把不完整数据伪装成完整输出。

大文件传输没有服务端总时长限制；只在明确失败或连续 300 秒没有协议进度时
停止。RustShell 自身版本与握手上报的 RustDesk 1.4.9 协议版本彼此独立，以便
远端开启摘要和续传协商。协议偏移是 32 位；分片超过 4 GiB 时会从可表示的
最大偏移安全续传。受此协议限制，4 GiB 之后的尾部可能重传并覆盖，但不会从头重下；
每次连接的实际传输字节会单独计数，确保覆盖尾部时仍能自动续传。如果连续两次确有传输但持久化高水位不增长，工具返回
`chunk_fallback_required: true`，让客户端改用已校验分块，不再消耗全部 32 次重连。MCP 客户端的超时仍需覆盖整个传输过程；
Codex 可例如配置：

```toml
[mcp_servers.rustdesk]
tool_timeout_sec = 86400
```

### 容器镜像

该 fork 会把 amd64 和 arm64 Linux 镜像发布到
`ghcr.io/linjzy/rustshell`。运行时要挂载待传的本地文件；使用交互终端时
需要分配 TTY：

```bash
docker run --rm -it \
  -v "$PWD:/data" -w /data \
  -e RUSTSHELL_ID -e RUSTSHELL_SERVER -e RUSTSHELL_KEY -e RUSTSHELL_PASSWORD \
  ghcr.io/linjzy/rustshell:latest \
  push /data/report.txt /Users/name/Desktop/report.txt
```

容器内运行的是 RustShell 客户端；远端 RustDesk 设备可以是 Windows、
macOS 或 Linux。

## 工作原理

```
rustshell                         RustDesk 基础设施                 远程设备
    │                                    │                            │
    ├── TCP 连接 ────────────────────► ID 服务器 (:21116)              │
    │   PunchHoleRequest{id, key}        │                            │
    │   ◄── PunchHoleResponse ──────────┤                            │
    │   {peer_addr, relay_fallback}      │                            │
    │                                    │                            │
    ├── 直连 TCP ────────────────(尝试)──┼────────────────────────►   │
    │   (失败则降级)                                    │             │
    │   ─── 中继 TCP ────────────────► 中继 (:21117)    │             │
    │       RequestRelay{id, uuid}      │               │             │
    │                                    ├── 桥接 ─────►│             │
    │                                    │                            │
    │   ◄══ 端到端加密通道 ════════════════════════════════════════   │
    │   ◄── SignedId ────────────────────────────────────────────    │
    │   ──── PublicKey (NaCl 密钥交换) ─────────────────────────►    │
    │   ◄── Hash 质询 ──────────────────────────────────────────    │
    │   ──── LoginRequest{terminal} ────────────────────────────►    │
    │   ◄══ 终端 I/O (stdin/stdout) ══════════════════════════════   │
    │                                                                 │
    ▼                                                                 ▼
 本地终端                                                         远程 Shell
 (raw mode)                                                  (bash/zsh/sh)
```

1. **信令**：连接 ID 服务器，请求连接到目标设备
2. **中继**：ID 服务器分配中继服务器；双方连接到中继
3. **密钥交换**：基于 NaCl 的端到端加密 (Curve25519 + XSalsa20-Poly1305)
4. **认证**：SHA-256 质询-响应，使用设备密码
5. **终端**：在远端打开 PTY，本地进入 raw 模式，双向 I/O

## 环境要求

- Rust 1.88+
- 运行中的 [RustDesk 服务端](https://github.com/rustdesk/rustdesk-server) (hbbs + hbbr)
- 目标设备上运行 RustDesk，且已开启终端或文件传输权限

## 快捷键

| 按键 | 操作 |
|------|------|
| Ctrl+Q | 断开连接并退出（字母可通过 `--quit-key` 自定义） |
| Ctrl+C | 发送到远端（可终止远端进程） |
| Ctrl+D | 发送到远端（发送 EOF） |

## 故障排除

**连接立即断开：**
- 确认远程设备 ID 正确且设备在线
- 检查 ID 服务器地址和端口是否正确
- 确认许可证密钥与服务器配置一致

**中文/CJK 字符显示为乱码：**
- 远端 Shell 的 locale 可能未设置为 UTF-8
- RustShell 连接后会打印相应的修复命令提示
- macOS/Linux：复制并执行 `export LANG=en_US.UTF-8 LC_ALL=en_US.UTF-8`
- Windows：复制并执行 `chcp 65001`

**Windows 远端输入 `exit` 后连接挂起：**
- 这是 RustDesk 服务端的[已知 bug](https://github.com/rustdesk/rustdesk/blob/caadd72ab2db8cc66e3d237e3e1cb60edbab7bc5/src/server/terminal_service.rs#L1267-L1270)：Windows ConPTY 在子进程退出时不发送 EOF 信号，导致服务端无法检测到会话已结束
- **变通方案**：用 Ctrl+Q 替代 `exit` 来关闭会话。Ctrl+Q 会发送显式的 `CloseTerminal` 消息，服务端能正确处理
- 此问题仅影响 Windows 远端；macOS 和 Linux 远端使用 `exit` 正常工作

**空闲时连接断开：**
- 每 15 秒发送一次心跳保活；中继或服务端的超时可能更短
- 检查中继服务器的超时配置

## 许可证

AGPL-3.0
