# fish-agent

WebSocket PTY agent for [fish-term](https://github.com/ystyle/fish-term) — a HarmonyOS terminal emulator.

> Fork from [ystyle/fish-agent](https://github.com/ystyle/fish-agent)，增加了会话保持、环形缓冲区屏幕恢复、CWD 轮询、fork 会话等功能。

Creates a PTY session for each WebSocket connection, forwarding terminal I/O between the HarmonyOS app and a shell running in an openEuler container.

## Quick Start

```bash
# 下载最新版本
# ARM64（openEuler / LOH 轻量级鸿蒙）：
curl -LO https://github.com/picklerick422/fish-agent/releases/latest/download/fish-agent-linux-arm64
# AMD64（x86_64 服务器）：
curl -LO https://github.com/picklerick422/fish-agent/releases/latest/download/fish-agent-linux-amd64

chmod +x fish-agent-linux-*
mv fish-agent-linux-* fish-agent

# 启动
./fish-agent --token harmonyterm
```

## 后台管理（推荐：zinit + zshrc）

### 安装 zinit

```bash
mkdir -p ~/bin
curl -Lo ~/bin/zinit https://github.com/threefoldtech/zinit/releases/latest/download/zinit-linux-arm64
chmod +x ~/bin/zinit
```

### 注册系统服务

```bash
sudo ~/bin/zinit init 2>/dev/null || true
sudo tee /etc/zinit/fish-agent.yaml << 'EOF'
exec: /home/user/fish-agent --token harmonyterm
after:
  - net-eth0
EOF
sudo zinit start fish-agent
```

### zshrc 快捷管理

在 `~/.zshrc` 中添加：

```zsh
alias fa-up='sudo zinit start fish-agent'
alias fa-down='sudo zinit stop fish-agent'
alias fa-restart='sudo zinit restart fish-agent'
alias fa-status='sudo zinit status fish-agent'
alias fa-log='sudo zinit log fish-agent'
```

重载配置：

```bash
source ~/.zshrc
```

## 连接 fish-term

1. 在 openEuler 容器中查看 IP：

   ```bash
   ip addr show eth0
   ```

   默认 IP: `172.16.100.2` · 端口: `8765`

2. 在 fish-term 中点击连接状态圆点 → **连接管理...**
3. 填入上述 IP、端口和 Token（默认 `harmonyterm`）
4. 点击「保存 & 重连」

## 命令行参数

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--host` | `0.0.0.0` | 监听地址 |
| `--port` | `8765` | 监听端口 |
| `--token` | `harmonyterm` | WebSocket 连接认证 Token |
| `--max-sessions` | `10` | 最大会话数 |

## 协议

WebSocket 端点: `/ws?token=<token>&cols=80&rows=24&cwd=/path&session_id=<session_id>`

- **Binary frames** (client→server): 终端输入（键盘数据）
- **Binary frames** (server→client): PTY 输出（ANSI/VT 序列）
- **Text frames** (client→server): JSON 控制消息

### 控制消息

| Type | Direction | Description |
|------|-----------|-------------|
| `{"type":"session","id":"..."}` | server→client | Session ID (首次连接时发送) |
| `{"type":"resize","cols":80,"rows":24}` | client→server | Resize PTY |
| `{"type":"cwd"}` | client→server | Query working directory |
| `{"type":"cwd","dir":"/path"}` | server→client | Working directory response |
| `{"type":"fork","cwd":"/path"}` | client→server | Fork new session at path |
| `{"type":"forked","id":"..."}` | server→client | New session ID |
| `{"type":"ping","ts":123}` | bidirectional | Heartbeat |
| `{"type":"error","error":"..."}` | server→client | Error notification |
| `{"type":"task_event","event":"completed\|awaiting","message":"..."}` | server→client | Claude-code hook event (see below) |
| `{"type":"list_dir","path":"/absolute/path"}` | client→server | Request directory listing |
| `{"type":"list_dir_result","path":"...","entries":[...]}` | server→client | Directory entries (dirs first, dotfiles last, max 2000) |
| `{"type":"file_read","path":"/absolute/path"}` | client→server | Read a remote file |
| `{"type":"file_read_result","path":"...","content":"base64...","size":123}` | server→client | File content (base64-encoded, max 10MB) |

## 会话保持与屏幕恢复

fish-agent 支持会话保持功能，类似于 tmux。客户端断连后保持 PTY 会话存活，重连时恢复终端界面（包括正在运行的程序如 claude code）。

### 工作原理

1. **首次连接**：创建新 PTY，生成 session_id，通过控制消息发给客户端
2. **重连**：客户端带 session_id 参数连接，服务端查找已有 session 并 attach
3. **断连**：保持 session 存活，不杀进程，不关 PTY
4. **屏幕恢复**：重连时重放环形缓冲区中的输出数据

### 环形缓冲区

服务端维护一个 8MB 的环形缓冲区，保存最近的 PTY 输出。重连时重放缓冲区内容，恢复断连前的终端界面。

## 任务事件通知（claude-code hooks）

fish-term 的任务通知由 claude-code hooks 驱动，而不是解析终端输出：

- **Stop hook** → 每轮回复完成 → `task_event / completed`
- **Notification hook（matcher: `permission_prompt`）** → claude 等待权限批准 → `task_event / awaiting`

每个 session 的 shell 环境里注入 `FISH_SESSION_ID` 和 `FISH_TOKEN`，hook 脚本据此把事件 POST 到 `/notify` 端点，fish-agent 再经该 session 的 WebSocket 推送给设备端。在 fish-term 之外运行的 claude（无这两个环境变量）会自动静默跳过。

### 部署 hooks（在 fish-agent 宿主机上）

1. 安装上报脚本：

   ```bash
   mkdir -p ~/bin
   cp scripts/notify-hook.sh ~/bin/fish-agent-notify.sh
   chmod +x ~/bin/fish-agent-notify.sh
   ```

2. 在 `~/.claude/settings.json` 中合并 hooks 配置（示例见 `scripts/hooks-settings.example.json`；已有 `hooks` 键时合并而不是覆盖）：

   ```json
   {
     "hooks": {
       "Stop": [
         { "hooks": [ { "type": "command", "command": "$HOME/bin/fish-agent-notify.sh completed" } ] }
       ],
       "Notification": [
         { "matcher": "permission_prompt",
           "hooks": [ { "type": "command", "command": "$HOME/bin/fish-agent-notify.sh awaiting" } ] }
       ]
     }
   }
   ```

3. 重启 fish-agent 使新的 session 环境变量生效。

要求：宿主机 claude-code ≥ 2.0.37（Notification hook）；脚本需要 `curl`（`python3` 可选，用于更可靠的 JSON 处理）。

## 构建

```bash
go build -buildvcs=false -ldflags="-s -w" -o fish-agent .
```

## License

MIT
