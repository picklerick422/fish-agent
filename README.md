# fish-agent

WebSocket PTY agent for [fish-term](https://github.com/ystyle/fish-term) — a HarmonyOS terminal emulator.

> Fork from [ystyle/fish-agent](https://github.com/ystyle/fish-agent)，增加了会话保持、环形缓冲区屏幕恢复、CWD 轮询、fork 会话等功能。

Creates a PTY session for each WebSocket connection, forwarding terminal I/O between the HarmonyOS app and a shell running in an openEuler container.

## Quick Start

```bash
# ARM64 (openEuler / LOH 轻量级鸿蒙)
curl -LO https://github.com/picklerick422/fish-agent/releases/latest/download/fish-agent-linux-arm64
mv fish-agent-linux-arm64 fish-agent
chmod +x fish-agent

# x86_64
curl -LO https://github.com/picklerick422/fish-agent/releases/latest/download/fish-agent-linux-amd64
mv fish-agent-linux-amd64 fish-agent
chmod +x fish-agent

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

## 会话保持与屏幕恢复

fish-agent 支持会话保持功能，类似于 tmux。客户端断连后保持 PTY 会话存活，重连时恢复终端界面（包括正在运行的程序如 claude code）。

### 工作原理

1. **首次连接**：创建新 PTY，生成 session_id，通过控制消息发给客户端
2. **重连**：客户端带 session_id 参数连接，服务端查找已有 session 并 attach
3. **断连**：保持 session 存活，不杀进程，不关 PTY
4. **屏幕恢复**：重连时重放环形缓冲区中的输出数据

### 环形缓冲区

服务端维护一个 1MB 的环形缓冲区，保存最近的 PTY 输出。重连时重放缓冲区内容，恢复断连前的终端界面。

## 构建

```bash
go build -buildvcs=false -ldflags="-s -w" -o fish-agent .
```

## License

MIT
