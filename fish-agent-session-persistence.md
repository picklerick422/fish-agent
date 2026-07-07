# fish-agent 会话保持与屏幕恢复

> **目标：** 客户端断连后保持 PTY 会话存活，重连时恢复终端界面（包括正在运行的程序如 claude code）。
>
> **面向：** AI vibe-coding，Go 语言，WebSocket 服务端。

---

## 背景

fish-term（HarmonyOS 客户端）已实现页签恢复：重启后自动用保存的连接参数重建 WebSocket 连接。但当前 fish-agent 每次新连接都创建一个新的 PTY，旧会话直接丢弃。用户期望像 tmux 一样断连后界面还在。

## 核心改动

### 1. 会话管理器（Session Manager）

新增一个全局的 session manager，用 `session_id` 索引每个活跃的 PTY 会话。

```go
type Session struct {
    ID        string        // UUID，首次连接时生成
    PTY       *os.File      // PTY master fd
    Cmd       *exec.Cmd     // shell 进程
    OutputBuf *RingBuffer   // 环形缓冲区，存最近 N KB 输出
    CreatedAt time.Time
    LastAccess time.Time
    mu        sync.Mutex
}

type SessionManager struct {
    mu       sync.RWMutex
    sessions map[string]*Session
}
```

**行为：**
- 首次连接（无 `session_id`）：创建新 PTY，生成 `session_id`，立即通过控制消息发给客户端
- 重连（带 `session_id`）：查找已有 session，attach 到同一个 PTY
- 断连：**保持 session 存活**，不杀进程，不关 PTY
- 清理：后台 goroutine 定期清理超过 N 小时未访问的空闲 session

### 2. 协议变更

#### 2.1 连接 URL 增加 `session_id` 参数

```
ws://host:port/ws?token=xxx&cols=80&rows=24&session_id=abc123
```

客户端重启时把上次保存的 `session_id` 带上。首次连接不带此参数。

#### 2.2 新增控制消息

**服务端 → 客户端（首次连接时）：**
```json
{"type": "session", "id": "abc123-def456"}
```

**客户端 → 服务端（重连时，通过 URL 参数传递）：**
```
?session_id=abc123-def456
```

#### 2.3 已有消息保持不变

- `{"type": "cwd", "dir": "/home/user/project"}` — 已有，继续使用
- `{"type": "error", "error": "..."}` — session 不存在或已过期时使用
- `{"type": "resize", "cols": 120, "rows": 40}` — 已有，继续使用

### 3. 屏幕恢复（Output Replay）

关键挑战：断连期间服务端仍在接收 PTY 输出，但客户端没收到。重连后需要补发。

#### 3.1 环形缓冲区

```go
type RingBuffer struct {
    buf    []byte
    size   int    // 总容量（如 1MB）
    head   int    // 写入位置
    filled bool   // 是否已写满一轮
}

func (rb *RingBuffer) Write(data []byte) {
    for _, b := range data {
        rb.buf[rb.head] = b
        rb.head = (rb.head + 1) % rb.size
        if rb.head == 0 {
            rb.filled = true
        }
    }
}

// Snapshot 返回缓冲区内容的线性副本（时间顺序，最老→最新）
func (rb *RingBuffer) Snapshot() []byte {
    if !rb.filled {
        return rb.buf[:rb.head]
    }
    result := make([]byte, rb.size)
    n := copy(result, rb.buf[rb.head:])
    copy(result[n:], rb.buf[:rb.head])
    return result
}
```

容量建议 **1–2 MB**（约 1–2 万个 80 列终端的满屏内容）。

#### 3.2 PTY 输出双写

```go
func (s *Session) handleOutput() {
    buf := make([]byte, 4096)
    for {
        n, err := s.PTY.Read(buf)
        if err != nil {
            return // PTY closed
        }
        data := buf[:n]
        s.OutputBuf.Write(data)         // 写入环形缓冲区
        s.mu.Lock()
        if s.wsConn != nil {
            s.wsConn.Write(data)        // 实时发送给已连接的客户端
        }
        s.mu.Unlock()
    }
}
```

#### 3.3 重连时重放

```go
func (s *Session) attach(ws *websocket.Conn, cols, rows int) {
    s.mu.Lock()
    s.wsConn = ws
    s.mu.Unlock()
    
    // 1. 先发 session 确认
    ws.WriteJSON(map[string]string{"type": "session", "id": s.ID})
    
    // 2. 重放缓冲区（恢复屏幕内容）
    snapshot := s.OutputBuf.Snapshot()
    if len(snapshot) > 0 {
        ws.Write(snapshot)  // binary frame
    }
    
    // 3. 发送 resize 让 shell 重绘（防止缓冲区数据不完整）
    ws.WriteJSON(map[string]interface{}{
        "type": "resize", "cols": cols, "rows": rows,
    })
}
```

**注意：** 环形缓冲区只能恢复最近 1–2MB 的内容。如果断连时间很长，旧的输出会被覆盖。这是可接受的折衷——至少能恢复当前屏幕。

### 4. 客户端配合改动（fish-term）

客户端需要做两个小改动来配合服务端：

#### 4.1 保存 `session_id`

`FishWebSocketDriver` 收到 `{"type":"session","id":"..."}` 时保存 id。页签恢复时将其写入 `SavedTab`，重连时拼到 URL 里。

#### 4.2 `FishConnectConfig` 增加字段

```typescript
// entry/src/main/ets/transport/TerminalDriver.ets
export interface FishConnectConfig {
  host: string;
  port: number;
  token: string;
  tls: boolean;
  cwd?: string;
  shell?: string;
  sessionId?: string;  // 新增
}
```

---

## 实现顺序（推荐）

### Phase 1 — 最小可行（会话保持 + 简单重放）

1. 实现 `SessionManager` 和 `Session` 结构
2. 实现 `RingBuffer`（1MB 容量）
3. 修改连接处理：首次 → 创建 session，重连 → attach
4. 重连时重放环形缓冲区
5. 发送 resize 信号触发 shell 重绘

### Phase 2 — 健壮性

6. 空闲 session 清理（goroutine 定时扫描，超过 24h 未访问的 kill 掉）
7. Session 上限（最多 N 个活跃 session，超出拒绝新连接）
8. 优雅关闭（服务停止时 kill 所有 session）

### Phase 3 — 优化

9. 根据 cols×rows 动态调整环形缓冲区大小
10. 压缩存储（zstd/lz4）以减少内存占用
11. Session 元数据持久化（可选，服务重启后恢复 session）

---

## 现有代码参考

客户端恢复逻辑的关键文件（了解上下文用）：

| 文件 | 内容 |
|---|---|
| `FishWebSocketDriver.ets` | WebSocket 连接，控制消息处理（`handleCtrl`），`onCwd` 回调 |
| `FishUrl.ets` | WebSocket URL 构建（加 `session_id` 参数的地方） |
| `TerminalDriver.ets` | `FishConnectConfig` 接口定义 |
| `ConnectionStore.ets` | 客户端持久化，`SavedTab` 接口 |
| `Index.ets` | 页签恢复主逻辑 `loadRestoreTabsAndRestore()` |

---

## 效果预期

完成后用户重启 fish-term 的体验：
1. 应用启动 → 自动恢复 3 个页签
2. 每个页签自动 WebSocket 重连（带 session_id）
3. 服务端 attach 到已有 PTY，重放缓冲区
4. 终端显示断连前的内容 — **claudecode 的输出、运行中的编译、vim 界面都还在**
5. 用户可以继续在原有会话中操作
