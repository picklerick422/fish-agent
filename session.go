package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

type Session struct {
	ID         string
	PTY        *os.File
	Cmd        *exec.Cmd
	CWD        string
	Shell      string
	OutputBuf  *RingBuffer
	wsConn     interface{}
	CreatedAt  time.Time
	LastAccess time.Time
	mu         sync.Mutex
	closed     bool
	dead       bool // true once the PTY process has exited (EOF on read)

	// WebSocket writes are not safe for concurrent use by multiple goroutines.
	// ptyToWS, pollCwd, and handleCtrl all write to the same conn — serialize
	// them through this mutex.
	writeMu sync.Mutex
}

type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]*Session
	token    string // auth token, injected into session env for hook reporting
}

func NewSessionManager(token string) *SessionManager {
	return &SessionManager{sessions: make(map[string]*Session), token: token}
}

// Notify delivers a claude-code hook event (completed / awaiting) to the
// session's active WebSocket connection, if any. Best-effort: events fired
// while the client is disconnected are dropped by design.
func (sm *SessionManager) Notify(sessionID, event, message string) {
	sm.mu.Lock()
	session := sm.sessions[sessionID]
	sm.mu.Unlock()
	if session == nil {
		return
	}
	session.mu.Lock()
	conn := session.wsConn
	session.mu.Unlock()
	ws, ok := conn.(*websocket.Conn)
	if !ok || ws == nil {
		return
	}
	msg, _ := json.Marshal(map[string]string{
		"type":    "task_event",
		"event":   event,
		"message": message,
	})
	session.writeWS(ws, websocket.TextMessage, msg)
}

var loginShell string

func init() {
	loginShell = os.Getenv("SHELL")
	if loginShell == "" {
		loginShell = "/bin/bash"
	}
}

func (sm *SessionManager) Create(cwd string, cols, rows int, shell ...string) (*Session, error) {
	shellPath := loginShell
	if len(shell) > 0 && shell[0] != "" {
		shellPath = shell[0]
	}
	cmd := exec.Command(shellPath, "-l")
	if cwd != "" {
		cmd.Dir = cwd
	} else if home, err := os.UserHomeDir(); err == nil {
		cmd.Dir = home
	}
	env := os.Environ()
	if os.Getenv("LANG") == "" {
		env = append(env, "LANG=C.UTF-8")
	}
	// Generate the session ID before starting the PTY so it can be injected
	// into the environment: claude-code hooks running inside this session
	// report completion / permission events back to /notify with these vars.
	id := newID()
	env = append(env,
		"TERM=xterm-256color",
		"COLORTERM=truecolor",
		"TERM_PROGRAM=fish-agent",
		"TERM_PROGRAM_VERSION=0.1.0",
		"FISH_SESSION_ID="+id,
		"FISH_TOKEN="+sm.token,
	)
	cmd.Env = env

	fd, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: uint16(rows), Cols: uint16(cols)})
	if err != nil {
		return nil, fmt.Errorf("pty start: %w", err)
	}

	session := &Session{
		ID:         id,
		PTY:        fd,
		Cmd:        cmd,
		CWD:        cwd,
		Shell:      shellPath,
		OutputBuf:  NewRingBuffer(defaultRingBufferSize),
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
	}

	if cwd == "" {
		link := fmt.Sprintf("/proc/%d/cwd", cmd.Process.Pid)
		if wd, err := os.Readlink(link); err == nil {
			session.CWD = wd
		}
	}

	sm.mu.Lock()
	sm.sessions[session.ID] = session
	sm.mu.Unlock()

	return session, nil
}

func (sm *SessionManager) Get(id string) *Session {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	session, ok := sm.sessions[id]
	if !ok {
		return nil
	}
	session.mu.Lock()
	dead := session.dead
	session.LastAccess = time.Now()
	session.mu.Unlock()
	if dead {
		return nil
	}
	return session
}

func (sm *SessionManager) Remove(id string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.sessions, id)
}

func (sm *SessionManager) Cleanup(maxIdle time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	now := time.Now()
	for id, session := range sm.sessions {
		session.mu.Lock()
		if session.dead || now.Sub(session.LastAccess) > maxIdle {
			session.closed = true
			if session.PTY != nil {
				session.PTY.Close()
			}
			if session.Cmd != nil && session.Cmd.Process != nil {
				session.Cmd.Process.Kill()
				session.Cmd.Wait()
			}
			delete(sm.sessions, id)
		}
		session.mu.Unlock()
	}
}

func (s *Session) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	s.PTY.Close()
}

func (s *Session) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func newID() string {
	buf := make([]byte, 16)
	rand.Read(buf)
	return hex.EncodeToString(buf)
}

func (s *Session) sendSessionID(conn *websocket.Conn) {
	resp, _ := json.Marshal(map[string]string{"type": "session", "id": s.ID})
	s.writeWS(conn, websocket.TextMessage, resp)
}

func (s *Session) attach(conn *websocket.Conn, cols, rows int) {
	s.mu.Lock()
	s.wsConn = conn
	s.LastAccess = time.Now()
	s.mu.Unlock()

	s.sendSessionID(conn)

	snapshot := s.OutputBuf.Snapshot()
	if len(snapshot) > 0 {
		s.writeWS(conn, websocket.BinaryMessage, snapshot)
	}

	conn.WriteJSON(map[string]interface{}{
		"type": "resize", "cols": cols, "rows": rows,
	})
}

// writeWS serialises all writes to a single WebSocket connection so that
// concurrent goroutines (ptyToWS, pollCwd, handleCtrl responses) never
// interleave frames and trigger a flushFrame panic.
func (s *Session) writeWS(conn *websocket.Conn, messageType int, data []byte) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteMessage(messageType, data)
}
