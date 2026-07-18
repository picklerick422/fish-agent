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

	// Awaiting-input detection (backend-driven notification signal).
	lastOutput       time.Time
	awaitingNotified bool
	outputMu         sync.Mutex
}

// --- awaiting-input helpers -------------------------------------------------

func (s *Session) noteOutput() {
	s.outputMu.Lock()
	s.lastOutput = time.Now()
	s.outputMu.Unlock()
}

func (s *Session) timeSinceOutput() time.Duration {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	return time.Since(s.lastOutput)
}

func (s *Session) setAwaitingNotified(v bool) {
	s.outputMu.Lock()
	s.awaitingNotified = v
	s.outputMu.Unlock()
}

func (s *Session) isAwaitingNotified() bool {
	s.outputMu.Lock()
	defer s.outputMu.Unlock()
	return s.awaitingNotified
}

type SessionManager struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

func NewSessionManager() *SessionManager {
	return &SessionManager{sessions: make(map[string]*Session)}
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
	cmd.Env = append(env,
		"TERM=xterm-256color",
		"COLORTERM=truecolor",
		"TERM_PROGRAM=fish-agent",
		"TERM_PROGRAM_VERSION=0.1.0",
	)

	fd, err := pty.StartWithSize(cmd, &pty.Winsize{Rows: uint16(rows), Cols: uint16(cols)})
	if err != nil {
		return nil, fmt.Errorf("pty start: %w", err)
	}

	session := &Session{
		ID:         newID(),
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
	if ok {
		session.mu.Lock()
		session.LastAccess = time.Now()
		session.mu.Unlock()
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
		if now.Sub(session.LastAccess) > maxIdle {
			session.closed = true
			session.PTY.Close()
			session.Cmd.Process.Kill()
			session.Cmd.Wait()
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
	conn.WriteMessage(websocket.TextMessage, resp)
}

func (s *Session) attach(conn *websocket.Conn, cols, rows int) {
	s.mu.Lock()
	s.wsConn = conn
	s.LastAccess = time.Now()
	s.mu.Unlock()

	s.sendSessionID(conn)

	snapshot := s.OutputBuf.Snapshot()
	if len(snapshot) > 0 {
		conn.WriteMessage(websocket.BinaryMessage, snapshot)
	}

	conn.WriteJSON(map[string]interface{}{
		"type": "resize", "cols": cols, "rows": rows,
	})
}
