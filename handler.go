package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/creack/pty"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func handleWS(sm *SessionManager, w http.ResponseWriter, r *http.Request) {
	cols, rows := 80, 24
	cwd := ""
	sessionID := ""
	if err := r.ParseForm(); err == nil {
		if c := r.Form.Get("cols"); c != "" {
			if v, ok := parseInt(c); ok {
				cols = v
			}
		}
		if r := r.Form.Get("rows"); r != "" {
			if v, ok := parseInt(r); ok {
				rows = v
			}
		}
		cwd = r.Form.Get("cwd")
		sessionID = r.Form.Get("session_id")
	}
	shell := r.Form.Get("shell")
	if shell == "" {
		shell = loginShell
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("upgrade: %v", err)
		return
	}
	defer conn.Close()

	var session *Session
	if sessionID != "" {
		session = sm.Get(sessionID)
		if session == nil {
			resp, _ := json.Marshal(map[string]string{"type": "error", "error": "session not found or expired"})
			conn.WriteMessage(websocket.TextMessage, resp)
			return
		}
		session.attach(conn, cols, rows)
	} else {
		session, err = sm.Create(cwd, cols, rows, shell)
		if err != nil {
			log.Printf("create session: %v", err)
			return
		}
		session.sendSessionID(conn)
	}

	// Push initial CWD to frontend
	if initCwd, _ := json.Marshal(map[string]string{"type": "cwd", "dir": session.CWD}); initCwd != nil {
		conn.WriteMessage(websocket.TextMessage, initCwd)
	}

	done := make(chan struct{})

	// Start CWD polling goroutine
	go pollCwd(session, conn, done)

	go ptyToWS(session, conn, done)

	wsToPTY(sm, session, conn, done)
}

func pollCwd(session *Session, conn *websocket.Conn, done chan struct{}) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
		}
		if session.isClosed() {
			return
		}
		link := fmt.Sprintf("/proc/%d/cwd", session.Cmd.Process.Pid)
		if wd, err := os.Readlink(link); err == nil && wd != session.CWD {
			session.mu.Lock()
			session.CWD = wd
			session.mu.Unlock()
			if msg, err := json.Marshal(map[string]string{"type": "cwd", "dir": wd}); err == nil {
				conn.WriteMessage(websocket.TextMessage, msg)
			}
		}
	}
}

func ptyToWS(session *Session, conn *websocket.Conn, done chan struct{}) {
	buf := make([]byte, 65536)
	acc := make([]byte, 0, 131072)
	for {
		n, err := session.PTY.Read(buf)
		if err != nil {
			if len(acc) > 0 {
				conn.WriteMessage(websocket.BinaryMessage, acc)
			}
			close(done)
			return
		}
		if session.isClosed() {
			return
		}
		acc = append(acc, buf[:n]...)
		session.OutputBuf.Write(buf[:n])
		if len(acc) >= 65536 || n < len(buf) {
			if err := conn.WriteMessage(websocket.BinaryMessage, acc); err != nil {
				close(done)
				return
			}
			acc = acc[:0]
		}
	}
}

func wsToPTY(sm *SessionManager, session *Session, conn *websocket.Conn, done chan struct{}) {
	cols, rows := 80, 24

	for {
		_, msg, err := conn.ReadMessage()
		if err != nil {
			return
		}
		if session.isClosed() {
			return
		}

		log.Printf("wsToPTY received %d bytes", len(msg))

		if isJSONCtrl(msg) {
			if handled := handleCtrl(sm, session, conn, msg, &cols, &rows); handled {
				continue
			}
		}

		// Bracketed paste mode markers — bash 4.4+ processes paste without
		// echoing each character, dramatically reducing PTY round-trips.
		useBracketedPaste := len(msg) > 4096
		if useBracketedPaste {
			session.PTY.Write([]byte("\x1b[200~"))
		}
		chunkSize := 16384
		for off := 0; off < len(msg); off += chunkSize {
			end := off + chunkSize
			if end > len(msg) {
				end = len(msg)
			}
			chunk := msg[off:end]
			n, err := session.PTY.Write(chunk)
			if err != nil {
				log.Printf("PTY write error at offset %d: %v", off, err)
				break
			}
			if n < len(chunk) {
				log.Printf("PTY partial write: %d < %d at offset %d", n, len(chunk), off)
				time.Sleep(5 * time.Millisecond)
			}
		}
		if useBracketedPaste {
			session.PTY.Write([]byte("\x1b[201~"))
		}
		log.Printf("PTY write total %d bytes", len(msg))
	}
}

func isJSONCtrl(msg []byte) bool {
	return len(msg) > 0 && msg[0] == '{'
}

type ctrlMsg struct {
	Type string `json:"type"`
	Cols int    `json:"cols,omitempty"`
	Rows int    `json:"rows,omitempty"`
	CWD  string `json:"cwd,omitempty"`
}

func handleCtrl(sm *SessionManager, session *Session, conn *websocket.Conn, msg []byte, cols, rows *int) bool {
	var ctrl ctrlMsg
	if err := json.Unmarshal(msg, &ctrl); err != nil || ctrl.Type == "" {
		return false
	}

	switch ctrl.Type {
	case "resize":
		if ctrl.Cols > 0 && ctrl.Rows > 0 {
			*cols = ctrl.Cols
			*rows = ctrl.Rows
			pty.Setsize(session.PTY, &pty.Winsize{
				Rows: uint16(ctrl.Rows),
				Cols: uint16(ctrl.Cols),
			})
		}
		return true
	case "ping":
		return true
	case "shells":
		data, _ := os.ReadFile("/etc/shells")
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		var shells []string
		for _, line := range lines {
			if trimmed := strings.TrimSpace(line); trimmed != "" && !strings.HasPrefix(trimmed, "#") {
				shells = append(shells, trimmed)
			}
		}
		resp, _ := json.Marshal(map[string]interface{}{"type": "shells", "list": shells})
		conn.WriteMessage(websocket.TextMessage, resp)
		return true
	case "cwd":
		resp, _ := json.Marshal(map[string]string{"type": "cwd", "dir": session.CWD})
		conn.WriteMessage(websocket.TextMessage, resp)
		return true
	case "fork":
		cwd := session.CWD
		if ctrl.CWD != "" {
			cwd = ctrl.CWD
		}
		forked, err := sm.Create(cwd, *cols, *rows, session.Shell)
		if err != nil {
			resp, _ := json.Marshal(map[string]string{"type": "error", "error": err.Error()})
			conn.WriteMessage(websocket.TextMessage, resp)
			return true
		}
		resp, _ := json.Marshal(map[string]string{"type": "forked", "id": forked.ID})
		conn.WriteMessage(websocket.TextMessage, resp)
		return true
	}
	return false
}

func cleanupSession(sm *SessionManager, session *Session) {
	session.Close()
	session.Cmd.Process.Kill()
	session.Cmd.Wait()
	sm.Remove(session.ID)
}

func parseInt(s string) (int, bool) {
	var n int
	for _, c := range s {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int(c-'0')
	}
	return n, true
}
