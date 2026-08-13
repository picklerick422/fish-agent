package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"sort"
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

	var session *Session
	defer func() {
		// Clear the session's wsConn reference when this connection dies so
		// /notify doesn't write task events into a closed socket.
		if session != nil {
			session.mu.Lock()
			if session.wsConn == conn {
				session.wsConn = nil
			}
			session.mu.Unlock()
		}
		conn.Close()
	}()
	if sessionID != "" {
		session = sm.Get(sessionID)
		if session == nil {
			// Session is dead (PTY process exited) or expired — create a fresh
			// session so the client isn't stuck in a reconnect loop. A new
			// session ID is pushed via the session control message so the client
			// can persist it for future reconnects.  We use the same CWD and
			// shell from the URL so the environment matches what the user
			// originally configured.
			session, err = sm.Create(cwd, cols, rows, shell)
			if err != nil {
				log.Printf("create session (for stale session_id %s): %v", sessionID, err)
				resp, _ := json.Marshal(map[string]string{"type": "error", "error": err.Error()})
				conn.WriteMessage(websocket.TextMessage, resp)
				return
			}
			session.sendSessionID(conn)
		} else {
			session.attach(conn, cols, rows)
		}
	} else {
		session, err = sm.Create(cwd, cols, rows, shell)
		if err != nil {
			log.Printf("create session: %v", err)
			return
		}
		session.sendSessionID(conn)
	}

	// Register this connection on the session so /notify can route claude-code
	// hook events to it. attach() does the same for reconnects; doing it here
	// covers fresh sessions too (their first connection never calls attach).
	session.mu.Lock()
	session.wsConn = conn
	session.mu.Unlock()

	// Push initial CWD to frontend
	if initCwd, _ := json.Marshal(map[string]string{"type": "cwd", "dir": session.CWD}); initCwd != nil {
		session.writeWS(conn, websocket.TextMessage, initCwd)
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
				session.writeWS(conn, websocket.TextMessage, msg)
			}
		}
	}
}

// ptyToWS reads PTY output and forwards it to the WebSocket.
// Task-completion / awaiting-input notifications are not derived from PTY
// output analysis here — they arrive via claude-code hooks on /notify.
func ptyToWS(session *Session, conn *websocket.Conn, done chan struct{}) {
	buf := make([]byte, 65536)
	acc := make([]byte, 0, 131072)

	for {
		n, err := session.PTY.Read(buf)
		if err != nil {
			// Connection or PTY closed.
			if len(acc) > 0 {
				session.writeWS(conn, websocket.BinaryMessage, acc)
			}
			// Mark the session as dead so future reconnect attempts with this
			// session_id won't replay stale ring-buffer output for a dead PTY.
			session.mu.Lock()
			session.dead = true
			session.closed = true
			session.mu.Unlock()
			close(done)
			return
		}
		if session.isClosed() {
			return
		}
		acc = append(acc, buf[:n]...)
		session.OutputBuf.Write(buf[:n])
		if len(acc) >= 65536 || n < len(buf) {
			if err := session.writeWS(conn, websocket.BinaryMessage, acc); err != nil {
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
	Path string `json:"path,omitempty"` // list_dir target path
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
		session.writeWS(conn, websocket.TextMessage, resp)
		return true
	case "cwd":
		resp, _ := json.Marshal(map[string]string{"type": "cwd", "dir": session.CWD})
		session.writeWS(conn, websocket.TextMessage, resp)
		return true
	case "fork":
		cwd := session.CWD
		if ctrl.CWD != "" {
			cwd = ctrl.CWD
		}
		forked, err := sm.Create(cwd, *cols, *rows, session.Shell)
		if err != nil {
			resp, _ := json.Marshal(map[string]string{"type": "error", "error": err.Error()})
			session.writeWS(conn, websocket.TextMessage, resp)
			return true
		}
		resp, _ := json.Marshal(map[string]string{"type": "forked", "id": forked.ID})
		session.writeWS(conn, websocket.TextMessage, resp)
		return true
	case "list_dir":
		result := handleListDir(ctrl.Path)
		resp, _ := json.Marshal(result)
		session.writeWS(conn, websocket.TextMessage, resp)
		return true
	case "file_read":
		result := handleFileRead(ctrl.Path)
		resp, _ := json.Marshal(result)
		session.writeWS(conn, websocket.TextMessage, resp)
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

// --- list_dir support --------------------------------------------------------

// listDirEntry describes a single directory entry in a list_dir_result.
type listDirEntry struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Size  int64  `json:"size,omitempty"`
	Mtime int64  `json:"mtime,omitempty"`
	Mode  string `json:"mode,omitempty"`
}

// listDirResult is the response sent back for a list_dir request.
type listDirResult struct {
	Type      string         `json:"type"`
	Path      string         `json:"path"`
	Entries   []listDirEntry `json:"entries"`
	Truncated bool           `json:"truncated,omitempty"`
	Error     string         `json:"error,omitempty"`
}

// handleListDir reads a directory and returns its contents as a list_dir_result.
func handleListDir(path string) listDirResult {
	const maxEntries = 2000

	if path == "" {
		return listDirResult{Type: "list_dir_result", Path: path, Error: "missing path"}
	}
	if !strings.HasPrefix(path, "/") {
		return listDirResult{Type: "list_dir_result", Path: path, Error: "path must be absolute"}
	}
	if strings.Contains(path, "..") {
		return listDirResult{Type: "list_dir_result", Path: path, Error: "invalid path"}
	}

	entries, err := os.ReadDir(path)
	if err != nil {
		return listDirResult{Type: "list_dir_result", Path: path, Error: err.Error()}
	}

	// Separate directories and files; sort each group case-insensitively.
	dirs := make([]fs.DirEntry, 0, len(entries))
	files := make([]fs.DirEntry, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			dirs = append(dirs, e)
		} else {
			files = append(files, e)
		}
	}
	sort.Slice(dirs, func(i, j int) bool {
		return strings.ToLower(dirs[i].Name()) < strings.ToLower(dirs[j].Name())
	})
	sort.Slice(files, func(i, j int) bool {
		return strings.ToLower(files[i].Name()) < strings.ToLower(files[j].Name())
	})

	// Hide dotfiles at the end within each group.
	sort.SliceStable(dirs, func(i, j int) bool {
		return !strings.HasPrefix(dirs[i].Name(), ".") && strings.HasPrefix(dirs[j].Name(), ".")
	})
	sort.SliceStable(files, func(i, j int) bool {
		return !strings.HasPrefix(files[i].Name(), ".") && strings.HasPrefix(files[j].Name(), ".")
	})

	truncated := false
	total := len(dirs) + len(files)
	if total > maxEntries {
		truncated = true
		// Truncate files first, then dirs if still over.
		keepFiles := maxEntries - len(dirs)
		if keepFiles < 0 {
			keepFiles = 0
		}
		if len(files) > keepFiles {
			files = files[:keepFiles]
		}
		if len(dirs)+len(files) > maxEntries {
			dirs = dirs[:maxEntries-len(files)]
		}
	}

	result := make([]listDirEntry, 0, len(dirs)+len(files))
	for _, e := range dirs {
		info, err := e.Info()
		entry := listDirEntry{Name: e.Name(), Type: "directory"}
		if err == nil {
			entry.Size = info.Size()
			entry.Mtime = info.ModTime().UnixMilli()
			entry.Mode = info.Mode().String()
		}
		result = append(result, entry)
	}
	for _, e := range files {
		info, err := e.Info()
		t := "file"
		if e.Type()&fs.ModeSymlink != 0 {
			t = "symlink"
		}
		entry := listDirEntry{Name: e.Name(), Type: t}
		if err == nil {
			entry.Size = info.Size()
			entry.Mtime = info.ModTime().UnixMilli()
			entry.Mode = info.Mode().String()
		}
		result = append(result, entry)
	}

	return listDirResult{
		Type:      "list_dir_result",
		Path:      path,
		Entries:   result,
		Truncated: truncated,
	}
}
// --- file_read support -------------------------------------------------------

// fileReadResult is the response sent back for a file_read request.
type fileReadResult struct {
	Type    string `json:"type"`
	Path    string `json:"path"`
	Content string `json:"content,omitempty"` // base64-encoded
	Size    int64  `json:"size,omitempty"`
	Error   string `json:"error,omitempty"`
}

// handleFileRead reads a file and returns its base64-encoded content.
func handleFileRead(path string) fileReadResult {
	if path == "" {
		return fileReadResult{Type: "file_read_result", Path: path, Error: "missing path"}
	}
	if !strings.HasPrefix(path, "/") {
		return fileReadResult{Type: "file_read_result", Path: path, Error: "path must be absolute"}
	}
	if strings.Contains(path, "..") {
		return fileReadResult{Type: "file_read_result", Path: path, Error: "invalid path"}
	}

	info, err := os.Stat(path)
	if err != nil {
		return fileReadResult{Type: "file_read_result", Path: path, Error: err.Error()}
	}
	if info.IsDir() {
		return fileReadResult{Type: "file_read_result", Path: path, Error: "is a directory"}
	}
	// Limit file size to 10 MB
	if info.Size() > 10*1024*1024 {
		return fileReadResult{Type: "file_read_result", Path: path, Error: "file too large (>10MB)"}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return fileReadResult{Type: "file_read_result", Path: path, Error: err.Error()}
	}

	return fileReadResult{
		Type:    "file_read_result",
		Path:    path,
		Content: base64.StdEncoding.EncodeToString(data),
		Size:    info.Size(),
	}
}
