package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"strings"
	"time"
)

func main() {
	addr := flag.String("addr", ":8765", "listen address")
	token := flag.String("token", "", "auth token (auto-generated if empty)")
	flag.Parse()

	authToken := *token
	if authToken == "" {
		buf := make([]byte, 16)
		rand.Read(buf)
		authToken = hex.EncodeToString(buf)
	}

	sm := NewSessionManager(authToken)

	go func() {
		ticker := time.NewTicker(1 * time.Hour)
		defer ticker.Stop()
		for range ticker.C {
			sm.Cleanup(24 * time.Hour)
		}
	}()

	log.Printf("login shell: %s", loginShell)
	if data, err := os.ReadFile("/etc/shells"); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			line = strings.TrimSpace(line)
			if line != "" && !strings.HasPrefix(line, "#") {
				log.Printf("available shell: %s", line)
			}
		}
	}

	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		r.ParseForm()
		if r.Form.Get("token") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		handleWS(sm, w, r)
	})

	// /notify receives claude-code hook events (Stop -> "completed",
	// Notification/permission_prompt -> "awaiting") from hook scripts running
	// inside session shells. The hook script authenticates with the same token
	// it inherits from the session env (FISH_TOKEN) and routes the event to the
	// owning session via FISH_SESSION_ID.
	http.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("X-Fish-Token") != authToken {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var payload struct {
			Session string `json:"session"`
			Event   string `json:"event"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		if payload.Session == "" || (payload.Event != "completed" && payload.Event != "awaiting") {
			http.Error(w, "bad request", http.StatusBadRequest)
			return
		}
		sm.Notify(payload.Session, payload.Event, payload.Message)
		log.Printf("notify: session=%s event=%s", payload.Session, payload.Event)
		w.WriteHeader(http.StatusOK)
	})

	log.Printf("fish-agent listening on %s", *addr)
	log.Printf("auth token: %s", authToken)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
