package main

import (
	"crypto/rand"
	"encoding/hex"
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

	sm := NewSessionManager()

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

	log.Printf("fish-agent listening on %s", *addr)
	log.Printf("auth token: %s", authToken)
	log.Fatal(http.ListenAndServe(*addr, nil))
}
