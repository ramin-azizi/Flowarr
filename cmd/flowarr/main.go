package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

const version = "0.1.0"

// qBittorrent-compatible auth endpoint
func authLogin(w http.ResponseWriter, r *http.Request) {
	// Radarr/Sonarr send POST with username/password form data
	// We accept anything for now and respond with "Ok."
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "Ok.")
}

// qBittorrent-compatible app version endpoint
func appVersion(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "v4.6.0") // pretend to be qBittorrent v4.6.0
}

// qBittorrent-compatible API version endpoint
func apiVersion(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "2.9.3")
}

// qBittorrent-compatible torrents/info — returns empty list for now
func torrentsInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode([]interface{}{})
}

// Flowarr-specific health endpoint
func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": version,
	})
}

func main() {
	mux := http.NewServeMux()

	// qBittorrent-compatible endpoints
	mux.HandleFunc("/api/v2/auth/login", authLogin)
	mux.HandleFunc("/api/v2/app/version", appVersion)
	mux.HandleFunc("/api/v2/app/webapiVersion", apiVersion)
	mux.HandleFunc("/api/v2/torrents/info", torrentsInfo)

	// Flowarr-specific endpoints
	mux.HandleFunc("/api/health", health)

	addr := ":8888"
	log.Printf("Flowarr v%s starting on %s", version, addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}
