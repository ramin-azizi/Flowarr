// Package handoff watches active Flowarr torrents and performs an atomic
// symlink swap once Decypharr confirms the hash is cached on Real-Debrid.
// After the swap Plex reads from the RD-backed path transparently, and
// Flowarr drops its in-progress torrent download.
package handoff

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ramin-azizi/flowarr/internal/decypharr"
	"github.com/ramin-azizi/flowarr/internal/torrent"
)

// Watcher polls Decypharr on a fixed interval and hands off torrents that
// have been confirmed cached on Real-Debrid.
type Watcher struct {
	mgr      *torrent.Manager
	dc       *decypharr.Client
	interval time.Duration

	mu     sync.Mutex
	handed map[string]bool // hashes handed off this session (idempotency guard)
}

func New(mgr *torrent.Manager, dc *decypharr.Client, interval time.Duration) *Watcher {
	return &Watcher{
		mgr:      mgr,
		dc:       dc,
		interval: interval,
		handed:   make(map[string]bool),
	}
}

// Run blocks until ctx is cancelled.
func (w *Watcher) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			w.tick(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (w *Watcher) tick(ctx context.Context) {
	cached, err := w.dc.CachedHashes(ctx)
	if err != nil {
		log.Printf("handoff poll: %v", err)
		return
	}
	if len(cached) == 0 {
		return
	}

	for _, hash := range w.mgr.Hashes() {
		w.mu.Lock()
		done := w.handed[hash]
		w.mu.Unlock()
		if done {
			continue
		}

		info, ok := cached[hash]
		if !ok {
			continue // Decypharr doesn't have this hash cached yet
		}

		name := w.mgr.TorrentName(hash)
		if name == "" {
			continue // metadata not ready
		}

		savePath := w.mgr.SavePath(hash)
		filePaths := w.mgr.FilePaths(hash)

		if savePath != "" && len(filePaths) > 0 {
			if !w.performSwap(name, info.Name, savePath, filePaths) {
				continue // swap failed — leave torrent running, retry next tick
			}
		}

		// Drop the fallback torrent if one was active for this hash.
		if fbHash := w.mgr.GetFallback(hash); fbHash != "" {
			w.mgr.Remove(fbHash)
			w.mgr.ClearFallback(hash)
			log.Printf("handoff: dropped fallback torrent %s", fbHash)
		}

		w.mgr.Remove(hash)

		w.mu.Lock()
		w.handed[hash] = true
		w.mu.Unlock()

		log.Printf("handoff complete: %s (%s) now served from %s", info.Name, hash, w.dc.MountDir())
	}
}

// performSwap atomically repoints every symlink from the Flowarr FUSE mount to
// the Decypharr/RD mount. torrentName is the local torrent's display name;
// rdName is the name Decypharr/RD uses. fileRelPaths are relative to the
// torrent root (e.g. "Season 01/ep01.mkv"). Returns false if any swap failed.
func (w *Watcher) performSwap(torrentName, rdName, savePath string, fileRelPaths []string) bool {
	for _, rel := range fileRelPaths {
		link := filepath.Join(savePath, torrentName, rel)
		newTarget := filepath.Join(w.dc.MountDir(), rdName, rel)

		if err := os.MkdirAll(filepath.Dir(link), 0755); err != nil {
			log.Printf("handoff mkdir %s: %v", filepath.Dir(link), err)
			return false
		}
		if err := atomicSwap(link, newTarget); err != nil {
			log.Printf("handoff swap %s → %s: %v", link, newTarget, err)
			return false
		}
		log.Printf("handoff swap: %s → %s", link, newTarget)
	}
	return true
}

// atomicSwap replaces link with a symlink pointing to newTarget.
// Uses a temp file + rename so the link is never absent from the filesystem.
func atomicSwap(link, newTarget string) error {
	tmp := link + ".tmp"
	os.Remove(tmp)
	if err := os.Symlink(newTarget, tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, link); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}
