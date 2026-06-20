package torrent

import (
	"fmt"
	"sync"
	"time"

	"github.com/anacrolix/torrent"

	"github.com/ramin-azizi/flowarr/internal/memstorage"
)

type entry struct {
	t        *torrent.Torrent
	savePath string
	category string
}

// Manager wraps the torrent client and tracks added torrents.
type Manager struct {
	client    *torrent.Client
	torrents  map[string]*entry  // keyed by infohash hex
	fallbacks map[string]string  // originalHash → fallbackHash
	trackers  [][]string         // single tier injected into every torrent
	speeds    map[string]int64   // hash → bytes/sec (updated every 2s)
	prevBytes map[string]int64   // hash → last BytesReadUsefulData sample
	mu        sync.RWMutex
}

// bestTrackers is injected into every torrent to maximise peer discovery on
// public swarms regardless of what trackers were in the original magnet link.
// List is sourced from ngosang/trackerslist (trackers_best) — verified active.
var bestTrackers = []string{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://open.stealth.si:80/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://tracker.qu.ax:6969/announce",
	"udp://open.demonii.com:1337/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
	"udp://tracker.dler.org:6969/announce",
	"udp://tracker.auctor.tv:6969/announce",
	"udp://explodie.org:6969/announce",
	"udp://p4p.arenabg.com:1337/announce",
	"udp://tracker.publictracker.xyz:6969/announce",
	"udp://wepzone.net:6969/announce",
	"udp://tracker.bittor.pw:1337/announce",
	"https://tracker.tamersunion.org:443/announce",
}

// NewManager creates a torrent client. extraTrackers are appended to the
// built-in tracker list and injected into every torrent that is added.
func NewManager(dataDir string, extraTrackers []string) (*Manager, error) {
	cfg := torrent.NewDefaultClientConfig()
	// In-memory storage: pieces live in RAM, oldest are evicted at 4 GB.
	// DataDir is unused but must be set to avoid the default file storage.
	cfg.DefaultStorage = memstorage.New(4 << 30)
	cfg.DataDir = dataDir
	cfg.Seed = false
	// More connections → more peers → better streaming throughput.
	cfg.EstablishedConnsPerTorrent = 100
	cfg.HalfOpenConnsPerTorrent = 50

	client, err := torrent.NewClient(cfg)
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}

	all := make([]string, 0, len(bestTrackers)+len(extraTrackers))
	all = append(all, bestTrackers...)
	for _, t := range extraTrackers {
		if t != "" {
			all = append(all, t)
		}
	}

	m := &Manager{
		client:    client,
		torrents:  make(map[string]*entry),
		fallbacks: make(map[string]string),
		trackers:  [][]string{all},
		speeds:    make(map[string]int64),
		prevBytes: make(map[string]int64),
	}
	go m.trackSpeeds()
	return m, nil
}

// trackSpeeds samples BytesReadUsefulData every 2 seconds and computes a
// rolling bytes/sec rate for each active torrent.
func (m *Manager) trackSpeeds() {
	const interval = 2 * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for range ticker.C {
		m.mu.Lock()
		for hash, e := range m.torrents {
			stats := e.t.Stats()
			cur := stats.BytesReadUsefulData.Int64()
			prev := m.prevBytes[hash]
			delta := cur - prev
			if delta < 0 {
				delta = 0
			}
			m.speeds[hash] = delta / int64(interval.Seconds())
			m.prevBytes[hash] = cur
		}
		m.mu.Unlock()
	}
}

// Speed returns the current download rate in bytes/sec for a torrent.
func (m *Manager) Speed(hash string) int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.speeds[hash]
}

func (m *Manager) AddMagnet(magnet, savePath, category string) (*torrent.Torrent, error) {
	t, err := m.client.AddMagnet(magnet)
	if err != nil {
		return nil, err
	}

	t.AddTrackers(m.trackers)

	hash := t.InfoHash().HexString()

	m.mu.Lock()
	m.torrents[hash] = &entry{t: t, savePath: savePath, category: category}
	m.mu.Unlock()

	go func() {
		<-t.GotInfo()
		// Prioritise only the last piece so Plex can read duration/index
		// metadata immediately. All other pieces are fetched on-demand as
		// Plex reads through the FUSE — nothing is pre-downloaded.
		n := t.NumPieces()
		if n > 0 {
			t.Piece(n - 1).SetPriority(torrent.PiecePriorityNow)
		}
	}()

	return t, nil
}

func (m *Manager) List() []*torrent.Torrent {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*torrent.Torrent, 0, len(m.torrents))
	for _, e := range m.torrents {
		out = append(out, e.t)
	}
	return out
}

func (m *Manager) Get(hash string) (*torrent.Torrent, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.torrents[hash]
	if !ok {
		return nil, false
	}
	return e.t, true
}

func (m *Manager) SavePath(hash string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.torrents[hash]; ok {
		return e.savePath
	}
	return ""
}

func (m *Manager) Category(hash string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if e, ok := m.torrents[hash]; ok {
		return e.category
	}
	return ""
}

// Hashes returns the infohash hex strings of all tracked torrents.
func (m *Manager) Hashes() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]string, 0, len(m.torrents))
	for h := range m.torrents {
		out = append(out, h)
	}
	return out
}

// TorrentName returns the torrent's display name, or "" if metadata isn't ready.
func (m *Manager) TorrentName(hash string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.torrents[hash]
	if !ok || e.t.Info() == nil {
		return ""
	}
	return e.t.Name()
}

// FilePaths returns each file's path relative to the torrent root directory
// (e.g. "Season 01/ep01.mkv"), or nil if metadata isn't ready yet.
func (m *Manager) FilePaths(hash string) []string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	e, ok := m.torrents[hash]
	if !ok || e.t.Info() == nil {
		return nil
	}
	files := e.t.Files()
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.DisplayPath() // relative to torrent root, preserves subdirs
	}
	return out
}

func (m *Manager) Remove(hash string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.torrents[hash]
	if !ok {
		return false
	}
	e.t.Drop()
	delete(m.torrents, hash)
	return true
}


// SetFallback records that fallbackHash is serving as a streaming substitute
// for the original torrent identified by originalHash.
func (m *Manager) SetFallback(originalHash, fallbackHash string) {
	m.mu.Lock()
	m.fallbacks[originalHash] = fallbackHash
	m.mu.Unlock()
}

// GetFallback returns the fallback hash for an original hash, or "".
func (m *Manager) GetFallback(originalHash string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.fallbacks[originalHash]
}

// ClearFallback removes the fallback mapping for originalHash.
func (m *Manager) ClearFallback(originalHash string) {
	m.mu.Lock()
	delete(m.fallbacks, originalHash)
	m.mu.Unlock()
}

func (m *Manager) Close() error {
	errs := m.client.Close()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}
