// Package seeder manages a BitTorrent client that seeds from the Real-Debrid
// FUSE mount. No data is downloaded locally; anacrolix/torrent reads pieces
// directly from the RD mount (keeping files cached on RD) and uploads them to
// the swarm. Up to MaxActive torrents seed simultaneously; the rest are queued
// and rotated in over time so the entire library is covered periodically.
//
// When campaign settings are configured the seeder also tracks per-torrent
// upload progress across a "cycle". Each torrent is rotated out once it hits
// its min-seed target (or the hard caps). Torrents with no peers for too long
// get chunks pulled directly from the Decypharr FUSE mount to keep them warm
// in RD's cache, then are marked done for this cycle.
package seeder

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/anacrolix/torrent"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
	"golang.org/x/time/rate"
)

// quietHandler suppresses known-harmless WARN messages from anacrolix storage.
type quietHandler struct{ slog.Handler }

func (q quietHandler) Handle(ctx context.Context, r slog.Record) error {
	if r.Level == slog.LevelWarn &&
		(strings.Contains(r.Message, "error checking file size for piece marked as complete") ||
			strings.Contains(r.Message, "file too small for piece marked as complete")) {
		return nil
	}
	return q.Handler.Handle(ctx, r)
}

// alwaysComplete reports every piece as verified without reading data.
type alwaysComplete struct{}

func (alwaysComplete) Get(_ metainfo.PieceKey) (storage.Completion, error) {
	return storage.Completion{Ok: true, Complete: true}, nil
}
func (alwaysComplete) Set(_ metainfo.PieceKey, _ bool) error { return nil }
func (alwaysComplete) Close() error                          { return nil }

// SeedItem is a torrent the seeder should manage, sourced from decypharr.
type SeedItem struct {
	Hash string
	Name string
}

// Config holds all runtime-adjustable seeder settings.
type Config struct {
	MountDir        string
	StateDir        string
	UploadLimitMBPS int
	MaxActive       int
	RotateAfter     time.Duration

	FavorYearFrom        int
	FavorYearTo          int
	PrioritizeLowSeeders bool

	// Campaign — all zero/empty disables campaign mode.
	CampaignMinSeedMB        float64
	CampaignMaxSeedMB        float64
	CampaignMinTime          time.Duration
	CampaignMaxTime          time.Duration
	CampaignPeerWait         time.Duration
	CampaignChunkDir         string
	CampaignChunkDirFallback string
	CampaignChunkSizeMB      float64
	CampaignLoop             bool
}

func (c Config) campaignEnabled() bool {
	return c.CampaignMinSeedMB > 0 || c.CampaignMinTime > 0
}

type seedState int

const (
	stateVerifying seedState = iota
	stateQueued
	stateActive
	statePaused
)

func (s seedState) String() string {
	switch s {
	case stateActive:
		return "seeding"
	case stateQueued:
		return "queued"
	case statePaused:
		return "paused"
	default:
		return "verifying"
	}
}

var yearRe = regexp.MustCompile(`\b((?:19|20)\d{2})\b`)

func extractYear(name string) int {
	if m := regexp.MustCompile(`\((\d{4})\)`).FindStringSubmatch(name); m != nil {
		if y, err := strconv.Atoi(m[1]); err == nil && y >= 1900 && y <= 2100 {
			return y
		}
	}
	if m := yearRe.FindStringSubmatch(name); m != nil {
		if y, err := strconv.Atoi(m[1]); err == nil {
			return y
		}
	}
	return 0
}

type entry struct {
	t           *torrent.Torrent
	magnet      string
	hash        string
	name        string
	year        int
	addedAt     time.Time
	activeSince time.Time
	state       seedState

	// Campaign cycle tracking — reset each cycle.
	cycleStart       time.Time // when this torrent was last activated in the current cycle
	cycleUploadStart int64     // BytesWrittenData at cycleStart
	cycleDone        bool      // has this torrent met its campaign target this cycle?
	peerlessStart    time.Time // when we first noticed 0 peers; zero = has peers / not tracked
}

// Seeder manages rotating torrent seeding from the RD mount.
type Seeder struct {
	cfg    Config
	client *torrent.Client
	lim    *rate.Limiter

	mu           sync.Mutex
	entries      map[string]*entry
	order        []string
	prevUpload   map[string]int64
	prevUploadAt time.Time
}

// New creates a Seeder.
func New(cfg Config) (*Seeder, error) {
	slog.SetDefault(slog.New(quietHandler{slog.NewTextHandler(os.Stderr, nil)}))

	cc := torrent.NewDefaultClientConfig()
	cc.DefaultStorage = storage.NewFileOpts(storage.NewFileClientOpts{
		ClientBaseDir:   cfg.MountDir,
		PieceCompletion: alwaysComplete{},
	})
	cc.Slogger = slog.New(quietHandler{slog.Default().Handler()})
	cc.DataDir = cfg.StateDir
	cc.Seed = true
	cc.NoUpload = false
	cc.EstablishedConnsPerTorrent = 80
	cc.HalfOpenConnsPerTorrent = 40
	cc.ListenPort = 42070

	var lim *rate.Limiter
	if cfg.UploadLimitMBPS > 0 {
		bps := rate.Limit(int64(cfg.UploadLimitMBPS) * 1024 * 1024)
		lim = rate.NewLimiter(bps, int(bps))
		cc.UploadRateLimiter = lim
	}

	client, err := torrent.NewClient(cc)
	if err != nil {
		return nil, fmt.Errorf("create seeder client: %w", err)
	}

	s := &Seeder{
		cfg:          cfg,
		client:       client,
		lim:          lim,
		entries:      make(map[string]*entry),
		prevUpload:   make(map[string]int64),
		prevUploadAt: time.Now(),
	}
	go s.rotationLoop()
	return s, nil
}

// ApplyConfig updates runtime-adjustable settings without restarting.
func (s *Seeder) ApplyConfig(cfg Config) {
	if cfg.UploadLimitMBPS > 0 {
		bps := rate.Limit(int64(cfg.UploadLimitMBPS) * 1024 * 1024)
		if s.lim != nil {
			s.lim.SetLimit(bps)
		}
	} else if s.lim != nil {
		s.lim.SetLimit(rate.Inf)
	}

	s.mu.Lock()
	s.cfg.UploadLimitMBPS = cfg.UploadLimitMBPS
	s.cfg.MaxActive = cfg.MaxActive
	s.cfg.RotateAfter = cfg.RotateAfter
	s.cfg.FavorYearFrom = cfg.FavorYearFrom
	s.cfg.FavorYearTo = cfg.FavorYearTo
	s.cfg.PrioritizeLowSeeders = cfg.PrioritizeLowSeeders
	s.cfg.CampaignMinSeedMB = cfg.CampaignMinSeedMB
	s.cfg.CampaignMaxSeedMB = cfg.CampaignMaxSeedMB
	s.cfg.CampaignMinTime = cfg.CampaignMinTime
	s.cfg.CampaignMaxTime = cfg.CampaignMaxTime
	s.cfg.CampaignPeerWait = cfg.CampaignPeerWait
	s.cfg.CampaignChunkDir = cfg.CampaignChunkDir
	s.cfg.CampaignChunkDirFallback = cfg.CampaignChunkDirFallback
	s.cfg.CampaignChunkSizeMB = cfg.CampaignChunkSizeMB
	s.rebalanceLocked()
	s.mu.Unlock()
}

// ResetCycle clears all per-torrent cycle progress so the next pass starts fresh.
func (s *Seeder) ResetCycle() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		e.cycleDone = false
		e.peerlessStart = time.Time{}
		if e.state == stateActive {
			e.cycleStart = time.Now()
			ts := e.t.Stats()
			e.cycleUploadStart = (&ts.BytesWrittenData).Int64()
		}
	}
	log.Printf("seeder: campaign cycle reset")
}

// Sync updates the queue to match items (typically sourced from decypharr).
func (s *Seeder) Sync(items []SeedItem) {
	newSet := make(map[string]SeedItem, len(items))
	for _, it := range items {
		h := strings.ToLower(it.Hash)
		if h != "" {
			newSet[h] = it
		}
	}

	s.mu.Lock()
	var toRemove []string
	for h := range s.entries {
		if _, ok := newSet[h]; !ok {
			toRemove = append(toRemove, h)
		}
	}
	for _, h := range toRemove {
		e := s.entries[h]
		e.t.Drop()
		delete(s.entries, h)
		for i, oh := range s.order {
			if oh == h {
				s.order = append(s.order[:i], s.order[i+1:]...)
				break
			}
		}
	}
	var toAdd []SeedItem
	for h, it := range newSet {
		if _, exists := s.entries[h]; !exists {
			toAdd = append(toAdd, it)
		}
	}
	s.mu.Unlock()

	for _, it := range toAdd {
		if err := s.addItem(it); err != nil {
			log.Printf("seeder: add %s [%s]: %v", it.Name, it.Hash[:8], err)
		}
	}

	if len(toRemove) > 0 || len(toAdd) > 0 {
		log.Printf("seeder: sync — added %d, removed %d", len(toAdd), len(toRemove))
	}
}

// AddMagnet manually adds a specific magnet link.
func (s *Seeder) AddMagnet(magnet string) (string, error) {
	t, err := s.client.AddMagnet(magnet)
	if err != nil {
		return "", fmt.Errorf("add magnet: %w", err)
	}
	t.AddTrackers(bestTrackers)
	t.DisallowDataUpload()
	hash := t.InfoHash().HexString()

	s.mu.Lock()
	if _, exists := s.entries[hash]; exists {
		s.mu.Unlock()
		t.Drop()
		return hash, nil
	}
	e := &entry{
		t:       t,
		magnet:  magnet,
		hash:    hash,
		addedAt: time.Now(),
		state:   stateVerifying,
	}
	s.entries[hash] = e
	s.order = append(s.order, hash)
	s.mu.Unlock()

	go s.waitAndActivate(e)
	return hash, nil
}

func (s *Seeder) addItem(it SeedItem) error {
	magnet := "magnet:?xt=urn:btih:" + it.Hash + "&dn=" + url.QueryEscape(it.Name)
	t, err := s.client.AddMagnet(magnet)
	if err != nil {
		return err
	}
	t.AddTrackers(bestTrackers)
	t.DisallowDataUpload()
	hash := strings.ToLower(t.InfoHash().HexString())

	s.mu.Lock()
	if _, exists := s.entries[hash]; exists {
		s.mu.Unlock()
		t.Drop()
		return nil
	}
	e := &entry{
		t:       t,
		magnet:  magnet,
		hash:    hash,
		name:    it.Name,
		year:    extractYear(it.Name),
		addedAt: time.Now(),
		state:   stateVerifying,
	}
	s.entries[hash] = e
	s.order = append(s.order, hash)
	s.mu.Unlock()

	go s.waitAndActivate(e)
	return nil
}

func (s *Seeder) waitAndActivate(e *entry) {
	<-e.t.GotInfo()
	name := e.t.Name()
	log.Printf("seeder: %s [%s] metadata ready", name, e.hash[:8])
	s.mu.Lock()
	if cur, ok := s.entries[e.hash]; ok {
		if cur.state == stateVerifying {
			cur.state = stateQueued
		}
		if cur.name == "" {
			cur.name = name
		}
		if cur.year == 0 {
			cur.year = extractYear(name)
		}
	}
	s.rebalanceLocked()
	s.mu.Unlock()
}

func (s *Seeder) Pause(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return false
	}
	if e.state == statePaused {
		return true
	}
	e.t.DisallowDataUpload()
	e.state = statePaused
	e.activeSince = time.Time{}
	s.rebalanceLocked()
	return true
}

func (s *Seeder) Resume(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return false
	}
	if e.state != statePaused {
		return true
	}
	e.state = stateQueued
	s.rebalanceLocked()
	return true
}

func (s *Seeder) Prioritize(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return false
	}
	for i, h := range s.order {
		if h == hash {
			s.order = append([]string{hash}, append(s.order[:i], s.order[i+1:]...)...)
			break
		}
	}
	if e.state == statePaused {
		e.state = stateQueued
	}
	s.rebalanceLocked()
	return true
}

func (s *Seeder) Remove(hash string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[hash]
	if !ok {
		return false
	}
	e.t.Drop()
	delete(s.entries, hash)
	for i, h := range s.order {
		if h == hash {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.rebalanceLocked()
	return true
}

// Info is a status snapshot for one seeded torrent.
type Info struct {
	Hash          string    `json:"hash"`
	Name          string    `json:"name"`
	Magnet        string    `json:"magnet"`
	State         string    `json:"state"`
	Year          int       `json:"year"`
	AddedAt       time.Time `json:"added_at"`
	ActiveSince   time.Time `json:"active_since"`
	SizeBytes     int64     `json:"size"`
	Uploaded      int64     `json:"uploaded"`
	UploadBPS     int64     `json:"upload_bps"`
	Peers         int       `json:"peers"`
	Seeds         int       `json:"seeds"`
	QueuePos      int       `json:"queue_pos"`
	CycleUploadMB float64   `json:"cycle_upload_mb"`
	CycleTimeSec  float64   `json:"cycle_time_sec"`
	CycleDone     bool      `json:"cycle_done"`
}

// List returns status for all tracked torrents.
func (s *Seeder) List() []Info {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos := make(map[string]int, len(s.order))
	for i, h := range s.order {
		pos[h] = i + 1
	}
	out := make([]Info, 0, len(s.entries))
	now := time.Now()
	for _, e := range s.entries {
		stats := e.t.Stats()
		info := e.t.Info()
		name := e.name
		var size int64
		if info != nil {
			if n := e.t.Name(); n != "" {
				name = n
			}
			size = info.TotalLength()
		}
		var cycleUpMB float64
		var cycleSec float64
		if !e.cycleStart.IsZero() && e.state == stateActive {
			cycleUpMB = float64(stats.BytesWrittenData.Int64()-e.cycleUploadStart) / (1024 * 1024)
			cycleSec = now.Sub(e.cycleStart).Seconds()
		}
		out = append(out, Info{
			Hash:          e.hash,
			Name:          name,
			Magnet:        e.magnet,
			State:         e.state.String(),
			Year:          e.year,
			AddedAt:       e.addedAt,
			ActiveSince:   e.activeSince,
			SizeBytes:     size,
			Uploaded:      stats.BytesWrittenData.Int64(),
			UploadBPS:     stats.BytesWrittenData.Int64(),
			Peers:         stats.ActivePeers,
			Seeds:         stats.ConnectedSeeders,
			QueuePos:      pos[e.hash],
			CycleUploadMB: cycleUpMB,
			CycleTimeSec:  cycleSec,
			CycleDone:     e.cycleDone,
		})
	}
	return out
}

// AggStats returns aggregate statistics.
type AggStats struct {
	Active     int     `json:"active"`
	Queued     int     `json:"queued"`
	Verifying  int     `json:"verifying"`
	Paused     int     `json:"paused"`
	TotalItems int     `json:"total"`
	UploadedGB float64 `json:"uploaded_gb"`
	UploadBPS  int64   `json:"upload_bps"`
	Peers      int     `json:"peers"`
	CycleDone  int     `json:"cycle_done"`
	CycleTotal int     `json:"cycle_total"`
}

func (s *Seeder) AggStats() AggStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st AggStats
	st.TotalItems = len(s.entries)
	now := time.Now()
	for _, e := range s.entries {
		switch e.state {
		case stateActive:
			st.Active++
		case stateQueued:
			st.Queued++
		case stateVerifying:
			st.Verifying++
		case statePaused:
			st.Paused++
		}
		if e.state != stateVerifying {
			st.CycleTotal++
			if e.cycleDone {
				st.CycleDone++
			}
		}
		ts := e.t.Stats()
		st.UploadedGB += float64(ts.BytesWrittenData.Int64()) / (1 << 30)
		st.Peers += ts.ActivePeers
		written := ts.BytesWrittenData.Int64()
		if prev, ok := s.prevUpload[e.hash]; ok {
			elapsed := now.Sub(s.prevUploadAt).Seconds()
			if elapsed > 0 {
				st.UploadBPS += int64(float64(written-prev) / elapsed)
			}
		}
		s.prevUpload[e.hash] = written
	}
	s.prevUploadAt = now
	return st
}

// Close shuts down the seeder.
func (s *Seeder) Close() error {
	errs := s.client.Close()
	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

func (s *Seeder) rebalanceLocked() {
	max := s.cfg.MaxActive

	type candidate struct {
		hash  string
		score int
	}
	var candidates []candidate
	for i, h := range s.order {
		e := s.entries[h]
		if e.state != stateQueued {
			continue
		}
		// In campaign mode, skip torrents marked done — let them sit queued until cycle resets.
		if s.cfg.campaignEnabled() && e.cycleDone {
			continue
		}
		score := i * 1000
		if s.cfg.FavorYearFrom > 0 || s.cfg.FavorYearTo > 0 {
			y := e.year
			from := s.cfg.FavorYearFrom
			to := s.cfg.FavorYearTo
			if (from == 0 || y >= from) && (to == 0 || y <= to) && y > 0 {
				score -= 100000
			}
		}
		if s.cfg.PrioritizeLowSeeders {
			peers := e.t.Stats().ActivePeers
			score -= (100 - peers) * 500
		}
		candidates = append(candidates, candidate{hash: h, score: score})
	}
	for i := 1; i < len(candidates); i++ {
		for j := i; j > 0 && candidates[j].score < candidates[j-1].score; j-- {
			candidates[j], candidates[j-1] = candidates[j-1], candidates[j]
		}
	}

	if max <= 0 {
		for _, c := range candidates {
			s.activateLocked(s.entries[c.hash])
		}
		return
	}

	active := 0
	for _, e := range s.entries {
		if e.state == stateActive {
			active++
		}
	}
	for _, c := range candidates {
		if active >= max {
			break
		}
		s.activateLocked(s.entries[c.hash])
		active++
	}
	for i := len(s.order) - 1; i >= 0 && active > max; i-- {
		if e := s.entries[s.order[i]]; e.state == stateActive {
			s.deactivateLocked(e)
			active--
		}
	}
}

func (s *Seeder) activateLocked(e *entry) {
	e.t.AllowDataUpload()
	e.state = stateActive
	e.activeSince = time.Now()
	// Initialise campaign cycle tracking for this activation.
	if !e.cycleDone {
		e.cycleStart = time.Now()
		ts0 := e.t.Stats()
		e.cycleUploadStart = (&ts0.BytesWrittenData).Int64()
		e.peerlessStart = time.Time{}
	}
	log.Printf("seeder: activated %s [%s]", e.name, e.hash[:8])
}

func (s *Seeder) deactivateLocked(e *entry) {
	e.t.DisallowDataUpload()
	e.state = stateQueued
	e.activeSince = time.Time{}
}

// rotationLoop fires every 15 s and handles both the time-based rotation
// (RotateAfter) and campaign-based rotation (min/max seed + peer-wait).
func (s *Seeder) rotationLoop() {
	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		cfg := s.cfg
		rotated := false

		for _, h := range append([]string(nil), s.order...) {
			e := s.entries[h]
			if e.state != stateActive {
				continue
			}

			ts := e.t.Stats()
			peers := ts.ActivePeers

			// ── Campaign rotation ──────────────────────────────────────
			if cfg.campaignEnabled() && !e.cycleDone {
				uploadedMB := float64(ts.BytesWrittenData.Int64()-e.cycleUploadStart) / (1024 * 1024)
				elapsed := now.Sub(e.cycleStart)

				// Track how long this torrent has had zero peers.
				if peers == 0 {
					if e.peerlessStart.IsZero() {
						e.peerlessStart = now
					}
				} else {
					e.peerlessStart = time.Time{}
				}

				hitMin := (cfg.CampaignMinSeedMB <= 0 || uploadedMB >= cfg.CampaignMinSeedMB) &&
					(cfg.CampaignMinTime <= 0 || elapsed >= cfg.CampaignMinTime)
				hitMaxSeed := cfg.CampaignMaxSeedMB > 0 && uploadedMB >= cfg.CampaignMaxSeedMB
				hitMaxTime := cfg.CampaignMaxTime > 0 && elapsed >= cfg.CampaignMaxTime
				noPeersTooLong := peers == 0 && cfg.CampaignPeerWait > 0 &&
					!e.peerlessStart.IsZero() && now.Sub(e.peerlessStart) >= cfg.CampaignPeerWait

				if noPeersTooLong && cfg.CampaignChunkDir != "" {
					// Launch chunk pull in a goroutine (blocking IO — must not hold mutex).
					name := e.name
					go s.chunkPull(name, cfg)
				}

				if hitMin || hitMaxSeed || hitMaxTime || noPeersTooLong {
					reason := "min targets met"
					switch {
					case hitMaxSeed:
						reason = "max seed cap"
					case hitMaxTime:
						reason = "max time cap"
					case noPeersTooLong:
						reason = "no peers → chunk-pull"
					}
					log.Printf("seeder: campaign done %s [%s] (%s, %.1fMB, %s, %d peers)",
						e.name, h[:8], reason, uploadedMB, elapsed.Round(time.Second), peers)
					e.cycleDone = true
					s.deactivateLocked(e)
					s.moveToBackLocked(h)
					rotated = true
					continue
				}
			}

			// ── Time-based rotation (RotateAfter) ─────────────────────
			if cfg.RotateAfter > 0 && !e.activeSince.IsZero() &&
				now.Sub(e.activeSince) >= cfg.RotateAfter {
				s.deactivateLocked(e)
				s.moveToBackLocked(h)
				rotated = true
			}
		}

		// Campaign: when every ready torrent is done, reset for the next cycle.
		if cfg.campaignEnabled() {
			total, done := 0, 0
			for _, e := range s.entries {
				if e.state != stateVerifying {
					total++
					if e.cycleDone {
						done++
					}
				}
			}
			if total > 0 && done == total && cfg.CampaignLoop {
				log.Printf("seeder: campaign cycle complete (%d/%d), looping", done, total)
				for _, e := range s.entries {
					e.cycleDone = false
					e.peerlessStart = time.Time{}
				}
				rotated = true
			} else if total > 0 && done == total {
				log.Printf("seeder: campaign cycle complete (%d/%d), loop disabled — cycle stopped", done, total)
			}
		}

		if rotated {
			s.rebalanceLocked()
		}
		s.mu.Unlock()
	}
}

func (s *Seeder) moveToBackLocked(hash string) {
	for i, h := range s.order {
		if h == hash {
			s.order = append(append(s.order[:i], s.order[i+1:]...), hash)
			return
		}
	}
}

// chunkPull reads up to CampaignChunkSizeMB bytes from the torrent's files on
// the Decypharr FUSE mount, writes them to a temp file, then deletes it.
// This causes RD to stream the data server-side, keeping the torrent warm in cache.
func (s *Seeder) chunkPull(torrentName string, cfg Config) {
	torrentDir := filepath.Join(cfg.MountDir, torrentName)
	if _, err := os.Stat(torrentDir); err != nil {
		log.Printf("seeder: chunk-pull %s: dir not found (%v)", torrentName, err)
		return
	}

	chunkDir := cfg.CampaignChunkDir
	if chunkDir == "" {
		return
	}
	if _, err := os.Stat(chunkDir); err != nil && cfg.CampaignChunkDirFallback != "" {
		log.Printf("seeder: chunk-pull primary dir %s unavailable, using %s", chunkDir, cfg.CampaignChunkDirFallback)
		chunkDir = cfg.CampaignChunkDirFallback
	}
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		log.Printf("seeder: chunk-pull mkdir %s: %v", chunkDir, err)
		return
	}

	remaining := int64(cfg.CampaignChunkSizeMB * 1024 * 1024)
	if remaining <= 0 {
		remaining = 100 * 1024 * 1024
	}
	var totalRead int64

	_ = filepath.WalkDir(torrentDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || remaining <= 0 {
			return nil
		}
		info, err2 := d.Info()
		if err2 != nil {
			return nil
		}
		toRead := remaining
		if info.Size() < toRead {
			toRead = info.Size()
		}
		f, err2 := os.Open(path)
		if err2 != nil {
			return nil
		}
		defer f.Close()

		tmp, err2 := os.CreateTemp(chunkDir, "rc-chunk-*")
		if err2 != nil {
			return fmt.Errorf("create temp: %w", err2)
		}
		tmpPath := tmp.Name()
		defer func() { tmp.Close(); os.Remove(tmpPath) }()

		n, _ := io.CopyN(tmp, f, toRead)
		remaining -= n
		totalRead += n
		return nil
	})

	log.Printf("seeder: chunk-pull %s complete — %.1f MB pulled from RD FUSE", torrentName, float64(totalRead)/(1024*1024))
}

var bestTrackers = [][]string{{
	"udp://tracker.opentrackr.org:1337/announce",
	"udp://open.stealth.si:80/announce",
	"udp://tracker.torrent.eu.org:451/announce",
	"udp://tracker.qu.ax:6969/announce",
	"udp://open.demonii.com:1337/announce",
	"udp://tracker.openbittorrent.com:6969/announce",
	"udp://tracker.dler.org:6969/announce",
	"udp://explodie.org:6969/announce",
	"udp://p4p.arenabg.com:1337/announce",
	"https://tracker.tamersunion.org:443/announce",
}}
