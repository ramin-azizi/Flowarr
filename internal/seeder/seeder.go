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
	Hash     string
	Name     string
	Provider string // "realdebrid" or "torbox"
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
	CampaignMinTime          time.Duration // fallback when no provider-specific target set
	CampaignMaxTime          time.Duration
	CampaignPeerWait         time.Duration
	CampaignChunkDir         string
	CampaignChunkDirFallback string
	CampaignChunkSizeMB      float64
	CampaignLoop             bool

	// Per-provider cycle targets. When set, the per-item slot time is computed
	// automatically as: targetDays*24*60*MaxActive / totalProviderItems
	// TorBox caches expire after 30 days — set CampaignTBCycleDays=30.
	// RD caches don't expire — set CampaignRDCycleDays=0 to disable auto-calc.
	CampaignTBCycleDays float64
	CampaignRDCycleDays float64
}

func (c Config) campaignEnabled() bool {
	return c.CampaignMinSeedMB > 0 || c.CampaignMinTime > 0 ||
		c.CampaignTBCycleDays > 0 || c.CampaignRDCycleDays > 0
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
	provider    string // "realdebrid" or "torbox"
	addedAt     time.Time
	activeSince time.Time
	state       seedState

	// Campaign cycle tracking — reset each cycle.
	cycleStart       time.Time // when this torrent was last activated in the current cycle
	cycleUploadStart int64     // BytesWrittenData at cycleStart
	cycleDone        bool      // has this torrent met its campaign target this cycle?
	peerlessStart    time.Time // when we first noticed 0 peers; zero = has peers / not tracked
	chunkReadMB      float64   // MB read from FUSE this cycle (primary cache-warming metric)
	chunkPulling     bool      // true while a chunkPull goroutine is running for this entry
	chunkNotFound    bool      // true once we've confirmed this item has no FUSE directory
	warmedAt         time.Time // last time this item was successfully cache-warmed
}

// clientCap is the maximum number of torrents held in the anacrolix client at
// once. Items beyond this stay in the pending queue and are loaded lazily as
// slots open up. This prevents flooding DHT/trackers when the library is large.
const clientCap = 100

// Seeder manages rotating torrent seeding from the RD mount.
type Seeder struct {
	cfg    Config
	client *torrent.Client
	lim    *rate.Limiter

	mu             sync.Mutex
	entries        map[string]*entry
	order          []string
	prevUpload     map[string]int64
	prevUploadAt   time.Time
	uploadBPSCache map[string]int64 // per-torrent BPS snapshot, refreshed by AggStats
	uploadOffset   map[string]int64 // per-torrent baseline for "uploaded since last clear"

	// pending holds items known to the seeder but not yet loaded into the
	// anacrolix client. Loaded lazily by refillFromPending.
	pendingMu           sync.Mutex
	pending             []SeedItem
	pendingSet          map[string]struct{} // hashes in pending (for O(1) dedup)
	pendingProviderCount map[string]int      // count of pending items per provider
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
		cfg:                  cfg,
		client:               client,
		lim:                  lim,
		entries:              make(map[string]*entry),
		prevUpload:           make(map[string]int64),
		prevUploadAt:         time.Now(),
		uploadBPSCache:       make(map[string]int64),
		uploadOffset:         make(map[string]int64),
		pendingSet:           make(map[string]struct{}),
		pendingProviderCount: make(map[string]int),
	}
	go s.rotationLoop()
	return s, nil
}

// refillFromPending moves items from the pending queue into the anacrolix
// client until the client holds clientCap torrents. Called after Sync and
// from rotationLoop so the pipeline stays topped up without adding everything
// at startup.
func (s *Seeder) refillFromPending() {
	s.mu.Lock()
	have := len(s.entries)
	s.mu.Unlock()

	cap := clientCap
	if s.cfg.MaxActive > 0 && s.cfg.MaxActive*10 > cap {
		cap = s.cfg.MaxActive * 10
	}

	s.pendingMu.Lock()
	toAdd := cap - have
	if toAdd <= 0 || len(s.pending) == 0 {
		s.pendingMu.Unlock()
		return
	}
	if toAdd > len(s.pending) {
		toAdd = len(s.pending)
	}
	batch := s.pending[:toAdd]
	s.pending = s.pending[toAdd:]
	for _, it := range batch {
		delete(s.pendingSet, strings.ToLower(it.Hash))
		if it.Provider != "" {
			s.pendingProviderCount[it.Provider]--
		}
	}
	s.pendingMu.Unlock()

	for _, it := range batch {
		if err := s.addItem(it); err != nil {
			log.Printf("seeder: add %s [%s]: %v", it.Name, it.Hash[:8], err)
		}
	}
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

// ResetUploadStats sets each torrent's upload baseline to its current total so
// "session uploaded" counters start from zero again.
func (s *Seeder) ResetUploadStats() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.entries {
		ts := e.t.Stats()
		s.uploadOffset[e.hash] = ts.BytesWrittenData.Int64()
	}
	log.Printf("seeder: upload stats reset")
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

	// Remove entries no longer in the library.
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
	inClient := make(map[string]struct{}, len(s.entries))
	for h := range s.entries {
		inClient[h] = struct{}{}
	}
	s.mu.Unlock()

	// Prune pending items that were removed from the library.
	s.pendingMu.Lock()
	filtered := s.pending[:0]
	for _, it := range s.pending {
		h := strings.ToLower(it.Hash)
		if _, ok := newSet[h]; ok {
			filtered = append(filtered, it)
		} else {
			delete(s.pendingSet, h)
			if it.Provider != "" {
				s.pendingProviderCount[it.Provider]--
			}
		}
	}
	s.pending = filtered

	// Queue new items into pending (not directly into anacrolix).
	added := 0
	for h, it := range newSet {
		if _, inC := inClient[h]; inC {
			continue
		}
		if _, inP := s.pendingSet[h]; inP {
			continue
		}
		s.pending = append(s.pending, it)
		s.pendingSet[h] = struct{}{}
		if it.Provider != "" {
			s.pendingProviderCount[it.Provider]++
		}
		added++
	}
	s.pendingMu.Unlock()

	if len(toRemove) > 0 || added > 0 {
		log.Printf("seeder: sync — queued %d new, removed %d (pending=%d)", added, len(toRemove), len(s.pending))
	}

	// Top up the anacrolix client from pending.
	s.refillFromPending()
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
		t:        t,
		magnet:   magnet,
		hash:     hash,
		name:     it.Name,
		year:     extractYear(it.Name),
		provider: it.Provider,
		addedAt:  time.Now(),
		state:    stateVerifying,
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
	Hash            string    `json:"hash"`
	Name            string    `json:"name"`
	Magnet          string    `json:"magnet"`
	State           string    `json:"state"`
	Provider        string    `json:"provider"`
	Year            int       `json:"year"`
	AddedAt         time.Time `json:"added_at"`
	ActiveSince     time.Time `json:"active_since"`
	WarmedAt        time.Time `json:"warmed_at"`
	SizeBytes       int64     `json:"size"`
	Uploaded        int64     `json:"uploaded"`
	SessionUploaded int64     `json:"session_uploaded"`
	UploadBPS       int64     `json:"upload_bps"`
	Peers           int       `json:"peers"`
	Seeds           int       `json:"seeds"`
	QueuePos        int       `json:"queue_pos"`
	CycleReadMB     float64   `json:"cycle_read_mb"`
	CycleTimeSec    float64   `json:"cycle_time_sec"`
	CycleDone       bool      `json:"cycle_done"`
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
		var cycleSec float64
		if !e.cycleStart.IsZero() && e.state == stateActive {
			cycleSec = now.Sub(e.cycleStart).Seconds()
		}
		totalUploaded := stats.BytesWrittenData.Int64()
		sessionUploaded := totalUploaded - s.uploadOffset[e.hash]
		if sessionUploaded < 0 {
			sessionUploaded = 0
		}
		out = append(out, Info{
			Hash:            e.hash,
			Name:            name,
			Magnet:          e.magnet,
			State:           e.state.String(),
			Provider:        e.provider,
			Year:            e.year,
			AddedAt:         e.addedAt,
			ActiveSince:     e.activeSince,
			WarmedAt:        e.warmedAt,
			SizeBytes:       size,
			Uploaded:        totalUploaded,
			SessionUploaded: sessionUploaded,
			UploadBPS:       s.uploadBPSCache[e.hash],
			Peers:           stats.ActivePeers,
			Seeds:           stats.ConnectedSeeders,
			QueuePos:        pos[e.hash],
			CycleReadMB:     e.chunkReadMB,
			CycleTimeSec:    cycleSec,
			CycleDone:       e.cycleDone,
		})
	}
	return out
}

// AggStats returns aggregate statistics.
type AggStats struct {
	Active            int     `json:"active"`
	Queued            int     `json:"queued"`
	Verifying         int     `json:"verifying"`
	Paused            int     `json:"paused"`
	Pending           int     `json:"pending"`
	TotalItems        int     `json:"total"`
	UploadedGB        float64 `json:"uploaded_gb"`
	SessionUploadedGB float64 `json:"session_uploaded_gb"`
	UploadBPS         int64   `json:"upload_bps"`
	Peers             int     `json:"peers"`
	CycleDone         int     `json:"cycle_done"`
	CycleTotal        int     `json:"cycle_total"`

	// Per-provider stats.
	RDTotal         int     `json:"rd_total"`
	RDWarmedCycle   int     `json:"rd_warmed_cycle"`   // warmed (chunkRead > 0) in current cycle
	TBTotal         int     `json:"tb_total"`
	TBWarmedCycle   int     `json:"tb_warmed_cycle"`
	TBMinTimeMin    float64 `json:"tb_min_time_min"`   // auto-calculated slot time for TorBox
	RDMinTimeMin    float64 `json:"rd_min_time_min"`   // auto-calculated slot time for RD
	TBCycleDaysEst  float64 `json:"tb_cycle_days_est"` // estimated days to finish TorBox cycle
	RDCycleDaysEst  float64 `json:"rd_cycle_days_est"`
}

func (s *Seeder) AggStats() AggStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	var st AggStats
	s.pendingMu.Lock()
	st.Pending = len(s.pending)
	// Provider totals: entries in anacrolix + pending.
	tbPending := s.pendingProviderCount["torbox"]
	rdPending := s.pendingProviderCount["realdebrid"]
	s.pendingMu.Unlock()
	st.TotalItems = len(s.entries) + st.Pending
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
		switch e.provider {
		case "torbox":
			st.TBTotal++
			if e.chunkReadMB > 0 || !e.warmedAt.IsZero() {
				st.TBWarmedCycle++
			}
		case "realdebrid":
			st.RDTotal++
			if e.chunkReadMB > 0 || !e.warmedAt.IsZero() {
				st.RDWarmedCycle++
			}
		}
		if e.state != stateVerifying {
			st.CycleTotal++
			if e.cycleDone {
				st.CycleDone++
			}
		}
		ts := e.t.Stats()
		written := ts.BytesWrittenData.Int64()
		st.UploadedGB += float64(written) / (1 << 30)
		sessionBytes := written - s.uploadOffset[e.hash]
		if sessionBytes < 0 {
			sessionBytes = 0
		}
		st.SessionUploadedGB += float64(sessionBytes) / (1 << 30)
		st.Peers += ts.ActivePeers
		var torrentBPS int64
		if prev, ok := s.prevUpload[e.hash]; ok {
			elapsed := now.Sub(s.prevUploadAt).Seconds()
			if elapsed > 0 {
				torrentBPS = int64(float64(written-prev) / elapsed)
				st.UploadBPS += torrentBPS
			}
		}
		s.uploadBPSCache[e.hash] = torrentBPS
		s.prevUpload[e.hash] = written
	}
	s.prevUploadAt = now

	// Add pending counts to provider totals.
	st.TBTotal += tbPending
	st.RDTotal += rdPending

	// Auto-calculated per-provider slot times and cycle estimates.
	maxActive := s.cfg.MaxActive
	if maxActive <= 0 {
		maxActive = 1
	}
	if s.cfg.CampaignTBCycleDays > 0 && st.TBTotal > 0 {
		st.TBMinTimeMin = s.cfg.CampaignTBCycleDays * 24 * 60 * float64(maxActive) / float64(st.TBTotal)
		// Estimate days to complete current cycle at current pace.
		remaining := st.TBTotal - st.TBWarmedCycle
		st.TBCycleDaysEst = float64(remaining) / float64(maxActive) * st.TBMinTimeMin / 60 / 24
	}
	if s.cfg.CampaignRDCycleDays > 0 && st.RDTotal > 0 {
		st.RDMinTimeMin = s.cfg.CampaignRDCycleDays * 24 * 60 * float64(maxActive) / float64(st.RDTotal)
		remaining := st.RDTotal - st.RDWarmedCycle
		st.RDCycleDaysEst = float64(remaining) / float64(maxActive) * st.RDMinTimeMin / 60 / 24
	}

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
		e.chunkReadMB = 0
		e.chunkPulling = false
		e.chunkNotFound = false
	}
	log.Printf("seeder: activated %s [%s]", e.name, e.hash[:8])
}

func (s *Seeder) deactivateLocked(e *entry) {
	e.t.DisallowDataUpload()
	e.state = stateQueued
	e.activeSince = time.Time{}
}

// evictAndRequeueLocked drops the torrent from anacrolix, removes it from
// s.entries/s.order, and (if loop is true) pushes a fresh SeedItem to the
// back of s.pending so it cycles through the full library rather than
// the same clientCap items forever. Must be called with s.mu held.
func (s *Seeder) evictAndRequeueLocked(e *entry, loop bool) {
	hash := e.hash
	name := e.name
	prov := e.provider
	e.t.Drop()
	delete(s.entries, hash)
	for i, h := range s.order {
		if h == hash {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	delete(s.uploadOffset, hash)
	delete(s.prevUpload, hash)
	delete(s.uploadBPSCache, hash)

	if loop {
		it := SeedItem{Hash: hash, Name: name, Provider: prov}
		s.pendingMu.Lock()
		s.pending = append(s.pending, it)
		s.pendingSet[hash] = struct{}{}
		if prov != "" {
			s.pendingProviderCount[prov]++
		}
		s.pendingMu.Unlock()
	}
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
				elapsed := now.Sub(e.cycleStart)

				// Proactive cache warming: start reading from Decypharr FUSE → /dev/shm
				// immediately when a torrent is active, regardless of peer count.
				// This keeps the torrent alive in RD/TorBox's CDN cache even if the
				// P2P swarm is completely dead.
				if cfg.CampaignChunkDir != "" && !e.chunkPulling && !e.chunkNotFound {
					name := e.name
					hash := h
					e.chunkPulling = true
					go func() {
						mb := s.chunkPull(name, cfg)
						s.mu.Lock()
						if en, ok := s.entries[hash]; ok {
							en.chunkPulling = false
							if mb > 0 {
								en.chunkReadMB += mb
								en.warmedAt = time.Now()
							} else {
								en.chunkNotFound = true
							}
						}
						s.mu.Unlock()
					}()
				}

				// Compute effective minimum slot time for this entry's provider.
				// If a provider-specific cycle target is set, auto-calculate from:
				// targetDays * 24 * 60 * maxActive / totalProviderItems
				effectiveMinTime := cfg.CampaignMinTime
				if cycDays := map[string]float64{
					"torbox":     cfg.CampaignTBCycleDays,
					"realdebrid": cfg.CampaignRDCycleDays,
				}[e.provider]; cycDays > 0 {
					s.pendingMu.Lock()
					provTotal := s.pendingProviderCount[e.provider]
					s.pendingMu.Unlock()
					for _, en := range s.entries {
						if en.provider == e.provider {
							provTotal++
						}
					}
					if provTotal > 0 {
						maxA := cfg.MaxActive
						if maxA <= 0 {
							maxA = 1
						}
						minMin := cycDays * 24 * 60 * float64(maxA) / float64(provTotal)
						effectiveMinTime = time.Duration(minMin * float64(time.Minute))
					}
				}

				// Progress is measured by MB read from FUSE (not BitTorrent uploads),
				// so this works even when there are zero peers.
				readMB := e.chunkReadMB

				hitMin := (cfg.CampaignMinSeedMB <= 0 || readMB >= cfg.CampaignMinSeedMB) &&
					(effectiveMinTime <= 0 || elapsed >= effectiveMinTime)
				hitMaxSeed := cfg.CampaignMaxSeedMB > 0 && readMB >= cfg.CampaignMaxSeedMB
				hitMaxTime := cfg.CampaignMaxTime > 0 && elapsed >= cfg.CampaignMaxTime

				if hitMin || hitMaxSeed || hitMaxTime {
					reason := "min targets met"
					if hitMaxSeed {
						reason = "max read cap"
					} else if hitMaxTime {
						reason = "max time cap"
					}
					log.Printf("seeder: campaign done %s [%s] (%s, %.1f MB read, %s, %d peers)",
						e.name, h[:8], reason, readMB, elapsed.Round(time.Second), peers)
					// Evict from anacrolix entirely and requeue at the back of pending.
					// This frees the slot so the next pending item can be loaded, allowing
					// all 15k+ library items to cycle through rather than the same 100.
					s.evictAndRequeueLocked(e, cfg.CampaignLoop)
					rotated = true
					continue
				}
			}

			// ── Time-based rotation (RotateAfter) ─────────────────────
			if cfg.RotateAfter > 0 && !e.activeSince.IsZero() &&
				now.Sub(e.activeSince) >= cfg.RotateAfter {
				if cfg.campaignEnabled() {
					// In campaign mode, also evict+requeue for time-based rotation.
					s.evictAndRequeueLocked(e, true)
				} else {
					s.deactivateLocked(e)
					s.moveToBackLocked(h)
				}
				rotated = true
			}
		}

		if rotated {
			s.rebalanceLocked()
		}
		s.mu.Unlock()

		// Top up anacrolix from pending whenever the rotation loop runs.
		s.refillFromPending()
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

// resolveFUSEPath finds the actual directory for a torrent name in the FUSE mount.
// RD's API returns `filename` which for single-file torrents is the .mkv name,
// but Decypharr's __all__ directory uses the torrent name (no extension). We try
// the exact name first, then strip common video extensions as a fallback.
// Returns "" if no matching directory is found (logs once).
// sitePrefixRe matches torrent names prefixed by site domains (e.g. "www.UIndex.org    -    ").
var sitePrefixRe = regexp.MustCompile(`(?i)^www\.[^.]+\.[a-z]{2,6}[\s\-]+`)

// mediaExts are the only extensions we strip when normalizing.
// Torrent dir names have dots in them (e.g. "Movie.5.1.BONE") that must not be
// stripped; only real media extensions from the debrid API name get removed.
var mediaExts = map[string]bool{
	".mkv": true, ".mp4": true, ".avi": true, ".mov": true,
	".m4v": true, ".wmv": true, ".ts":  true, ".m2ts": true,
	".rar": true, ".zip": true, ".7z":  true,
}

// normalizeFUSEName strips site prefix and known media extension to get a comparable base name.
func normalizeFUSEName(name string) string {
	n := sitePrefixRe.ReplaceAllString(name, "")
	n = strings.TrimSpace(n)
	if ext := filepath.Ext(n); mediaExts[strings.ToLower(ext)] {
		n = n[:len(n)-len(ext)]
	}
	return strings.ToLower(n)
}

func resolveFUSEPath(mountDir, torrentName string) string {
	// Fast path: exact match or extension-stripped match.
	candidates := []string{torrentName}
	if ext := filepath.Ext(torrentName); ext != "" {
		candidates = append(candidates, torrentName[:len(torrentName)-len(ext)])
	}
	for _, name := range candidates {
		p := filepath.Join(mountDir, name)
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}

	// Slow path: scan mountDir for a dir whose normalized name matches.
	// Handles torrents where the actual dir has a site prefix (e.g. "www.UIndex.org - Foo")
	// while the debrid API returns just "Foo.mkv".
	want := normalizeFUSEName(torrentName)
	entries, err := os.ReadDir(mountDir)
	if err == nil {
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if normalizeFUSEName(e.Name()) == want {
				return filepath.Join(mountDir, e.Name())
			}
		}
	}

	log.Printf("seeder: chunk-pull %s: not found in FUSE mount", torrentName)
	return ""
}

// chunkPull reads up to CampaignChunkSizeMB bytes from the torrent's files via
// the Decypharr FUSE mount. Data is buffered through a RAM-only temp directory
// (/dev/shm or a configured fallback) and immediately discarded. Reading the
// data forces RD/TorBox to keep the torrent's CDN cache alive even when no
// peers are downloading it over BitTorrent. Returns MB actually read.
func (s *Seeder) chunkPull(torrentName string, cfg Config) float64 {
	torrentDir := resolveFUSEPath(cfg.MountDir, torrentName)
	if torrentDir == "" {
		return 0
	}

	// Resolve write directory: must be RAM-backed (/dev/shm) or explicit fallback.
	// Default to /dev/shm when nothing is configured.
	chunkDir := cfg.CampaignChunkDir
	if chunkDir == "" {
		chunkDir = "/dev/shm"
	}
	if _, err := os.Stat(chunkDir); err != nil {
		if cfg.CampaignChunkDirFallback != "" {
			log.Printf("seeder: chunk-pull primary dir %s unavailable, using %s", chunkDir, cfg.CampaignChunkDirFallback)
			chunkDir = cfg.CampaignChunkDirFallback
		} else {
			log.Printf("seeder: chunk-pull %s: write dir %s unavailable and no fallback set", torrentName, chunkDir)
			return 0
		}
	}
	if err := os.MkdirAll(chunkDir, 0755); err != nil {
		log.Printf("seeder: chunk-pull mkdir %s: %v", chunkDir, err)
		return 0
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

		// Write to a temp file then delete immediately — the only purpose is to
		// trigger the FUSE read so RD/TorBox streams the data server-side.
		tmp, err2 := os.CreateTemp(chunkDir, "flowarr-rc-*")
		if err2 != nil {
			// Fall back to discarding into /dev/null if temp creation fails.
			n, _ := io.CopyN(io.Discard, f, toRead)
			remaining -= n
			totalRead += n
			return nil
		}
		tmpPath := tmp.Name()
		defer func() { tmp.Close(); os.Remove(tmpPath) }()

		n, _ := io.CopyN(tmp, f, toRead)
		remaining -= n
		totalRead += n
		return nil
	})

	mb := float64(totalRead) / (1024 * 1024)
	if totalRead > 0 {
		log.Printf("seeder: chunk-pull %s — %.1f MB read from FUSE → cache warmed", torrentName, mb)
	}
	return mb
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
