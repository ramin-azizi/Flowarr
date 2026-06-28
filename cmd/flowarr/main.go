package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	aktorrent "github.com/anacrolix/torrent"
	"github.com/ramin-azizi/flowarr/internal/config"
	"gopkg.in/yaml.v3"
	"github.com/ramin-azizi/flowarr/internal/decypharr"
	"github.com/ramin-azizi/flowarr/internal/fallback"
	"github.com/ramin-azizi/flowarr/internal/handoff"
	"github.com/ramin-azizi/flowarr/internal/realdebrid"
	"github.com/ramin-azizi/flowarr/internal/seeder"
	"github.com/ramin-azizi/flowarr/internal/torbox"
	"github.com/ramin-azizi/flowarr/internal/torrent"
	"github.com/ramin-azizi/flowarr/internal/vfs"
)

const version = "0.1.0"

var (
	mgr         *torrent.Manager
	mountPath   string // absolute path to FUSE mount
	categorysMu sync.RWMutex
	categories  = map[string]map[string]string{} // name → {name, savePath}

	activeCfg        *config.Config
	cfgFilePath      string             // path to flowarr.yaml, set in main()
	fuseOK           bool
	dcClient         *decypharr.Client  // nil when Decypharr is not configured
	fallbackSearcher *fallback.Searcher // nil when Prowlarr is not configured
	seedMgr          *seeder.Seeder     // nil when seeder is disabled

	logBufMu sync.RWMutex
	logLines []string
)

const logBufMax = 1000

// logTee duplicates log output to os.Stderr and to the in-memory ring buffer.
type logTee struct{ w io.Writer }

func (lt logTee) Write(p []byte) (int, error) {
	n, err := lt.w.Write(p)
	line := strings.TrimRight(string(p), "\n")
	logBufMu.Lock()
	logLines = append(logLines, line)
	if len(logLines) > logBufMax {
		logLines = logLines[len(logLines)-logBufMax:]
	}
	logBufMu.Unlock()
	return n, err
}

func authLogin(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "Ok.")
}

func appVersion(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "v4.6.0")
}

func apiVersion(w http.ResponseWriter, r *http.Request) {
	fmt.Fprint(w, "2.9.3")
}

func appPreferences(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"save_path":                 activeCfg.Library.BaseDir,
		"temp_path_enabled":         false,
		"scan_dirs":                 map[string]interface{}{},
		"queueing_enabled":          true,
		"max_active_downloads":      5,
		"max_active_torrents":       10,
		"max_active_uploads":        5,
		"dht":                       true,
		"pex":                       true,
		"lsd":                       true,
		"encryption":                0,
		"anonymous_mode":            false,
		"proxy_type":                0,
		"use_https":                 false,
		"web_ui_port":               8888,
	})
}

func torrentsCategories(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	categorysMu.RLock()
	defer categorysMu.RUnlock()
	json.NewEncoder(w).Encode(categories)
}

func torrentsCreateCategory(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("category")
	savePath := r.FormValue("savePath")
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	categorysMu.Lock()
	categories[name] = map[string]string{"name": name, "savePath": savePath}
	categorysMu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func torrentsEditCategory(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	name := r.FormValue("category")
	savePath := r.FormValue("savePath")
	if name == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	categorysMu.Lock()
	categories[name] = map[string]string{"name": name, "savePath": savePath}
	categorysMu.Unlock()
	w.WriteHeader(http.StatusOK)
}

func torrentsRemoveCategories(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	for _, name := range strings.Split(r.FormValue("categories"), "\n") {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		categorysMu.Lock()
		delete(categories, name)
		categorysMu.Unlock()
	}
	w.WriteHeader(http.StatusOK)
}

func torrentsInfo(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	list := mgr.List()
	out := make([]map[string]interface{}, 0, len(list))
	for _, t := range list {
		hash := t.InfoHash().HexString()
		entry := map[string]interface{}{
			"hash":      hash,
			"state":     "downloading",
			"save_path": mgr.SavePath(hash),
			"category":  mgr.Category(hash),
		}
		select {
		case <-t.GotInfo():
			// Report complete as soon as metadata is ready. Files are served
			// on-demand through the FUSE — nothing is pre-downloaded — so Radarr/
			// Sonarr can import the symlink immediately and Plex streams lazily.
			total := t.Length()
			entry["name"] = t.Name()
			entry["size"] = total
			entry["progress"] = 1.0
			entry["state"] = "uploading"
			entry["num_seeds"] = t.Stats().ConnectedSeeders
			entry["num_leechs"] = t.Stats().ActivePeers
			sp := mgr.SavePath(hash)
			if sp != "" {
				entry["content_path"] = filepath.Join(sp, t.Name())
			} else {
				entry["content_path"] = filepath.Join(mountPath, t.Name())
			}
		default:
			entry["name"] = ""
			entry["size"] = 0
			entry["progress"] = 0.0
		}
		out = append(out, entry)
	}
	json.NewEncoder(w).Encode(out)
}

// POST /api/v2/torrents/add
// Fields: urls (magnet), savepath, category (ignored for now)
func torrentsAdd(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !activeCfg.Download.Enabled {
		// Silently acknowledge so arr apps don't retry, but don't download.
		fmt.Fprint(w, "Ok.")
		return
	}
	if err := r.ParseMultipartForm(1 << 20); err != nil {
		r.ParseForm()
	}
	urls := r.FormValue("urls")
	if urls == "" {
		http.Error(w, "Fails.", http.StatusBadRequest)
		return
	}
	rawSavePath := r.FormValue("savepath")
	category := r.FormValue("category")
	// Apply any category → folder mapping before storing the save path.
	savePath := activeCfg.ResolvedSavePath(rawSavePath, category)

	t, err := mgr.AddMagnet(urls, savePath, category)
	if err != nil {
		log.Printf("AddMagnet error: %v", err)
		fmt.Fprint(w, "Fails.")
		return
	}

	// Forward to Decypharr so RD caches the torrent in parallel.
	// Non-fatal: Flowarr still streams locally even if this fails.
	if dcClient != nil {
		go func() {
			if err := dcClient.Forward(context.Background(), urls, category, savePath); err != nil {
				log.Printf("Decypharr forward: %v", err)
			} else {
				log.Printf("Decypharr forward: queued %s", t.InfoHash().HexString())
			}
		}()
	}

	// Launch seed-health check. If seeds are low after 45 s, Prowlarr is queried
	// for a better alternative and symlinks are repointed transparently.
	if savePath != "" && fallbackSearcher != nil {
		go checkAndFallback(t, savePath, category)
	}

	if savePath != "" {
		h := t.InfoHash().HexString()
		go func() {
			<-t.GotInfo()
			name := t.Name()
			for _, f := range t.Files() {
				rel := f.DisplayPath()
				target := filepath.Join(mountPath, name, rel)
				link := symlinkLinkPath(savePath, name, rel)
				if err := os.MkdirAll(filepath.Dir(link), 0755); err != nil {
					log.Printf("mkdir %s: %v", filepath.Dir(link), err)
					continue
				}
				os.Remove(link)
				if err := os.Symlink(target, link); err != nil {
					log.Printf("symlink %s → %s: %v", link, target, err)
				} else {
					log.Printf("symlink %s → %s", link, target)
				}
			}
			scheduleArrScanAndRemove(savePath, h)
		}()
	}

	fmt.Fprint(w, "Ok.")
}

// POST /api/v2/torrents/delete
// Fields: hashes (pipe-separated), deleteFiles
func torrentsDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	r.ParseForm()
	hashes := strings.Split(r.FormValue("hashes"), "|")

	for _, hash := range hashes {
		hash = strings.TrimSpace(strings.ToLower(hash))
		if hash == "" {
			continue
		}
		t, ok := mgr.Get(hash)
		if !ok {
			continue
		}
		if t.Info() != nil {
			savePath := mgr.SavePath(hash)
			if savePath != "" {
				for _, f := range t.Files() {
					link := symlinkLinkPath(savePath, t.Name(), f.DisplayPath())
					if err := os.Remove(link); err != nil && !os.IsNotExist(err) {
						log.Printf("remove symlink %s: %v", link, err)
					}
				}
			}
		}
		mgr.Remove(hash)
		log.Printf("removed torrent %s", hash)
	}
	w.WriteHeader(http.StatusOK)
}

// scheduleArrScanAndRemove triggers a RescanFolders command on every configured
// arr instance 10 seconds after symlinking, then removes the torrent from
// Flowarr's queue 90 seconds after that.
//
// Why: Radarr's download-client import pipeline tries to read file content for
// sample detection, which fails on FUSE-backed symlinks that haven't warmed up
// yet. RescanFolders uses a stat-only disk scan that succeeds on FUSE. Once
// the scan links the file in Radarr's database, Radarr sees the torrent
// disappear from the download client, notices the movie already has files, and
// marks the item as imported — keeping the queue clean without errors.
func scheduleArrScanAndRemove(savePath, hash string) {
	go func() {
		time.Sleep(10 * time.Second)
		for _, arr := range activeCfg.Arrs {
			body := fmt.Sprintf(`{"name":"RescanFolders","folders":[%q]}`, savePath)
			req, err := http.NewRequest(http.MethodPost, arr.URL+"/api/v3/command", strings.NewReader(body))
			if err != nil {
				continue
			}
			req.Header.Set("X-Api-Key", arr.APIKey)
			req.Header.Set("Content-Type", "application/json")
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("arr rescan %s: %v", arr.URL, err)
				continue
			}
			resp.Body.Close()
			log.Printf("arr rescan %s: HTTP %d", arr.URL, resp.StatusCode)
		}
		time.Sleep(90 * time.Second)
		if mgr.Remove(hash) {
			log.Printf("auto-removed %s after arr rescan", hash)
		}
	}()
}

// symlinkLinkPath returns where the symlink for a torrent file should live.
// Multi-file torrents have a root folder (torrentName != displayPath), so the
// link mirrors qBittorrent's layout: {savePath}/{torrentName}/{displayPath}.
// Single-file torrents have no root folder; qBittorrent places them directly
// in savePath, so we do the same: {savePath}/{displayPath}.
func symlinkLinkPath(savePath, torrentName, displayPath string) string {
	if torrentName == displayPath {
		return filepath.Join(savePath, displayPath)
	}
	return filepath.Join(savePath, torrentName, displayPath)
}

// atomicSwapLink replaces link with a symlink pointing to newTarget using a
// temp-file rename so the path is never absent from the filesystem.
func atomicSwapLink(link, newTarget string) error {
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

// checkAndFallback waits 45 s after metadata is ready, then checks whether the
// torrent has enough seeds for reliable streaming. If not, it searches Prowlarr
// for a healthier alternative, adds it without a save-path (no auto-symlinks),
// then atomically repoints the existing library symlinks to the fallback FUSE
// paths so Plex can stream immediately without re-importing.
//
// Quality scoring: the search prefers results at or above the category's
// minimum resolution (e.g. 1080p for movies-hd, 4K for movies-4k). Below-
// minimum results are penalised by 65–95 % so a heavily-seeded lower-quality
// torrent must be dramatically healthier to win over a modestly-seeded match.
func checkAndFallback(t *aktorrent.Torrent, savePath, category string) {
	// Wait for metadata (up to 2 min).
	select {
	case <-t.GotInfo():
	case <-time.After(2 * time.Minute):
		log.Printf("fallback: timeout waiting for info on %s", t.InfoHash().HexString())
		return
	}

	delay := activeCfg.Streaming.HealthCheckDelay
	if delay <= 0 {
		delay = 45 * time.Second
	}
	time.Sleep(delay)

	hash := t.InfoHash().HexString()
	if _, ok := mgr.Get(hash); !ok {
		return // already handed off or removed
	}

	threshold := activeCfg.Streaming.SeedThreshold
	if threshold <= 0 {
		threshold = 5
	}
	seeds := t.Stats().ConnectedSeeders
	if seeds >= threshold {
		log.Printf("fallback: %s has %d seeds — healthy, skipping", t.Name(), seeds)
		return
	}

	if fallbackSearcher == nil {
		log.Printf("fallback: %s has only %d seeds but Prowlarr is not configured", t.Name(), seeds)
		return
	}

	log.Printf("fallback: %s has only %d seeds (threshold=%d) — searching Prowlarr", t.Name(), seeds, threshold)

	catType := fallback.ClassifyCategory(category)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	result, err := fallbackSearcher.FindFallback(ctx, t.Name(), catType)
	if err != nil {
		log.Printf("fallback: Prowlarr search error: %v", err)
		return
	}
	if result == nil {
		log.Printf("fallback: no suitable alternative found for %s", t.Name())
		return
	}

	log.Printf("fallback: selected %q (seeds=%d, res=%s, score=%.1f) for %q",
		result.Title, result.Seeds, result.Resolution, result.Score, t.Name())

	// Add fallback with empty savePath — no automatic symlinks.
	ft, err := mgr.AddMagnet(result.MagnetURL, "", category)
	if err != nil {
		log.Printf("fallback: add magnet error: %v", err)
		return
	}

	fbHash := ft.InfoHash().HexString()
	mgr.SetFallback(hash, fbHash)

	// Wait for fallback metadata before repointing.
	select {
	case <-ft.GotInfo():
	case <-time.After(2 * time.Minute):
		log.Printf("fallback: timeout waiting for fallback info — abandoning")
		mgr.Remove(fbHash)
		mgr.ClearFallback(hash)
		return
	}

	origFiles := t.Files()
	fbFiles := ft.Files()

	if len(origFiles) != len(fbFiles) {
		// Mismatched file count (e.g. extras vs no extras). Keep the original
		// symlinks — at least some peers are available.
		log.Printf("fallback: file count mismatch (orig=%d, fb=%d) for %q — keeping original symlinks",
			len(origFiles), len(fbFiles), t.Name())
		return
	}

	origName := t.Name()
	fbName := ft.Name()
	for i, of := range origFiles {
		fbf := fbFiles[i]
		link := symlinkLinkPath(savePath, origName, of.DisplayPath())
		newTarget := filepath.Join(mountPath, fbName, fbf.DisplayPath())
		if err := atomicSwapLink(link, newTarget); err != nil {
			log.Printf("fallback: swap %s → %s: %v", link, newTarget, err)
		} else {
			log.Printf("fallback: repointed %s → %s", link, newTarget)
		}
	}
	log.Printf("fallback: symlinks for %q now served from fallback %q", origName, fbName)
}

// GET /api/flowarr/seeder — aggregate status + full torrent list
func apiSeederList(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	enabled := activeCfg.Seeder.Enabled
	var agg seeder.AggStats
	var list []seeder.Info
	if seedMgr != nil {
		agg = seedMgr.AggStats()
		list = seedMgr.List()
	}
	if list == nil {
		list = []seeder.Info{}
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"enabled":           enabled,
		"downloads_enabled": activeCfg.Download.Enabled,
		"stats":             agg,
		"settings": map[string]interface{}{
			"upload_limit_mbps":            activeCfg.Seeder.UploadLimitMBPS,
			"max_active":                   activeCfg.Seeder.MaxActive,
			"rotate_after_hours":           activeCfg.Seeder.RotateAfterHours,
			"favor_year_from":              activeCfg.Seeder.FavorYearFrom,
			"favor_year_to":                activeCfg.Seeder.FavorYearTo,
			"prioritize_low_seeders":       activeCfg.Seeder.PrioritizeLowSeeders,
			"campaign_min_seed_mb":         activeCfg.Seeder.CampaignMinSeedMB,
			"campaign_max_seed_mb":         activeCfg.Seeder.CampaignMaxSeedMB,
			"campaign_min_time_minutes":    activeCfg.Seeder.CampaignMinTimeMinutes,
			"campaign_max_time_minutes":    activeCfg.Seeder.CampaignMaxTimeMinutes,
			"campaign_peer_wait_seconds":   activeCfg.Seeder.CampaignPeerWaitSeconds,
			"campaign_chunk_dir":           activeCfg.Seeder.CampaignChunkDir,
			"campaign_chunk_dir_fallback":  activeCfg.Seeder.CampaignChunkDirFallback,
			"campaign_chunk_size_mb":       activeCfg.Seeder.CampaignChunkSizeMB,
			"campaign_loop":                activeCfg.Seeder.CampaignLoop,
			"campaign_tb_cycle_days":       activeCfg.Seeder.CampaignTBCycleDays,
			"campaign_rd_cycle_days":       activeCfg.Seeder.CampaignRDCycleDays,
			"seed_from_providers":          activeCfg.Seeder.SeedFromProviders,
		},
		"torrents": list,
	})
}

// POST /api/flowarr/seeder/settings — update runtime settings
func apiSeederSettings(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UploadLimitMBPS          *int     `json:"upload_limit_mbps"`
		MaxActive                *int     `json:"max_active"`
		RotateAfterHours         *float64 `json:"rotate_after_hours"`
		DownloadsEnabled         *bool    `json:"downloads_enabled"`
		FavorYearFrom            *int     `json:"favor_year_from"`
		FavorYearTo              *int     `json:"favor_year_to"`
		PrioritizeLowSeeders     *bool    `json:"prioritize_low_seeders"`
		CampaignMinSeedMB        *float64 `json:"campaign_min_seed_mb"`
		CampaignMaxSeedMB        *float64 `json:"campaign_max_seed_mb"`
		CampaignMinTimeMinutes   *float64 `json:"campaign_min_time_minutes"`
		CampaignMaxTimeMinutes   *float64 `json:"campaign_max_time_minutes"`
		CampaignPeerWaitSeconds  *int     `json:"campaign_peer_wait_seconds"`
		CampaignChunkDir         *string  `json:"campaign_chunk_dir"`
		CampaignChunkDirFallback *string  `json:"campaign_chunk_dir_fallback"`
		CampaignChunkSizeMB      *float64 `json:"campaign_chunk_size_mb"`
		CampaignLoop             *bool    `json:"campaign_loop"`
		CampaignTBCycleDays      *float64 `json:"campaign_tb_cycle_days"`
		CampaignRDCycleDays      *float64 `json:"campaign_rd_cycle_days"`
		SeedFromProviders        []string `json:"seed_from_providers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if req.UploadLimitMBPS != nil {
		activeCfg.Seeder.UploadLimitMBPS = *req.UploadLimitMBPS
	}
	if req.MaxActive != nil {
		activeCfg.Seeder.MaxActive = *req.MaxActive
	}
	if req.RotateAfterHours != nil {
		activeCfg.Seeder.RotateAfterHours = *req.RotateAfterHours
	}
	if req.DownloadsEnabled != nil {
		activeCfg.Download.Enabled = *req.DownloadsEnabled
	}
	if req.FavorYearFrom != nil {
		activeCfg.Seeder.FavorYearFrom = *req.FavorYearFrom
	}
	if req.FavorYearTo != nil {
		activeCfg.Seeder.FavorYearTo = *req.FavorYearTo
	}
	if req.PrioritizeLowSeeders != nil {
		activeCfg.Seeder.PrioritizeLowSeeders = *req.PrioritizeLowSeeders
	}
	if req.CampaignMinSeedMB != nil {
		activeCfg.Seeder.CampaignMinSeedMB = *req.CampaignMinSeedMB
	}
	if req.CampaignMaxSeedMB != nil {
		activeCfg.Seeder.CampaignMaxSeedMB = *req.CampaignMaxSeedMB
	}
	if req.CampaignMinTimeMinutes != nil {
		activeCfg.Seeder.CampaignMinTimeMinutes = *req.CampaignMinTimeMinutes
	}
	if req.CampaignMaxTimeMinutes != nil {
		activeCfg.Seeder.CampaignMaxTimeMinutes = *req.CampaignMaxTimeMinutes
	}
	if req.CampaignPeerWaitSeconds != nil {
		activeCfg.Seeder.CampaignPeerWaitSeconds = *req.CampaignPeerWaitSeconds
	}
	if req.CampaignChunkDir != nil {
		activeCfg.Seeder.CampaignChunkDir = *req.CampaignChunkDir
	}
	if req.CampaignChunkDirFallback != nil {
		activeCfg.Seeder.CampaignChunkDirFallback = *req.CampaignChunkDirFallback
	}
	if req.CampaignChunkSizeMB != nil {
		activeCfg.Seeder.CampaignChunkSizeMB = *req.CampaignChunkSizeMB
	}
	if req.CampaignLoop != nil {
		activeCfg.Seeder.CampaignLoop = *req.CampaignLoop
	}
	if req.CampaignTBCycleDays != nil {
		activeCfg.Seeder.CampaignTBCycleDays = *req.CampaignTBCycleDays
	}
	if req.CampaignRDCycleDays != nil {
		activeCfg.Seeder.CampaignRDCycleDays = *req.CampaignRDCycleDays
	}
	if req.SeedFromProviders != nil {
		activeCfg.Seeder.SeedFromProviders = req.SeedFromProviders
	}
	if seedMgr != nil {
		seedMgr.ApplyConfig(seeder.Config{
			UploadLimitMBPS:          activeCfg.Seeder.UploadLimitMBPS,
			MaxActive:                activeCfg.Seeder.MaxActive,
			RotateAfter:              time.Duration(float64(time.Hour) * activeCfg.Seeder.RotateAfterHours),
			FavorYearFrom:            activeCfg.Seeder.FavorYearFrom,
			FavorYearTo:              activeCfg.Seeder.FavorYearTo,
			PrioritizeLowSeeders:     activeCfg.Seeder.PrioritizeLowSeeders,
			CampaignMinSeedMB:        activeCfg.Seeder.CampaignMinSeedMB,
			CampaignMaxSeedMB:        activeCfg.Seeder.CampaignMaxSeedMB,
			CampaignMinTime:          time.Duration(float64(time.Minute) * activeCfg.Seeder.CampaignMinTimeMinutes),
			CampaignMaxTime:          time.Duration(float64(time.Minute) * activeCfg.Seeder.CampaignMaxTimeMinutes),
			CampaignPeerWait:         time.Duration(activeCfg.Seeder.CampaignPeerWaitSeconds) * time.Second,
			CampaignChunkDir:         activeCfg.Seeder.CampaignChunkDir,
			CampaignChunkDirFallback: activeCfg.Seeder.CampaignChunkDirFallback,
			CampaignChunkSizeMB:      activeCfg.Seeder.CampaignChunkSizeMB,
			CampaignLoop:             activeCfg.Seeder.CampaignLoop,
			CampaignTBCycleDays:      activeCfg.Seeder.CampaignTBCycleDays,
			CampaignRDCycleDays:      activeCfg.Seeder.CampaignRDCycleDays,
		})
	}
	persistConfig()
	w.WriteHeader(http.StatusOK)
}

// POST /api/flowarr/seeder/pause — body JSON: {"hash":"..."}
func apiSeederPause(w http.ResponseWriter, r *http.Request) {
	if seedMgr == nil {
		http.Error(w, "seeder not enabled", http.StatusServiceUnavailable)
		return
	}
	var req struct{ Hash string `json:"hash"` }
	json.NewDecoder(r.Body).Decode(&req)
	if req.Hash == "" {
		http.Error(w, "hash required", http.StatusBadRequest)
		return
	}
	if seedMgr.Pause(strings.ToLower(strings.TrimSpace(req.Hash))) {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// POST /api/flowarr/seeder/resume — body JSON: {"hash":"..."}
func apiSeederResume(w http.ResponseWriter, r *http.Request) {
	if seedMgr == nil {
		http.Error(w, "seeder not enabled", http.StatusServiceUnavailable)
		return
	}
	var req struct{ Hash string `json:"hash"` }
	json.NewDecoder(r.Body).Decode(&req)
	if req.Hash == "" {
		http.Error(w, "hash required", http.StatusBadRequest)
		return
	}
	if seedMgr.Resume(strings.ToLower(strings.TrimSpace(req.Hash))) {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// POST /api/flowarr/seeder/prioritize — body JSON: {"hash":"..."}
func apiSeederPrioritize(w http.ResponseWriter, r *http.Request) {
	if seedMgr == nil {
		http.Error(w, "seeder not enabled", http.StatusServiceUnavailable)
		return
	}
	var req struct{ Hash string `json:"hash"` }
	json.NewDecoder(r.Body).Decode(&req)
	if req.Hash == "" {
		http.Error(w, "hash required", http.StatusBadRequest)
		return
	}
	if seedMgr.Prioritize(strings.ToLower(strings.TrimSpace(req.Hash))) {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// POST /api/flowarr/seeder/add — body JSON: {"magnet":"..."}
func apiSeederAdd(w http.ResponseWriter, r *http.Request) {
	if seedMgr == nil {
		http.Error(w, "seeder not enabled", http.StatusServiceUnavailable)
		return
	}
	var req struct{ Magnet string `json:"magnet"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		r.ParseForm()
		req.Magnet = r.FormValue("magnet")
	}
	if req.Magnet == "" {
		http.Error(w, "magnet required", http.StatusBadRequest)
		return
	}
	hash, err := seedMgr.AddMagnet(req.Magnet)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"hash": hash})
}

// POST /api/flowarr/seeder/remove — body JSON: {"hash":"..."}
func apiSeederRemove(w http.ResponseWriter, r *http.Request) {
	if seedMgr == nil {
		http.Error(w, "seeder not enabled", http.StatusServiceUnavailable)
		return
	}
	var req struct{ Hash string `json:"hash"` }
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		r.ParseForm()
		req.Hash = r.FormValue("hash")
	}
	hash := strings.ToLower(strings.TrimSpace(req.Hash))
	if hash == "" {
		http.Error(w, "hash required", http.StatusBadRequest)
		return
	}
	if seedMgr.Remove(hash) {
		w.WriteHeader(http.StatusOK)
	} else {
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// POST /api/flowarr/seeder/reset-cycle
func apiSeederResetCycle(w http.ResponseWriter, r *http.Request) {
	if seedMgr == nil {
		http.Error(w, "seeder not enabled", http.StatusServiceUnavailable)
		return
	}
	seedMgr.ResetCycle()
	w.WriteHeader(http.StatusOK)
}

// POST /api/flowarr/seeder/reset-stats — zeroes per-torrent "session uploaded" counters.
func apiSeederResetStats(w http.ResponseWriter, r *http.Request) {
	if seedMgr == nil {
		http.Error(w, "seeder not enabled", http.StatusServiceUnavailable)
		return
	}
	seedMgr.ResetUploadStats()
	w.WriteHeader(http.StatusOK)
}

// POST /api/flowarr/mirror — bulk-mirror debrid library to a secondary provider.
// Streams Server-Sent Events for live progress. Final event has phase:"done".
func apiMirrorToProvider(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Source   string `json:"source"`
		Provider string `json:"provider"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Provider == "" {
		http.Error(w, "missing provider", http.StatusBadRequest)
		return
	}
	if activeCfg.Decypharr.URL == "" {
		http.Error(w, "decypharr not configured", http.StatusServiceUnavailable)
		return
	}

	// SSE setup with mutex-protected writes so a background keepalive goroutine
	// can safely write concurrently with the main event loop.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, canFlush := w.(http.Flusher)
	var wMu sync.Mutex
	rawWrite := func(s string) {
		wMu.Lock()
		defer wMu.Unlock()
		fmt.Fprint(w, s)
		if canFlush {
			flusher.Flush()
		}
	}
	emit := func(v interface{}) {
		b, _ := json.Marshal(v)
		rawWrite("data: " + string(b) + "\n\n")
	}

	// Background goroutine sends SSE keepalive comments every 25s to prevent
	// nginx from closing the connection during long phases (library fetch, cache check).
	kaStop := make(chan struct{})
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-kaStop:
				return
			case <-t.C:
				rawWrite(": keepalive\n\n")
			}
		}
	}()
	defer close(kaStop)

	emit(map[string]interface{}{"phase": "fetching", "msg": "Fetching library from decypharr…"})

	dc := decypharr.New(activeCfg.Decypharr.URL, activeCfg.Decypharr.MountDir)
	items, err := dc.RawList(r.Context())
	if err != nil {
		emit(map[string]interface{}{"phase": "error", "msg": "Failed to fetch library: " + err.Error()})
		return
	}

	var candidates []mirrorCandidate
	var skipped int
	for _, it := range items {
		if it.Hash == "" {
			skipped++
			continue
		}
		if req.Source != "" && !strings.EqualFold(it.Debrid, req.Source) {
			skipped++
			continue
		}
		if strings.EqualFold(it.Debrid, req.Provider) {
			skipped++
			continue
		}
		candidates = append(candidates, mirrorCandidate{strings.ToLower(it.Hash), it.Category})
	}

	emit(map[string]interface{}{
		"phase":       "start",
		"total":       len(items),
		"candidates":  len(candidates),
		"skipped":     skipped,
		"msg":         fmt.Sprintf("Found %d candidates to submit (skipped %d already on destination)", len(candidates), skipped),
	})

	// TorBox fast path: batch hash-check → cached items go in instantly,
	// uncached items are still submitted so TorBox queues them for P2P download.
	tbKey, hasTBKey := activeCfg.DebridAPIKeys["torbox"]
	if strings.EqualFold(req.Provider, "torbox") && hasTBKey && tbKey != "" {
		mirrorStreamTorBox(r.Context(), tbKey, candidates, skipped, len(items), emit)
		return
	}

	// Fallback: submit via decypharr one by one
	submitted, errs := 0, 0
	for i, c := range candidates {
		if r.Context().Err() != nil {
			break
		}
		magnet := "magnet:?xt=urn:btih:" + c.hash
		if fwdErr := dc.ForwardToProvider(r.Context(), magnet, c.category, req.Provider); fwdErr != nil {
			log.Printf("mirror to %s: %v", req.Provider, fwdErr)
			errs++
		} else {
			submitted++
		}
		if (i+1)%50 == 0 {
			emit(map[string]interface{}{
				"phase":     "adding",
				"progress":  float64(i+1) / float64(len(candidates)),
				"submitted": submitted,
				"errors":    errs,
				"done_of":   i + 1,
				"total":     len(candidates),
			})
		}
	}
	emit(map[string]interface{}{
		"phase":     "done",
		"submitted": submitted,
		"skipped":   skipped,
		"errors":    errs,
		"total":     len(items),
	})
}

type mirrorCandidate struct {
	hash     string
	category string
}

// mirrorStreamTorBox batch-checks TorBox cache then submits ALL candidates.
// Uses a background context for TorBox API calls (independent of the SSE connection)
// and a ticker to stay within TorBox rate limits (~3 req/s).
// Cached items resolve instantly; uncached items are queued for P2P download.
func mirrorStreamTorBox(sseCtx context.Context, apiKey string, candidates []mirrorCandidate,
	skipped, totalItems int, emit func(interface{})) {

	tb := torbox.New(apiKey)

	// Use a long-lived background context for TorBox API calls so the mirror
	// continues even if the SSE browser connection drops.
	tbCtx, tbCancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer tbCancel()

	emit(map[string]interface{}{"phase": "checking_library", "msg": "Fetching your existing TorBox library…"})
	existing, err := tb.LibraryHashes(tbCtx)
	if err != nil {
		log.Printf("torbox mirror: could not fetch library: %v", err)
		existing = map[string]struct{}{}
	}

	var toSubmit []string
	alreadyIn := 0
	for _, c := range candidates {
		if _, ok := existing[c.hash]; ok {
			alreadyIn++
		} else {
			toSubmit = append(toSubmit, c.hash)
		}
	}

	emit(map[string]interface{}{
		"phase":      "checking_cache",
		"msg":        fmt.Sprintf("Checking TorBox global cache for %d hashes (single batch POST)…", len(toSubmit)),
		"already_in": alreadyIn,
		"to_check":   len(toSubmit),
	})

	// Single POST call for all hashes (much faster than 100-per-GET batching)
	cached, cErr := tb.CheckCachedBatch(tbCtx, toSubmit)
	if cErr != nil {
		log.Printf("torbox checkcached batch: %v", cErr)
		cached = map[string]struct{}{}
	}

	cachedCount := len(cached)
	// Save uncached hashes for the "Transfer via RD" button
	uncached := make([]string, 0, len(toSubmit)-cachedCount)
	for _, h := range toSubmit {
		if _, ok := cached[h]; !ok {
			uncached = append(uncached, h)
		}
	}
	lastUncachedHashes = uncached
	notCachedCount := len(uncached)

	// TorBox rate limits (from API docs):
	//   Cached items:   300/min = 5/sec → 200ms between requests
	//   Uncached items: 60/HOUR = 1/min → 61s between requests (hard limit!)
	hoursForUncached := float64(notCachedCount) / 60.0
	uncachedETA := fmt.Sprintf("%.1f hours", hoursForUncached)
	emit(map[string]interface{}{
		"phase":       "adding",
		"msg":         fmt.Sprintf("Found %d globally cached (fast) + %d uncached (60/hour limit — ETA %s). Starting cached submissions…", cachedCount, notCachedCount, uncachedETA),
		"cached":      cachedCount,
		"not_cached":  notCachedCount,
		"already_in":  alreadyIn,
		"eta_hours":   hoursForUncached,
	})

	cachedTicker := time.NewTicker(210 * time.Millisecond) // ~4.7/s well under 5/s limit
	defer cachedTicker.Stop()

	// waitUncached waits exactly 61s (60/hour rate limit = 1/min).
	// Keepalives are sent by the background goroutine in apiMirrorToProvider.
	waitUncached := func() bool {
		select {
		case <-tbCtx.Done():
			return false
		case <-time.After(61 * time.Second):
			return true
		}
	}

	// Submit cached items first (instant, fast rate)
	submittedInstant, submittedQueued, errs := 0, 0, 0
	cachedList := make([]string, 0, cachedCount)
	for _, h := range toSubmit {
		if _, ok := cached[h]; ok {
			cachedList = append(cachedList, h)
		}
	}
	for i, h := range cachedList {
		if tbCtx.Err() != nil { break }
		<-cachedTicker.C
		var addErr error
		for attempt := 0; attempt < 3; attempt++ {
			addErr = tb.AddByHash(tbCtx, h, false)
			if rl, ok := addErr.(torbox.ErrRateLimit); ok {
				log.Printf("torbox rate limited (cached), waiting %v", rl.RetryAfter)
				time.Sleep(rl.RetryAfter)
				continue
			}
			break
		}
		if addErr != nil { log.Printf("torbox add cached: %v", addErr); errs++ } else { submittedInstant++ }
		if (i+1)%50 == 0 || i+1 == len(cachedList) {
			emit(map[string]interface{}{
				"phase":             "adding_cached",
				"progress":          float64(i+1) / float64(len(cachedList)) * 0.3,
				"submitted_instant": submittedInstant,
				"errors":            errs,
				"done_of":           i + 1,
				"total":             len(cachedList),
				"msg":               fmt.Sprintf("Cached: %d/%d submitted", submittedInstant, len(cachedList)),
			})
		}
	}

	// Submit uncached items at 1/min (hard TorBox limit: 60/hour)
	if notCachedCount > 0 {
		hoursLeft := float64(notCachedCount) / 60.0
		emit(map[string]interface{}{
			"phase":       "adding_uncached",
			"progress":    0.3,
			"not_cached":  notCachedCount,
			"eta_hours":   hoursLeft,
			"msg":         fmt.Sprintf("Starting uncached P2P submissions: %d items at 1/min = %.1f hours remaining…", notCachedCount, hoursLeft),
		})
	}
	for i, h := range uncached {
		if tbCtx.Err() != nil { break }
		if !waitUncached() { break }
		var addErr error
		for attempt := 0; attempt < 3; attempt++ {
			addErr = tb.AddByHash(tbCtx, h, true) // as_queued=true for uncached
			if rl, ok := addErr.(torbox.ErrRateLimit); ok {
				log.Printf("torbox rate limited (uncached), waiting %v", rl.RetryAfter)
				time.Sleep(rl.RetryAfter)
				continue
			}
			break
		}
		if addErr != nil { log.Printf("torbox add uncached: %v", addErr); errs++ } else { submittedQueued++ }
		remaining := notCachedCount - i - 1
		if (i+1)%5 == 0 || i+1 == notCachedCount {
			hoursLeft := float64(remaining) / 60.0
			emit(map[string]interface{}{
				"phase":            "adding_uncached",
				"progress":         0.3 + float64(i+1)/float64(notCachedCount)*0.7,
				"submitted_queued": submittedQueued,
				"errors":           errs,
				"done_of":          i + 1,
				"total":            notCachedCount,
				"remaining":        remaining,
				"eta_hours":        hoursLeft,
				"msg":              fmt.Sprintf("Uncached P2P: %d/%d — %d queued, %.1f hours remaining", i+1, notCachedCount, submittedQueued, hoursLeft),
			})
		}
	}

	emit(map[string]interface{}{
		"phase":             "done",
		"submitted_instant": submittedInstant,
		"submitted_queued":  submittedQueued,
		"already_in":        alreadyIn,
		"errors":            errs,
		"skipped":           skipped,
		"not_cached":        notCachedCount,
		"total":             totalItems,
	})
}

// lastUncachedHashes stores hashes found uncached on TorBox after the last mirror run.
// Used so the "Transfer via RD" button doesn't have to re-check.
var lastUncachedHashes []string

// POST /api/flowarr/mirror/transfer — transfer uncached items from RD to TorBox
// via unrestricted RD download URLs → TorBox webdl. Streams SSE progress.
// Body: {"hashes":["aabb...", ...]} or {} to use last uncached list from mirror.
func apiMirrorTransferRD(w http.ResponseWriter, r *http.Request) {
	rdKey := activeCfg.DebridAPIKeys["realdebrid"]
	tbKey := activeCfg.DebridAPIKeys["torbox"]
	if rdKey == "" || tbKey == "" {
		http.Error(w, "realdebrid and torbox api keys required in settings", http.StatusBadRequest)
		return
	}

	var req struct {
		Hashes []string `json:"hashes"`
	}
	json.NewDecoder(r.Body).Decode(&req)
	hashes := req.Hashes
	if len(hashes) == 0 {
		hashes = lastUncachedHashes
	}
	if len(hashes) == 0 {
		http.Error(w, "no hashes to transfer — run Mirror first or provide hashes", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	flusher, canFlush := w.(http.Flusher)
	emit := func(v interface{}) {
		b, _ := json.Marshal(v)
		fmt.Fprintf(w, "data: %s\n\n", b)
		if canFlush {
			flusher.Flush()
		}
	}

	tbCtx, tbCancel := context.WithTimeout(context.Background(), 90*time.Minute)
	defer tbCancel()

	rd := realdebrid.New(rdKey)
	tb := torbox.New(tbKey)

	emit(map[string]interface{}{"phase": "fetching_rd", "msg": fmt.Sprintf("Fetching RD library to match %d hashes…", len(hashes))})

	rdTorrents, err := rd.ListTorrents(tbCtx)
	if err != nil {
		emit(map[string]interface{}{"phase": "error", "msg": "Failed to fetch RD library: " + err.Error()})
		return
	}

	// Build hash→torrent map (lowercase)
	hashSet := make(map[string]struct{}, len(hashes))
	for _, h := range hashes {
		hashSet[strings.ToLower(h)] = struct{}{}
	}
	type work struct {
		name  string
		links []string
	}
	byHash := make(map[string]work)
	for _, t := range rdTorrents {
		h := strings.ToLower(t.Hash)
		if _, ok := hashSet[h]; ok && len(t.Links) > 0 && t.Status == "downloaded" {
			byHash[h] = work{t.Name, t.Links}
		}
	}

	emit(map[string]interface{}{
		"phase": "start",
		"msg":   fmt.Sprintf("Found %d/%d hashes in RD with download links", len(byHash), len(hashes)),
		"found": len(byHash),
		"total": len(hashes),
	})

	ticker := time.NewTicker(400 * time.Millisecond) // ~2.5/s to stay under both RD and TorBox rate limits
	defer ticker.Stop()

	submitted, errs, noLinks := 0, 0, 0
	done := 0
	for h, w := range byHash {
		if tbCtx.Err() != nil {
			break
		}
		<-ticker.C
		// Use the first link (primary file); unrestrict it then send to TorBox webdl
		unres, uErr := rd.UnrestrictLink(tbCtx, w.links[0])
		if uErr != nil {
			log.Printf("rd unrestrict %s: %v", h, uErr)
			errs++
		} else {
			for attempt := 0; attempt < 3; attempt++ {
				tErr := tb.AddByURL(tbCtx, unres)
				if rl, ok := tErr.(torbox.ErrRateLimit); ok {
					time.Sleep(rl.RetryAfter)
					continue
				}
				if tErr != nil {
					log.Printf("torbox webdl %s: %v", h, tErr)
					errs++
				} else {
					submitted++
				}
				break
			}
		}
		done++
		if done%20 == 0 || done == len(byHash) {
			emit(map[string]interface{}{
				"phase":     "transferring",
				"progress":  float64(done) / float64(len(byHash)),
				"submitted": submitted,
				"errors":    errs,
				"done_of":   done,
				"total":     len(byHash),
				"msg":       fmt.Sprintf("Transferring: %d/%d — %d submitted, %d errors", done, len(byHash), submitted, errs),
			})
		}
		_ = noLinks
	}
	emit(map[string]interface{}{
		"phase":     "done",
		"submitted": submitted,
		"errors":    errs,
		"no_links":  len(hashes) - len(byHash),
		"total":     len(hashes),
	})
}

// persistConfig writes activeCfg back to flowarr.yaml so settings survive restarts.
func persistConfig() {
	if cfgFilePath == "" {
		return
	}
	data, err := yaml.Marshal(activeCfg)
	if err != nil {
		log.Printf("persistConfig marshal: %v", err)
		return
	}
	if err := os.WriteFile(cfgFilePath, data, 0o644); err != nil {
		log.Printf("persistConfig write: %v", err)
	}
}

func health(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{
		"status":  "ok",
		"version": version,
	})
}

// apiQueue returns a merged view of Decypharr's RD queue and Flowarr's own
// BitTorrent downloads, intended for the web UI.
func apiQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	type QueueItem struct {
		Hash        string  `json:"hash"`
		Name        string  `json:"name"`
		Size        int64   `json:"size"`
		Progress    float64 `json:"progress"`
		DlSpeed     int64   `json:"dlspeed"`
		ETA         int64   `json:"eta"`
		State       string  `json:"state"`
		Category    string  `json:"category"`
		SavePath    string  `json:"save_path"`
		ContentPath string  `json:"content_path"`
		Downloaded  int64   `json:"downloaded"`
		AmountLeft  int64   `json:"amount_left"`
		Debrid      string  `json:"debrid"`
		AddedOn     int64   `json:"added_on"`
		Seeds       int     `json:"num_seeds"`
		Source      string  `json:"source"` // "decypharr" | "flowarr" | "both"
	}

	byHash := map[string]*QueueItem{}

	// Pull from Decypharr.
	if dcClient != nil {
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if items, err := dcClient.RawList(ctx); err == nil {
			for _, it := range items {
				q := &QueueItem{
					Hash:        it.Hash,
					Name:        it.Name,
					Size:        it.Size,
					Progress:    it.Progress,
					DlSpeed:     it.DlSpeed,
					ETA:         it.ETA,
					State:       it.State,
					Category:    it.Category,
					SavePath:    it.SavePath,
					ContentPath: it.ContentPath,
					Downloaded:  it.Downloaded,
					AmountLeft:  it.AmountLeft,
					Debrid:      it.Debrid,
					AddedOn:     it.AddedOn,
					Seeds:       it.Seeds,
					Source:      "decypharr",
				}
				byHash[strings.ToLower(it.Hash)] = q
			}
		}
	}

	// Merge Flowarr's own BitTorrent downloads.
	for _, t := range mgr.List() {
		hash := t.InfoHash().HexString()
		if q, exists := byHash[hash]; exists {
			q.Source = "both"
			continue
		}
		q := &QueueItem{
			Hash:     hash,
			Source:   "flowarr",
			SavePath: mgr.SavePath(hash),
			Category: mgr.Category(hash),
		}
		select {
		case <-t.GotInfo():
			total := t.Length()
			q.Name = t.Name()
			q.Size = total
			q.Downloaded = 0 // on-demand: pieces fetched only as Plex reads
			q.AmountLeft = 0
			q.Progress = 1.0
			q.State = "uploading"
			q.Seeds = t.Stats().ConnectedSeeders
			q.DlSpeed = mgr.Speed(hash)
		default:
			q.State = "downloading"
		}
		byHash[hash] = q
	}

	out := make([]*QueueItem, 0, len(byHash))
	for _, q := range byHash {
		out = append(out, q)
	}
	json.NewEncoder(w).Encode(out)
}

func apiSettings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method == http.MethodPost {
		var patch struct {
			DecypharrURL     string                 `json:"decypharr_url"`
			DecypharrMount   string                 `json:"decypharr_mount"`
			DecypharrPoll    string                 `json:"decypharr_poll"`
			ProwlarrURL      string                 `json:"prowlarr_url"`
			ProwlarrAPIKey   string                 `json:"prowlarr_api_key"`
			Arrs             []config.ArrInstance   `json:"arrs"`
			CategoryMappings map[string]string      `json:"category_mappings"`
			DebridAPIKeys    map[string]string      `json:"debrid_api_keys"`
			Streaming        *struct {
				SeedThreshold    int      `json:"seed_threshold"`
				HealthCheckDelay string   `json:"health_check_delay"`
				ReadaheadMB      int      `json:"readahead_mb"`
				ExtraTrackers    []string `json:"extra_trackers"`
				ProwlarrIndexIDs []int    `json:"prowlarr_indexer_ids"`
			} `json:"streaming"`
		}
		if err := json.NewDecoder(r.Body).Decode(&patch); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if patch.DecypharrURL != "" {
			activeCfg.Decypharr.URL = patch.DecypharrURL
		}
		if patch.DecypharrMount != "" {
			activeCfg.Decypharr.MountDir = patch.DecypharrMount
		}
		if patch.DecypharrPoll != "" {
			if d, err := time.ParseDuration(patch.DecypharrPoll); err == nil {
				activeCfg.Decypharr.PollInterval = d
			}
		}
		if patch.ProwlarrURL != "" {
			activeCfg.Prowlarr.URL = patch.ProwlarrURL
		}
		if patch.ProwlarrAPIKey != "" {
			activeCfg.Prowlarr.APIKey = patch.ProwlarrAPIKey
		}
		if patch.Arrs != nil {
			activeCfg.Arrs = patch.Arrs
		}
		if patch.CategoryMappings != nil {
			activeCfg.CategoryMappings = patch.CategoryMappings
		}
		if patch.DebridAPIKeys != nil {
			if activeCfg.DebridAPIKeys == nil {
				activeCfg.DebridAPIKeys = map[string]string{}
			}
			for k, v := range patch.DebridAPIKeys {
				if v != "" {
					activeCfg.DebridAPIKeys[k] = v
				}
			}
		}
		if s := patch.Streaming; s != nil {
			if s.SeedThreshold > 0 {
				activeCfg.Streaming.SeedThreshold = s.SeedThreshold
			}
			if s.HealthCheckDelay != "" {
				if d, err := time.ParseDuration(s.HealthCheckDelay); err == nil {
					activeCfg.Streaming.HealthCheckDelay = d
				}
			}
			if s.ReadaheadMB > 0 {
				activeCfg.Streaming.ReadaheadMB = s.ReadaheadMB
			}
			if s.ExtraTrackers != nil {
				activeCfg.Streaming.ExtraTrackers = s.ExtraTrackers
			}
			if s.ProwlarrIndexIDs != nil {
				activeCfg.Streaming.ProwlarrIndexIDs = s.ProwlarrIndexIDs
			}
		}
		// Re-initialise Prowlarr searcher if credentials changed.
		if activeCfg.Prowlarr.URL != "" && activeCfg.Prowlarr.APIKey != "" {
			fallbackSearcher = fallback.New(activeCfg.Prowlarr.URL, activeCfg.Prowlarr.APIKey)
		}
		persistConfig()
	}

	indexIDs := activeCfg.Streaming.ProwlarrIndexIDs
	if indexIDs == nil {
		indexIDs = []int{}
	}
	extraTrackers := activeCfg.Streaming.ExtraTrackers
	if extraTrackers == nil {
		extraTrackers = []string{}
	}
	arrs := activeCfg.Arrs
	if arrs == nil {
		arrs = []config.ArrInstance{}
	}

	debridKeys := activeCfg.DebridAPIKeys
	if debridKeys == nil {
		debridKeys = map[string]string{}
	}
	json.NewEncoder(w).Encode(map[string]interface{}{
		"version":          version,
		"addr":             activeCfg.Server.Addr,
		"data_dir":         activeCfg.Torrent.DataDir,
		"mount_dir":        activeCfg.FUSE.MountDir,
		"library_dir":      activeCfg.Library.BaseDir,
		"fuse_ok":          fuseOK,
		"decypharr_url":    activeCfg.Decypharr.URL,
		"decypharr_mount":  activeCfg.Decypharr.MountDir,
		"decypharr_poll":   activeCfg.Decypharr.PollInterval.String(),
		"prowlarr_url":     activeCfg.Prowlarr.URL,
		"prowlarr_api_key": activeCfg.Prowlarr.APIKey,
		"arrs":             arrs,
		"category_mappings": activeCfg.CategoryMappings,
		"debrid_api_keys":  debridKeys,
		"streaming": map[string]interface{}{
			"seed_threshold":      activeCfg.Streaming.SeedThreshold,
			"health_check_delay":  activeCfg.Streaming.HealthCheckDelay.String(),
			"readahead_mb":        activeCfg.Streaming.ReadaheadMB,
			"extra_trackers":      extraTrackers,
			"prowlarr_indexer_ids": indexIDs,
		},
	})
}

// GET /api/flowarr/tblibrary — full TorBox library with rich metadata.
func apiTBLibrary(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tbKey := activeCfg.DebridAPIKeys["torbox"]
	if tbKey == "" {
		json.NewEncoder(w).Encode([]struct{}{})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	items, err := torbox.New(tbKey).ListTorrents(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	json.NewEncoder(w).Encode(items)
}

// GET /api/flowarr/tbqueue — TorBox queued items (not yet downloading).
func apiTBQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	tbKey := activeCfg.DebridAPIKeys["torbox"]
	if tbKey == "" {
		json.NewEncoder(w).Encode([]struct{}{})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	items, err := torbox.New(tbKey).ListQueued(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	json.NewEncoder(w).Encode(items)
}

// GET /api/flowarr/rdlibrary — full RD library from Decypharr, independent of seeder state.
func apiRDLibrary(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if activeCfg.Decypharr.URL == "" {
		json.NewEncoder(w).Encode([]struct{}{})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	items, err := dcClient.RawList(ctx)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	json.NewEncoder(w).Encode(items)
}

func apiLogs(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	logBufMu.RLock()
	out := make([]string, len(logLines))
	copy(out, logLines)
	logBufMu.RUnlock()
	json.NewEncoder(w).Encode(out)
}

var videoExts = map[string]string{
	".mp4": "video/mp4", ".m4v": "video/mp4", ".mov": "video/quicktime",
	".mkv": "video/x-matroska", ".webm": "video/webm", ".avi": "video/x-msvideo",
	".ts": "video/mp2t", ".m2ts": "video/mp2t",
}

// apiStream serves a file from the browse paths with Range-request support so
// the browser can seek. Pass ?path=<absolute> for filesystem files, or
// ?path=<absolute>&download=1 to force a download prompt.
func apiStream(w http.ResponseWriter, r *http.Request) {
	reqPath := filepath.Clean(r.URL.Query().Get("path"))

	allowed := []string{}
	if mountPath != "" {
		allowed = append(allowed, mountPath)
	}
	if dcClient != nil && dcClient.MountDir() != "" {
		allowed = append(allowed, dcClient.MountDir())
	}
	if activeCfg.Library.BaseDir != "" {
		allowed = append(allowed, activeCfg.Library.BaseDir)
	}
	permitted := false
	for _, a := range allowed {
		if a != "" && strings.HasPrefix(reqPath, a) {
			permitted = true
			break
		}
	}
	if !permitted {
		http.Error(w, "path not in allowed roots", http.StatusForbidden)
		return
	}

	f, err := os.Open(reqPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil || fi.IsDir() {
		http.Error(w, "not a file", http.StatusBadRequest)
		return
	}

	ext := strings.ToLower(filepath.Ext(reqPath))
	if r.URL.Query().Get("download") == "1" {
		w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(reqPath))
		w.Header().Set("Content-Type", "application/octet-stream")
	} else if mime, ok := videoExts[ext]; ok {
		w.Header().Set("Content-Type", mime)
	} else {
		w.Header().Set("Content-Disposition", "attachment; filename="+filepath.Base(reqPath))
		w.Header().Set("Content-Type", "application/octet-stream")
	}

	http.ServeContent(w, r, fi.Name(), fi.ModTime(), f)
}

func apiBrowse(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	reqPath := filepath.Clean(r.URL.Query().Get("path"))

	allowed := []string{}
	if mountPath != "" {
		allowed = append(allowed, mountPath)
	}
	if dcClient != nil && dcClient.MountDir() != "" {
		allowed = append(allowed, dcClient.MountDir())
	}
	if activeCfg.Library.BaseDir != "" {
		allowed = append(allowed, activeCfg.Library.BaseDir)
	}

	if reqPath == "" || reqPath == "." {
		// Default to library if FUSE mount is empty, else FUSE mount
		reqPath = mountPath
		if mountPath != "" {
			if entries, _ := os.ReadDir(mountPath); len(entries) == 0 && activeCfg.Library.BaseDir != "" {
				reqPath = activeCfg.Library.BaseDir
			}
		} else if activeCfg.Library.BaseDir != "" {
			reqPath = activeCfg.Library.BaseDir
		}
	}
	permitted := false
	for _, a := range allowed {
		if a != "" && strings.HasPrefix(reqPath, a) {
			permitted = true
			break
		}
	}
	if !permitted {
		http.Error(w, "path not in allowed roots", http.StatusForbidden)
		return
	}

	entries, err := os.ReadDir(reqPath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	type FileEntry struct {
		Name  string `json:"name"`
		IsDir bool   `json:"is_dir"`
		Size  int64  `json:"size"`
		Path  string `json:"path"`
	}

	items := make([]FileEntry, 0, len(entries))
	for _, e := range entries {
		size := int64(0)
		if info, err2 := e.Info(); err2 == nil && !e.IsDir() {
			size = info.Size()
		}
		items = append(items, FileEntry{
			Name:  e.Name(),
			IsDir: e.IsDir(),
			Size:  size,
			Path:  filepath.Join(reqPath, e.Name()),
		})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].IsDir != items[j].IsDir {
			return items[i].IsDir
		}
		return items[i].Name < items[j].Name
	})

	parent := filepath.Dir(reqPath)
	parentOK := false
	for _, a := range allowed {
		if strings.HasPrefix(parent, a) || parent == a {
			parentOK = true
			break
		}
	}
	if !parentOK {
		parent = reqPath
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"path":   reqPath,
		"parent": parent,
		"roots":  allowed,
		"items":  items,
	})
}

const uiHTML = `<!DOCTYPE html>
<html lang="en" data-theme="dark">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Flowarr</title>
<link rel="icon" type="image/svg+xml" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 100 100'%3E%3Cdefs%3E%3ClinearGradient id='pg' x1='0' y1='0' x2='1' y2='1'%3E%3Cstop offset='0%25' stop-color='%23d946ef'/%3E%3Cstop offset='100%25' stop-color='%2306b6d4'/%3E%3C/linearGradient%3E%3ClinearGradient id='cg' x1='0' y1='0' x2='1' y2='1'%3E%3Cstop offset='0%25' stop-color='%23f0abfc'/%3E%3Cstop offset='100%25' stop-color='%2367e8f9'/%3E%3C/linearGradient%3E%3C/defs%3E%3Cg transform='translate(50,50)'%3E%3Ccircle r='44' fill='none' stroke='url(%23pg)' stroke-width='2.5' opacity='.35'/%3E%3Cpath d='M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z' fill='url(%23pg)' opacity='.9'/%3E%3Cpath d='M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z' fill='url(%23pg)' opacity='.9' transform='rotate(60)'/%3E%3Cpath d='M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z' fill='url(%23pg)' opacity='.9' transform='rotate(120)'/%3E%3Cpath d='M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z' fill='url(%23pg)' opacity='.9' transform='rotate(180)'/%3E%3Cpath d='M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z' fill='url(%23pg)' opacity='.9' transform='rotate(240)'/%3E%3Cpath d='M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z' fill='url(%23pg)' opacity='.9' transform='rotate(300)'/%3E%3Ccircle r='9' fill='url(%23cg)'/%3E%3Ccircle r='3.5' fill='white' opacity='.95'/%3E%3C/g%3E%3C/svg%3E">
<link href="https://cdn.jsdelivr.net/npm/daisyui@4.12.10/dist/full.min.css" rel="stylesheet">
<link href="https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css" rel="stylesheet">
<script src="https://cdn.tailwindcss.com"></script>
<script>(function(){const t=localStorage.getItem('theme')||'dark';document.documentElement.setAttribute('data-theme',t);})();</script>
<style>
.log-error{color:#f87171}.log-warn{color:#fbbf24}.log-info{color:#6ee7b7}.log-debug{color:#94a3b8}
.tab-active{border-bottom:2px solid oklch(var(--p));color:oklch(var(--p))}
</style>
</head>
<body class="min-h-screen bg-base-200 flex flex-col font-sans">
<div id="toast" class="toast toast-end toast-bottom z-50 pointer-events-none"></div>

<!-- ── Top bar ─────────────────────────────────────────────── -->
<div class="bg-base-100 border-b border-base-300 shadow-sm sticky top-0 z-40">
  <div class="max-w-7xl mx-auto px-4 flex items-center h-14 gap-4">
    <a class="flex items-center gap-0 font-bold text-lg select-none" onclick="nav('dashboard')" style="color:oklch(var(--p));letter-spacing:-0.01em">
      <span>Fl</span>
      <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 100 100" style="width:1.35em;height:1.35em;flex-shrink:0;margin:0 0.02em">
        <defs>
          <linearGradient id="hpg" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#d946ef"/>
            <stop offset="100%" stop-color="#06b6d4"/>
          </linearGradient>
          <linearGradient id="hcg" x1="0" y1="0" x2="1" y2="1">
            <stop offset="0%" stop-color="#f0abfc"/>
            <stop offset="100%" stop-color="#67e8f9"/>
          </linearGradient>
        </defs>
        <g transform="translate(50,50)">
          <circle r="44" fill="none" stroke="url(#hpg)" stroke-width="2.5" opacity=".35"/>
          <path d="M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z" fill="url(#hpg)" opacity=".9"/>
          <path d="M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z" fill="url(#hpg)" opacity=".9" transform="rotate(60)"/>
          <path d="M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z" fill="url(#hpg)" opacity=".9" transform="rotate(120)"/>
          <path d="M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z" fill="url(#hpg)" opacity=".9" transform="rotate(180)"/>
          <path d="M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z" fill="url(#hpg)" opacity=".9" transform="rotate(240)"/>
          <path d="M0,0 C-10,-4,-10,-22,-2,-34 C4,-28,8,-14,0,0Z" fill="url(#hpg)" opacity=".9" transform="rotate(300)"/>
          <circle r="9" fill="url(#hcg)"/>
          <circle r="3.5" fill="white" opacity=".95"/>
        </g>
      </svg>
      <span>warr</span>
    </a>
    <nav class="flex gap-1 flex-shrink min-w-0">
      <button id="tab-dashboard" class="tab px-3 py-1.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap hover:bg-base-200" onclick="nav('dashboard')">
        <i class="bi bi-grid-3x3-gap mr-1.5"></i><span class="hidden sm:inline">Dashboard</span>
      </button>
      <button id="tab-downloads" class="tab px-3 py-1.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap hover:bg-base-200" onclick="nav('downloads')">
        <i class="bi bi-cloud-download mr-1.5"></i><span class="hidden sm:inline">Downloads</span>
      </button>
      <button id="tab-browse" class="tab px-3 py-1.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap hover:bg-base-200" onclick="nav('browse')">
        <i class="bi bi-folder2-open mr-1.5"></i><span class="hidden sm:inline">Browse</span>
      </button>
      <button id="tab-seeder" class="tab px-3 py-1.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap hover:bg-base-200" onclick="nav('seeder')">
        <i class="bi bi-upload mr-1.5"></i><span class="hidden sm:inline">Seeder</span>
      </button>
      <button id="tab-logs" class="tab px-3 py-1.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap hover:bg-base-200" onclick="nav('logs')">
        <i class="bi bi-terminal mr-1.5"></i><span class="hidden sm:inline">Logs</span>
      </button>
      <button id="tab-settings" class="tab px-3 py-1.5 rounded-md text-sm font-medium transition-colors whitespace-nowrap hover:bg-base-200" onclick="nav('settings')">
        <i class="bi bi-gear mr-1.5"></i><span class="hidden sm:inline">Settings</span>
      </button>
    </nav>
    <div class="flex items-center gap-2 flex-shrink-0">
      <span id="fuse-pill" class="hidden badge badge-sm badge-success gap-1"><i class="bi bi-hdd-network text-[10px]"></i>FUSE</span>
      <span id="debrid-pills" class="flex gap-1"></span>
      <button id="dl-toggle-btn" class="btn btn-xs gap-1 font-mono" onclick="toggleDownloads()" title="Enable/disable Flowarr as arr download client">
        <i class="bi bi-cloud-download" id="dl-toggle-icon"></i><span id="dl-toggle-label">DL</span>
      </button>
      <button class="btn btn-ghost btn-xs btn-circle" onclick="toggleTheme()"><i class="bi bi-circle-half"></i></button>
      <span class="text-xs text-base-content/30 font-mono" id="ver">v0.1.0</span>
    </div>
  </div>
</div>

<main class="flex-1 p-4 md:p-6 max-w-7xl mx-auto w-full">

<!-- ── DASHBOARD ──────────────────────────────────────────── -->
<div id="page-dashboard">
  <div class="grid grid-cols-2 sm:grid-cols-4 lg:grid-cols-7 gap-3 mb-5">
    <div class="bg-base-100 border border-base-300 rounded-xl p-3">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Downloading</div>
      <div class="text-2xl font-bold text-primary" id="s-dl">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Cached</div>
      <div class="text-2xl font-bold text-success" id="s-up">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Stalled</div>
      <div class="text-2xl font-bold text-warning" id="s-stall">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Total</div>
      <div class="text-2xl font-bold" id="s-total">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1" title="Debrid download speed (from Decypharr)">Debrid Speed</div>
      <div class="text-2xl font-bold text-accent" id="s-speed">—</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Total Seeds</div>
      <div class="text-2xl font-bold text-secondary" id="s-seeds">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Upload Speed</div>
      <div class="text-2xl font-bold text-success" id="s-upload">—</div>
    </div>
  </div>

  <!-- Per-provider stats — dynamically populated -->
  <div id="provider-stats-row" class="hidden gap-3 mb-5 flex-wrap"></div>

  <!-- Source breakdown — one card per debrid provider + one for BitTorrent -->
  <div id="source-panels" class="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-5">
    <!-- Debrid provider cards rendered dynamically by renderDashboard() -->
    <div class="bg-base-100 border border-base-300 rounded-xl" id="bt-panel">
      <div class="flex items-center gap-2 px-4 py-3 border-b border-base-300 cursor-pointer select-none" onclick="togglePanel('bt')">
        <i class="bi bi-magnet-fill text-primary"></i>
        <span class="font-semibold text-sm">BitTorrent (Flowarr)</span>
        <span class="badge badge-sm badge-primary ml-auto" id="bt-count">0</span>
        <i class="bi bi-chevron-down text-xs ml-1 transition-transform" id="bt-chevron"></i>
      </div>
      <div id="bt-list" class="divide-y divide-base-300 max-h-64 overflow-y-auto"></div>
    </div>
  </div>

  <!-- Quick add -->
  <div class="bg-base-100 border border-base-300 rounded-xl p-4">
    <div class="flex items-center gap-2 mb-3">
      <i class="bi bi-plus-circle-fill text-primary"></i>
      <span class="font-semibold text-sm">Add to Queue</span>
    </div>
    <div class="flex flex-wrap gap-2">
      <input id="magnet-in" class="input input-bordered input-sm flex-1 min-w-52 font-mono text-xs" placeholder="magnet:?xt=urn:btih:…"/>
      <input id="savepath-in" class="input input-bordered input-sm flex-1 min-w-52" placeholder="Save path e.g. /mnt/library/movies"/>
      <input id="cat-in" class="input input-bordered input-sm w-32" placeholder="Category"/>
      <button class="btn btn-primary btn-sm gap-1" onclick="addTorrent()"><i class="bi bi-cloud-download-fill"></i>Add</button>
    </div>
  </div>
</div>

<!-- ── DOWNLOADS ───────────────────────────────────────────── -->
<div id="page-downloads" class="hidden">
  <div class="flex flex-wrap items-center gap-3 mb-3">
    <h2 class="text-base font-bold">Downloads</h2>
    <input id="dl-search" class="input input-bordered input-xs w-44" placeholder="Search…" oninput="renderDownloads()"/>
    <select id="dl-state" class="select select-bordered select-xs" onchange="renderDownloads()">
      <option value="">All states</option>
      <option value="downloading">Downloading</option>
      <option value="uploading">Seeding/Cached</option>
      <option value="stalledUP">Stalled</option>
    </select>
    <button class="btn btn-ghost btn-xs gap-1 ml-auto" onclick="refresh()"><i class="bi bi-arrow-clockwise"></i>Refresh</button>
  </div>
  <!-- Quick add strip -->
  <div class="bg-base-100 border border-base-300 rounded-xl p-3 mb-4 flex flex-wrap gap-2">
    <input id="magnet-in2" class="input input-bordered input-xs flex-1 min-w-52 font-mono" placeholder="magnet:?xt=urn:btih:…"/>
    <input id="savepath-in2" class="input input-bordered input-xs flex-1 min-w-48" placeholder="Save path"/>
    <input id="cat-in2" class="input input-bordered input-xs w-28" placeholder="Category"/>
    <button class="btn btn-primary btn-xs gap-1" onclick="addTorrent2()"><i class="bi bi-plus-lg"></i>Add</button>
  </div>
  <!-- Per-provider collapsible sections rendered dynamically -->
  <div id="dl-sections" class="flex flex-col gap-3"></div>
  <div id="dl-empty" class="hidden text-center text-base-content/25 py-16">
    <i class="bi bi-inbox text-4xl block mb-2"></i>
    <p class="text-sm">No downloads</p>
  </div>
</div>

<!-- ── BROWSE ─────────────────────────────────────────────── -->
<div id="page-browse" class="hidden">
  <div class="flex flex-wrap items-center gap-2 mb-4">
    <h2 class="text-base font-bold flex-1">File Browser</h2>
    <div id="browse-roots" class="flex gap-2"></div>
  </div>
  <!-- Breadcrumb -->
  <div class="text-sm breadcrumbs bg-base-100 border border-base-300 rounded-xl px-4 py-2 mb-3 overflow-x-auto">
    <ul id="breadcrumb"><li class="text-base-content/30">—</li></ul>
  </div>
  <!-- Stats bar -->
  <div id="browse-stats" class="hidden flex flex-wrap items-center gap-x-5 gap-y-1 px-4 py-2 mb-3 text-xs text-base-content/50 bg-base-100 border border-base-300 rounded-xl">
    <span id="stat-folders" class="flex items-center gap-1"><i class="bi bi-folder-fill text-warning"></i><span></span></span>
    <span id="stat-files" class="flex items-center gap-1"><i class="bi bi-file-earmark"></i><span></span></span>
    <span id="stat-videos" class="flex items-center gap-1"><i class="bi bi-play-circle-fill text-primary"></i><span></span></span>
    <span id="stat-size" class="flex items-center gap-1 ml-auto font-mono"><i class="bi bi-hdd"></i><span></span></span>
  </div>
  <!-- File list -->
  <div class="bg-base-100 border border-base-300 rounded-xl overflow-hidden">
    <div id="browse-list" class="divide-y divide-base-300 min-h-24"></div>
    <div id="browse-empty" class="hidden text-center text-base-content/25 py-10 text-sm">
      <i class="bi bi-folder-x text-3xl block mb-2"></i>Empty directory
    </div>
    <div id="browse-error" class="hidden text-center text-error py-10 text-sm">
      <i class="bi bi-exclamation-triangle text-3xl block mb-2"></i>
      <span id="browse-error-msg"></span>
    </div>
  </div>
</div>

<!-- ── VIDEO PLAYER MODAL ─────────────────────────────────── -->
<dialog id="video-modal" class="modal">
  <div class="modal-box max-w-5xl w-full p-0 overflow-hidden bg-black">
    <div class="flex items-center justify-between px-4 py-2 bg-base-300">
      <span id="video-title" class="text-sm font-medium truncate flex-1 mr-4"></span>
      <div class="flex gap-2">
        <a id="video-dl-link" class="btn btn-xs btn-ghost gap-1" title="Download">
          <i class="bi bi-download"></i>
        </a>
        <button class="btn btn-xs btn-ghost" onclick="closeVideo()">
          <i class="bi bi-x-lg"></i>
        </button>
      </div>
    </div>
    <video id="video-player" controls class="w-full max-h-[80vh] bg-black" preload="metadata">
      Your browser does not support HTML5 video.
    </video>
  </div>
  <form method="dialog" class="modal-backdrop"><button onclick="closeVideo()">close</button></form>
</dialog>

<!-- ── LOGS ──────────────────────────────────────────────── -->
<div id="page-logs" class="hidden">
  <div class="flex items-center gap-3 mb-3">
    <h2 class="text-base font-bold flex-1">System Logs</h2>
    <label class="flex items-center gap-2 text-xs">
      <input type="checkbox" id="log-autoscroll" class="checkbox checkbox-xs" checked/>
      Auto-scroll
    </label>
    <select id="log-level" class="select select-bordered select-xs" onchange="renderLogs()">
      <option value="">All levels</option>
      <option value="error">Errors only</option>
      <option value="warn">Warnings+</option>
    </select>
    <button class="btn btn-ghost btn-xs gap-1" onclick="clearLogs()"><i class="bi bi-trash3"></i>Clear</button>
    <button class="btn btn-ghost btn-xs gap-1" onclick="refreshLogs()"><i class="bi bi-arrow-clockwise"></i>Refresh</button>
  </div>
  <div id="log-view" class="bg-base-300 rounded-xl p-4 h-[60vh] overflow-y-auto font-mono text-xs leading-relaxed"></div>
  <div class="text-[10px] text-base-content/30 mt-2 text-right" id="log-count">0 lines</div>
</div>

<!-- ── SETTINGS ───────────────────────────────────────────── -->
<div id="page-settings" class="hidden max-w-2xl">
  <h2 class="text-base font-bold mb-5">Settings</h2>
  <p class="text-xs text-base-content/40 mb-4">Changes apply immediately and are saved to <code class="bg-base-300 px-1 rounded font-mono">flowarr.yaml</code> — they survive restarts.</p>

  <!-- Server (read-only) -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-server text-primary"></i>
      <span class="font-semibold text-sm">Server</span>
      <span id="fuse-status-badge" class="ml-auto"></span>
    </div>
    <div class="p-4 grid grid-cols-1 sm:grid-cols-2 gap-3">
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Listen Address</span><span class="label-text-alt text-[10px] opacity-40">read-only</span></div>
        <input id="cfg-addr" class="input input-bordered input-sm font-mono" readonly/>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Library Base Path</span><span class="label-text-alt text-[10px] opacity-40">read-only</span></div>
        <input id="cfg-library" class="input input-bordered input-sm font-mono" readonly/>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">FUSE Mount</span><span class="label-text-alt text-[10px] opacity-40">read-only</span></div>
        <input id="cfg-mount" class="input input-bordered input-sm font-mono" readonly/>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Data Directory</span><span class="label-text-alt text-[10px] opacity-40">read-only</span></div>
        <input id="cfg-data" class="input input-bordered input-sm font-mono" readonly/>
      </label>
    </div>
  </div>

  <!-- Decypharr / RD -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-lightning-charge-fill text-warning"></i>
      <span class="font-semibold text-sm">Decypharr (Debrid Gateway)</span>
      <span id="rd-status-badge" class="ml-auto text-xs opacity-50"></span>
    </div>
    <div class="p-4 grid gap-3">
      <p class="text-xs opacity-40 leading-relaxed">Flowarr forwards every magnet to Decypharr so RD caches it in parallel. Once cached, symlinks are atomically repointed from the FUSE mount to the RD mount.</p>
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">Decypharr URL</span></div>
          <input id="cfg-dc-url" class="input input-bordered input-sm font-mono" placeholder="http://host:8282"/>
        </label>
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">Mount Path</span></div>
          <input id="cfg-dc-mount" class="input input-bordered input-sm font-mono" placeholder="/mnt/decypharr"/>
        </label>
      </div>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Poll Interval</span></div>
        <input id="cfg-dc-poll" class="input input-bordered input-sm w-32" placeholder="30s"/>
      </label>
    </div>
  </div>

  <!-- Debrid API Keys -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-key-fill text-warning"></i>
      <span class="font-semibold text-sm">Debrid Provider API Keys</span>
    </div>
    <div class="p-4 grid gap-3">
      <p class="text-xs opacity-40 leading-relaxed">Used for direct provider operations (RD→TorBox transfer, mirror stats). Not needed for normal Flowarr/Decypharr operation — those use Decypharr's own credentials.</p>
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">Real-Debrid API Key</span></div>
          <input id="cfg-rd-key" class="input input-bordered input-sm font-mono" placeholder="AK4JTD…"/>
        </label>
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">TorBox API Key</span></div>
          <input id="cfg-tb-key" class="input input-bordered input-sm font-mono" placeholder="87129441…"/>
        </label>
      </div>
    </div>
  </div>

  <!-- Prowlarr -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-search text-info"></i>
      <span class="font-semibold text-sm">Prowlarr — Fallback Search</span>
    </div>
    <div class="p-4 grid gap-3">
      <p class="text-xs opacity-40 leading-relaxed">When a torrent has fewer seeds than the threshold, Flowarr queries Prowlarr for a healthier alternative and repoints symlinks transparently.</p>
      <div class="grid grid-cols-1 sm:grid-cols-2 gap-3">
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">Prowlarr URL</span></div>
          <input id="cfg-pl-url" class="input input-bordered input-sm font-mono" placeholder="http://host:9696"/>
        </label>
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">API Key</span></div>
          <input id="cfg-pl-key" class="input input-bordered input-sm font-mono" placeholder="..."/>
        </label>
      </div>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Restrict to Indexer IDs</span></div>
        <input id="cfg-pl-indexers" class="input input-bordered input-sm" placeholder="1,2,3  (empty = all indexers)"/>
        <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">Comma-separated Prowlarr indexer IDs to use for fallback search</span></div>
      </label>
    </div>
  </div>

  <!-- Arr Instances -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-collection-fill text-primary"></i>
      <span class="font-semibold text-sm">Arr Instances</span>
      <button class="btn btn-primary btn-xs ml-auto gap-1" onclick="openArrModal(-1)"><i class="bi bi-plus-lg"></i>Add</button>
    </div>
    <div class="overflow-x-auto">
      <table class="table table-sm">
        <thead class="bg-base-200 text-[11px] uppercase tracking-wider opacity-50">
          <tr><th>Name</th><th>URL</th><th>Categories</th><th></th></tr>
        </thead>
        <tbody id="arrs-tbody">
          <tr id="arrs-empty"><td colspan="4" class="text-center text-xs opacity-25 py-4">No arr instances configured</td></tr>
        </tbody>
      </table>
    </div>
    <div class="px-4 py-2 text-[10px] opacity-30">Stored for future Radarr/Sonarr interactive search and proactive import notification.</div>
  </div>

  <!-- Category Mappings -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-arrow-left-right text-secondary"></i>
      <span class="font-semibold text-sm">Category → Library Folder</span>
      <button class="btn btn-ghost btn-xs ml-auto gap-1" onclick="addMappingRow()"><i class="bi bi-plus-lg"></i>Add</button>
    </div>
    <div class="p-4">
      <p class="text-xs opacity-40 mb-3">Maps the download category sent by Radarr/Sonarr (e.g. <code class="bg-base-300 px-1 rounded">movies-hd</code>) to the folder Plex expects under the library base path (e.g. <code class="bg-base-300 px-1 rounded">Movies HD</code>).</p>
      <div id="mappings-list" class="space-y-2"></div>
    </div>
  </div>

  <!-- Streaming Tuning -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-sliders text-accent"></i>
      <span class="font-semibold text-sm">Streaming Tuning</span>
    </div>
    <div class="p-4 grid gap-3">
      <div class="grid grid-cols-1 sm:grid-cols-3 gap-3">
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">Seed Threshold</span></div>
          <input id="cfg-seed-threshold" class="input input-bordered input-sm" type="number" min="0" placeholder="5"/>
          <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">Min seeds before fallback</span></div>
        </label>
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">Health Check Delay</span></div>
          <input id="cfg-health-delay" class="input input-bordered input-sm" placeholder="45s"/>
          <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">Wait after metadata ready</span></div>
        </label>
        <label class="form-control">
          <div class="label py-0.5"><span class="label-text text-xs">Readahead (MB)</span></div>
          <input id="cfg-readahead" class="input input-bordered input-sm" type="number" min="1" placeholder="5"/>
          <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">Pieces pre-fetched ahead of Plex</span></div>
        </label>
      </div>
    </div>
  </div>

  <!-- Extra Trackers -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-4">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-broadcast text-success"></i>
      <span class="font-semibold text-sm">Extra Trackers</span>
    </div>
    <div class="p-4">
      <p class="text-xs opacity-40 mb-2">Additional announce URLs injected into every torrent alongside the 14 built-in trackers. One URL per line. Takes effect on next restart.</p>
      <textarea id="cfg-trackers" class="textarea textarea-bordered w-full font-mono text-xs h-24" placeholder="udp://tracker.example.com:6969/announce"></textarea>
    </div>
  </div>

  <!-- Save -->
  <div class="flex justify-end mb-4">
    <button class="btn btn-primary gap-2" onclick="saveSettings()"><i class="bi bi-floppy-fill"></i>Save All Settings</button>
  </div>

  <!-- About -->
  <div class="bg-base-100 border border-base-300 rounded-xl">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-info-circle opacity-40"></i>
      <span class="font-semibold text-sm">About</span>
    </div>
    <div class="p-4 text-xs opacity-40 leading-relaxed">
      Flowarr presents as qBittorrent to Radarr/Sonarr. Torrents are mounted via FUSE and streamed on-demand — only the pieces Plex actually reads are fetched from peers. When Decypharr confirms RD has cached the torrent, symlinks are atomically repointed to the RD mount.
      <div class="mt-3 font-mono">v<span id="cfg-ver">—</span></div>
    </div>
  </div>
</div>

<!-- ── SEEDER ──────────────────────────────────────────────── -->
<div id="page-seeder" class="hidden">
  <div class="flex items-center gap-3 mb-2 flex-wrap">
    <h2 class="text-base font-bold">Community Seeder &amp; Cacher</h2>
    <span id="seeder-enabled-badge" class="hidden badge badge-success badge-sm gap-1"><i class="bi bi-upload"></i>Active</span>
    <span id="seeder-disabled-badge" class="hidden badge badge-error badge-sm gap-1"><i class="bi bi-exclamation-circle"></i>Disabled — set seeder.enabled: true in flowarr.yaml and restart</span>
    <div class="ml-auto flex gap-2">
      <button id="seed-all-btn" class="hidden btn btn-success btn-xs gap-1" onclick="seedAll()" title="Add all debrid library items to the seeder queue"><i class="bi bi-upload"></i>Seed All from Debrid</button>
      <button class="btn btn-ghost btn-xs gap-1" onclick="resetUploadStats()" title="Reset session upload counters to zero"><i class="bi bi-eraser-fill"></i>Clear Stats</button>
      <button class="btn btn-ghost btn-xs gap-1" onclick="refreshSeeder()"><i class="bi bi-arrow-clockwise"></i>Refresh</button>
    </div>
  </div>
  <p class="text-xs text-base-content/40 mb-5 leading-relaxed max-w-2xl">Seeds your library back to the BitTorrent swarm (helping other users &amp; keeping peers alive) while simultaneously cycling through your debrid library to keep files warm in the cache — so content is always instantly available for streaming.</p>

  <!-- ── COMMUNITY SEEDER stats ─────────────────────────────── -->
  <div class="flex items-center gap-2 mb-3">
    <i class="bi bi-upload text-success"></i>
    <span class="text-sm font-semibold text-success">Community Seeder</span>
    <span class="text-xs text-base-content/40 ml-1">— uploading to the BitTorrent swarm</span>
  </div>
  <div class="grid grid-cols-3 sm:grid-cols-5 lg:grid-cols-10 gap-3 mb-5">
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Torrents currently uploading to peers. Limited by Max Active in settings.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Seeding</div>
      <div class="text-2xl font-bold text-success" id="sd-active">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Torrents loaded and ready to seed, waiting for a free slot. Will activate when a currently-seeding torrent is rotated out.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Queued</div>
      <div class="text-2xl font-bold text-warning" id="sd-queued">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Manually paused torrents. Click Resume in the torrent row to re-queue.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Paused</div>
      <div class="text-2xl font-bold text-base-content/40" id="sd-paused">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Resolving torrent metadata from DHT / trackers. These will move to Queued once resolved. Normal at startup — takes 1–5 minutes per torrent.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Verifying</div>
      <div class="text-2xl font-bold text-info" id="sd-verifying">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Total torrents in your debrid library known to the seeder (rotation queue + pending backlog).">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Library</div>
      <div class="text-2xl font-bold" id="sd-total">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Torrents waiting in the backlog — not yet loaded into the seeder rotation. They will be loaded gradually as slots open up.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Backlog</div>
      <div class="text-2xl font-bold text-base-content/30" id="sd-pending">0</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Current combined upload speed across all active torrents.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">↑ Speed</div>
      <div class="text-lg font-bold text-success" id="sd-upload-speed">—</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Total uploaded since the last time you clicked Clear Stats (or since Flowarr last restarted). Starts at zero and climbs as peers download pieces from you.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Session ↑</div>
      <div class="text-lg font-bold text-primary" id="sd-session-uploaded">—</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Total uploaded since each torrent was loaded into Flowarr this run. Resets to zero when Flowarr restarts — not persisted to disk.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">All-Time ↑</div>
      <div class="text-lg font-bold text-base-content/60" id="sd-uploaded">—</div>
    </div>
    <div class="bg-base-100 border border-base-300 rounded-xl p-3" title="Total peers connected across all active torrents right now.">
      <div class="text-[10px] uppercase tracking-wider text-base-content/40 mb-1">Peers</div>
      <div class="text-2xl font-bold" id="sd-peers">0</div>
    </div>
  </div>

  <!-- Live seeding box -->
  <div class="bg-base-100 border border-success/30 rounded-xl mb-5">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <span class="inline-block w-2 h-2 rounded-full bg-success animate-pulse"></span>
      <span class="font-semibold text-sm text-success">Now Seeding</span>
    </div>
    <div id="sd-live-empty" class="px-4 py-4 text-xs opacity-30 text-center">No torrents actively seeding</div>
    <div id="sd-live-list" class="divide-y divide-base-200"></div>
  </div>

  <!-- Settings card -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-5">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-sliders text-primary"></i>
      <span class="font-semibold text-sm">Seeder Settings</span>
      <button class="btn btn-primary btn-xs ml-auto gap-1" onclick="saveSeederSettings()"><i class="bi bi-floppy-fill"></i>Save</button>
    </div>
    <div class="p-4 grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Max Upload Speed (MB/s)</span></div>
        <input id="sd-cfg-upload" class="input input-bordered input-sm" type="number" min="0" placeholder="5"/>
        <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">0 = unlimited</span></div>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Max Concurrent Seeds</span></div>
        <input id="sd-cfg-max" class="input input-bordered input-sm" type="number" min="0" placeholder="5"/>
        <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">0 = seed everything</span></div>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Rotate After (hours)</span></div>
        <input id="sd-cfg-rotate" class="input input-bordered input-sm" type="number" min="0" step="0.5" placeholder="24"/>
        <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">0 = never rotate</span></div>
      </label>
      <div class="form-control justify-start gap-3 pt-1">
        <label class="label cursor-pointer gap-3 py-0.5">
          <span class="label-text text-xs">Allow Torrent Downloads</span>
          <input id="sd-cfg-downloads" type="checkbox" class="toggle toggle-primary toggle-sm"/>
        </label>
        <label class="label cursor-pointer gap-3 py-0.5">
          <span class="label-text text-xs">Prioritise Low-Seeder Torrents</span>
          <input id="sd-cfg-lowseeders" type="checkbox" class="toggle toggle-warning toggle-sm"/>
        </label>
      </div>
    </div>
    <!-- Year filter -->
    <div class="px-4 pb-4 grid grid-cols-2 gap-4 max-w-sm">
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Favour Year From</span></div>
        <input id="sd-cfg-yearfrom" class="input input-bordered input-sm" type="number" min="1900" max="2100" placeholder="e.g. 2010"/>
        <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">0 = all years</span></div>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Favour Year To</span></div>
        <input id="sd-cfg-yearto" class="input input-bordered input-sm" type="number" min="1900" max="2100" placeholder="e.g. 2024"/>
        <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-30">0 = all years</span></div>
      </label>
    </div>
    <div class="px-4 pb-3 text-[11px] opacity-40">Year filter and low-seeder priority only affect which queued torrents get activated next — they don't stop existing active seeds.</div>

    <!-- ── COMMUNITY CACHER settings ──────────────────────────── -->
    <div class="border-t-2 border-base-300 mt-1">
      <div class="px-4 pt-3 pb-1 flex items-center gap-2">
        <i class="bi bi-shield-shaded text-info"></i>
        <span class="text-sm font-semibold text-info">Community Cacher</span>
        <span class="text-xs text-base-content/40 ml-1">— keeps your debrid library warm</span>
      </div>
      <p class="px-4 pb-2 text-[11px] text-base-content/40 leading-relaxed">Rotates through your entire debrid library on a schedule, seeding each item until it meets the targets below, then moving on. When no peers are available it falls back to a direct cache-pull from the debrid mount. Once every item has been touched, the cycle resets automatically.</p>
      <!-- Mirror between providers -->
      <div class="px-4 pb-3 border border-info/20 rounded-xl mx-4 mb-2 bg-base-200/40">
        <div class="flex items-center gap-2 pt-3 pb-1">
          <i class="bi bi-arrow-left-right text-info"></i>
          <span class="text-xs font-semibold">Mirror library between providers</span>
        </div>
        <p class="text-[11px] text-base-content/40 pb-2 leading-relaxed">Submits all items from the source provider to a destination provider that doesn't have them yet. The community seeder uploads from the source FUSE mount — the destination downloads from those seeds to warm its cache. Run this once to sync existing libraries; new additions are forwarded automatically.</p>
        <div class="flex gap-2 items-center flex-wrap pb-3">
          <select id="mirror-src-select" class="select select-bordered select-xs">
            <option value="realdebrid">Real-Debrid</option>
            <option value="torbox">TorBox</option>
            <option value="alldebrid">AllDebrid</option>
            <option value="debridlink">DebridLink</option>
          </select>
          <i class="bi bi-arrow-right text-base-content/40"></i>
          <select id="mirror-dst-select" class="select select-bordered select-xs">
            <option value="torbox">TorBox</option>
            <option value="realdebrid">Real-Debrid</option>
            <option value="alldebrid">AllDebrid</option>
            <option value="debridlink">DebridLink</option>
          </select>
          <button id="mirror-btn" class="btn btn-info btn-xs gap-1" onclick="mirrorToProvider()"><i class="bi bi-arrow-repeat"></i>Mirror Now</button>
        </div>
        <div id="mirror-result" class="hidden mt-2 rounded-lg bg-base-300 px-3 py-2 text-xs flex flex-wrap gap-3 items-center"></div>
        <div id="transfer-result" class="hidden mt-2 rounded-lg bg-base-300 px-3 py-2 text-xs flex flex-wrap gap-3 items-center"></div>
      </div>
    </div>
    <!-- Provider seeding selection -->
    <div class="border-t border-base-300 px-4 py-3 flex flex-wrap items-center gap-4">
      <span class="text-xs font-semibold opacity-60">Seed items from:</span>
      <label class="label cursor-pointer gap-2 py-0">
        <input id="sd-prov-rd" type="checkbox" class="checkbox checkbox-sm checkbox-warning" onchange="saveSeederSettings()"/>
        <span class="label-text text-xs flex items-center gap-1"><i class="bi bi-database-fill-check text-warning"></i>Real-Debrid</span>
      </label>
      <label class="label cursor-pointer gap-2 py-0">
        <input id="sd-prov-tb" type="checkbox" class="checkbox checkbox-sm checkbox-info" onchange="saveSeederSettings()"/>
        <span class="label-text text-xs flex items-center gap-1"><i class="bi bi-cloud-fill text-info"></i>TorBox</span>
      </label>
      <span class="text-[10px] opacity-30">Checked providers' items are included in the seeder queue.</span>
    </div>
    <div class="border-t border-base-300">
      <div class="collapse collapse-arrow rounded-none">
        <input type="checkbox" id="sd-campaign-toggle" checked/>
        <div class="collapse-title text-sm font-medium py-3 px-4 min-h-0 flex items-center gap-2">
          <i class="bi bi-shield-check text-info"></i>
          Cache Cycle Settings
          <span class="text-[10px] font-normal opacity-40 ml-1">— per-torrent targets before rotating to next</span>
        </div>
        <div class="collapse-content px-4 pb-4">
          <p class="text-[10px] opacity-50 mt-1 mb-3">When active, each torrent reads data directly from Real-Debrid / TorBox via the FUSE mount and writes it to RAM (<code>/dev/shm</code>), then discards it. This forces the debrid provider to keep the torrent in their CDN cache — works even when no peers are downloading. After meeting the targets below, the seeder rotates to the next torrent.</p>
          <div class="grid grid-cols-2 sm:grid-cols-4 gap-3 pt-1 pb-1">
            <label class="form-control">
              <div class="label py-0.5"><span class="label-text text-xs" title="Minimum MB to read from debrid before rotating. 0 = use time only.">Min Read (MB)</span></div>
              <input id="sd-campaign-min-seed" class="input input-bordered input-sm" type="number" min="0" step="10" placeholder="500" onblur="saveSeederSettings()"/>
              <div class="label py-0"><span class="label-text-alt text-[10px] opacity-30">0 = time only</span></div>
            </label>
            <label class="form-control">
              <div class="label py-0.5"><span class="label-text text-xs" title="Hard cap: rotate after this many MB regardless of time.">Max Read (MB)</span></div>
              <input id="sd-campaign-max-seed" class="input input-bordered input-sm" type="number" min="0" step="50" placeholder="0" onblur="saveSeederSettings()"/>
              <div class="label py-0"><span class="label-text-alt text-[10px] opacity-30">Hard cap (0 = none)</span></div>
            </label>
            <label class="form-control">
              <div class="label py-0.5"><span class="label-text text-xs" title="Minimum minutes to stay on each torrent before rotating. 0 = use MB only.">Min Time (min)</span></div>
              <input id="sd-campaign-min-time" class="input input-bordered input-sm" type="number" min="0" step="1" placeholder="30" onblur="saveSeederSettings()"/>
              <div class="label py-0"><span class="label-text-alt text-[10px] opacity-30">0 = MB only</span></div>
            </label>
            <label class="form-control">
              <div class="label py-0.5"><span class="label-text text-xs" title="Hard cap: rotate after this many minutes regardless of MB read.">Max Time (min)</span></div>
              <input id="sd-campaign-max-time" class="input input-bordered input-sm" type="number" min="0" step="5" placeholder="0" onblur="saveSeederSettings()"/>
              <div class="label py-0"><span class="label-text-alt text-[10px] opacity-30">Hard cap (0 = none)</span></div>
            </label>
            <label class="form-control">
              <div class="label py-0.5"><span class="label-text text-xs" title="How many MB to read per pass. Flowarr reads this much, discards, then starts another pass if still in the slot.">Read per Pass (MB)</span></div>
              <input id="sd-campaign-chunk-size" class="input input-bordered input-sm" type="number" min="1" step="10" placeholder="100" onblur="saveSeederSettings()"/>
              <div class="label py-0"><span class="label-text-alt text-[10px] opacity-30">Per read pass</span></div>
            </label>
            <label class="form-control sm:col-span-2">
              <div class="label py-0.5"><span class="label-text text-xs" title="Where to buffer read data before discarding. Must be a RAM disk. Defaults to /dev/shm if left empty.">RAM Buffer Dir</span></div>
              <input id="sd-campaign-chunk-dir" class="input input-bordered input-sm font-mono" placeholder="/dev/shm" onblur="saveSeederSettings()"/>
              <div class="label py-0"><span class="label-text-alt text-[10px] opacity-30">RAM only — e.g. /dev/shm (default)</span></div>
            </label>
            <label class="form-control">
              <div class="label py-0.5"><span class="label-text text-xs" title="Fallback buffer dir if the RAM dir is unavailable. Use a USB drive path, not the main SSD.">Fallback Dir (USB)</span></div>
              <input id="sd-campaign-chunk-fallback" class="input input-bordered input-sm font-mono" placeholder="/mnt/usb" onblur="saveSeederSettings()"/>
              <div class="label py-0"><span class="label-text-alt text-[10px] opacity-30">Used if RAM dir unavailable</span></div>
            </label>
          </div>
          <div class="flex items-center gap-3 mt-3 pt-2 border-t border-base-300">
            <label class="label cursor-pointer gap-3 py-0">
              <span class="label-text text-xs">Loop automatically</span>
              <input id="sd-campaign-loop" type="checkbox" class="toggle toggle-info toggle-sm" checked onchange="saveSeederSettings()"/>
            </label>
            <span class="text-[10px] opacity-30">Restarts from the beginning after all torrents complete their slot — runs indefinitely</span>
          </div>
          <p class="text-[10px] opacity-30 mt-2">Set Min Read MB and/or Min Time, or use the per-provider cycle targets below. Works for both Real-Debrid and TorBox via the shared FUSE mount.</p>
        </div>
      </div>

      <!-- ── Per-provider cycle targets ─────────────────────── -->
      <div class="collapse collapse-arrow bg-base-200 border border-base-300 mt-2">
        <input type="checkbox"/>
        <div class="collapse-title text-sm font-semibold py-2 min-h-0 flex items-center gap-2">
          <i class="bi bi-arrow-repeat text-warning"></i> Provider Cache Cycle Targets
          <span class="badge badge-warning badge-xs ml-1">TorBox expires in 30 days</span>
        </div>
        <div class="collapse-content px-4 pb-4">
          <p class="text-[10px] opacity-50 mb-3">Set a target number of days to cycle through all items for each provider. Flowarr automatically calculates the optimal per-item slot time so every item gets warmed within the window. <strong>TorBox caches expire after 30 days</strong> — set 28 to stay safe. RD caches don't expire (set 0 to disable).</p>
          <div class="grid grid-cols-2 gap-4">
            <div class="bg-base-100 rounded-lg p-3 border border-warning/30">
              <div class="flex items-center gap-2 mb-2">
                <i class="bi bi-box-seam text-warning text-sm"></i>
                <span class="text-xs font-semibold">TorBox</span>
                <span class="badge badge-warning badge-xs">30-day expiry</span>
              </div>
              <label class="form-control">
                <div class="label py-0.5"><span class="label-text text-xs" title="Target days to cycle through all TorBox items. Recommended: 28 (gives 2 days buffer before 30-day expiry).">Cycle target (days)</span></div>
                <input id="sd-tb-cycle-days" class="input input-bordered input-sm" type="number" min="0" step="1" placeholder="28" onblur="saveSeederSettings()"/>
              </label>
              <div class="text-[10px] opacity-50 mt-2" id="sd-tb-calc-label">Enter a target to see calculated slot time</div>
            </div>
            <div class="bg-base-100 rounded-lg p-3 border border-primary/20">
              <div class="flex items-center gap-2 mb-2">
                <i class="bi bi-cloud-arrow-down text-primary text-sm"></i>
                <span class="text-xs font-semibold">Real-Debrid</span>
                <span class="badge badge-ghost badge-xs">no expiry</span>
              </div>
              <label class="form-control">
                <div class="label py-0.5"><span class="label-text text-xs" title="Target days to cycle through all RD items. Optional — RD caches don't expire. Set 0 to disable.">Cycle target (days)</span></div>
                <input id="sd-rd-cycle-days" class="input input-bordered input-sm" type="number" min="0" step="1" placeholder="0 (disabled)" onblur="saveSeederSettings()"/>
              </label>
              <div class="text-[10px] opacity-50 mt-2" id="sd-rd-calc-label">0 = use global Min Time setting</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>

  <!-- ── COMMUNITY CACHER stats ────────────────────────────── -->
  <div class="flex items-center gap-2 mb-3 mt-4">
    <i class="bi bi-shield-shaded text-info"></i>
    <span class="text-sm font-semibold text-info">Cache Cycle Health</span>
    <button class="btn btn-ghost btn-xs gap-1 ml-auto" onclick="resetCycle()" title="Reset all cycle progress and restart from the beginning"><i class="bi bi-arrow-counterclockwise"></i>Reset cycle</button>
  </div>
  <div id="sd-campaign-progress-card" class="hidden bg-base-100 border border-info/20 rounded-xl mb-5">
    <div class="grid grid-cols-1 sm:grid-cols-2 divide-y sm:divide-y-0 sm:divide-x divide-base-300">
      <!-- TorBox -->
      <div class="px-4 py-3">
        <div class="flex items-center gap-2 mb-2">
          <i class="bi bi-box-seam text-warning text-sm"></i>
          <span class="text-xs font-semibold">TorBox</span>
          <span class="text-[10px] font-mono text-warning ml-auto" id="sd-tb-cycle-label">— / —</span>
        </div>
        <progress id="sd-tb-cycle-bar" class="progress progress-warning w-full h-2 mb-1" value="0" max="100"></progress>
        <div class="text-[10px] opacity-40" id="sd-tb-cycle-eta"></div>
      </div>
      <!-- RD -->
      <div class="px-4 py-3">
        <div class="flex items-center gap-2 mb-2">
          <i class="bi bi-cloud-arrow-down text-primary text-sm"></i>
          <span class="text-xs font-semibold">Real-Debrid</span>
          <span class="text-[10px] font-mono text-primary ml-auto" id="sd-rd-cycle-label">— / —</span>
        </div>
        <progress id="sd-rd-cycle-bar" class="progress progress-primary w-full h-2 mb-1" value="0" max="100"></progress>
        <div class="text-[10px] opacity-40" id="sd-rd-cycle-eta"></div>
      </div>
    </div>
    <div class="px-4 py-2 border-t border-base-300 bg-base-200/50 rounded-b-xl text-[10px] opacity-40">
      Seeder announces all active items to the BitTorrent network. When peers connect, items are uploaded automatically. Items with few seeders are prioritised when <em>Prioritise low-seeder torrents</em> is enabled.
    </div>
  </div>

  <!-- Manual add -->
  <div class="bg-base-100 border border-base-300 rounded-xl mb-5">
    <div class="px-4 py-3 border-b border-base-300 flex items-center gap-2">
      <i class="bi bi-magnet text-secondary"></i>
      <span class="font-semibold text-sm">Add Magnet Manually</span>
    </div>
    <div class="p-4 flex gap-2">
      <input id="sd-magnet-input" class="input input-bordered input-sm flex-1 font-mono text-xs" placeholder="magnet:?xt=urn:btih:..."/>
      <button class="btn btn-secondary btn-sm gap-1" onclick="seederAddMagnet()"><i class="bi bi-plus-lg"></i>Add</button>
    </div>
  </div>

  <!-- Per-provider collapsible queue sections -->
  <div class="flex flex-wrap gap-2 mb-3 items-center">
    <i class="bi bi-collection text-base-content/40 text-sm"></i>
    <span class="text-sm font-semibold">Rotation Queue</span>
    <span class="text-[10px] text-base-content/40" title="The table shows torrents currently loaded in the seeder. The full library backlog (shown in the Backlog stat above) loads gradually in the background.">— active seeder slots · full library loads gradually from backlog</span>
    <input id="sd-filter" class="input input-bordered input-xs w-40 ml-auto" placeholder="Filter by name…" oninput="renderSeederTable()"/>
    <select id="sd-filter-state" class="select select-bordered select-xs" onchange="renderSeederTable()">
      <option value="">All states</option>
      <option value="seeding">Seeding</option>
      <option value="queued">Queued</option>
      <option value="paused">Paused</option>
      <option value="verifying">Verifying</option>
    </select>
  </div>
  <div id="sd-queue-sections" class="flex flex-col gap-3"></div>
</div>

<!-- Arr instance modal -->
<dialog id="arr-modal" class="modal">
  <div class="modal-box max-w-md">
    <h3 class="font-bold text-base mb-4" id="arr-modal-title">Add Arr Instance</h3>
    <input type="hidden" id="arr-edit-idx" value="-1"/>
    <div class="grid gap-3">
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Name</span></div>
        <input id="arr-name" class="input input-bordered input-sm" placeholder="Radarr HD"/>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">URL</span></div>
        <input id="arr-url" class="input input-bordered input-sm font-mono" placeholder="http://192.168.4.8:7878"/>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">API Key</span></div>
        <input id="arr-apikey" class="input input-bordered input-sm font-mono" placeholder="..."/>
      </label>
      <label class="form-control">
        <div class="label py-0.5"><span class="label-text text-xs">Categories</span></div>
        <input id="arr-cats" class="input input-bordered input-sm" placeholder="movies-hd, movies-4k"/>
        <div class="label py-0.5"><span class="label-text-alt text-[10px] opacity-40">Comma-separated category names this instance manages</span></div>
      </label>
    </div>
    <div class="modal-action">
      <button class="btn btn-ghost btn-sm" onclick="document.getElementById('arr-modal').close()">Cancel</button>
      <button class="btn btn-primary btn-sm" onclick="saveArrInstance()">Save</button>
    </div>
  </div>
  <form method="dialog" class="modal-backdrop"><button>close</button></form>
</dialog>

</main>

<script>
const PAGES = ['dashboard','downloads','browse','logs','seeder','settings'];
let lastQueue = [];
let allLogs = [];
let browseCurrentPath = '';
let logsPaused = false;

// ── Navigation ──────────────────────────────────────────────
function nav(page, pushHash) {
  if (!PAGES.includes(page)) page = 'dashboard';
  PAGES.forEach(p => {
    document.getElementById('page-'+p).classList.toggle('hidden', p !== page);
    const t = document.getElementById('tab-'+p);
    if (t) {
      t.classList.toggle('bg-primary/10', p === page);
      t.classList.toggle('text-primary', p === page);
      t.classList.toggle('font-semibold', p === page);
    }
  });
  if (pushHash !== false) history.replaceState(null, '', '#'+page);
  if (page === 'settings') loadSettings();
  if (page === 'seeder') { refreshSeeder(); }
  if (page === 'browse' && browseCurrentPath === '') initBrowse();
  if (page === 'logs') refreshLogs();
}
window.addEventListener('hashchange', () => {
  const page = location.hash.slice(1);
  nav(page, false);
});

function toggleTheme() {
  const cur = document.documentElement.getAttribute('data-theme');
  const next = cur === 'dark' ? 'light' : 'dark';
  document.documentElement.setAttribute('data-theme', next);
  localStorage.setItem('theme', next);
}

// ── Toast ───────────────────────────────────────────────────
function toast(msg, type='info') {
  const el = document.createElement('div');
  el.className = 'alert alert-'+type+' py-2 px-4 text-sm shadow pointer-events-auto';
  el.innerHTML = msg;
  document.getElementById('toast').appendChild(el);
  setTimeout(()=>el.remove(), 3500);
}

// ── Formatters ──────────────────────────────────────────────
function fmtBytes(n) {
  if (!n || n <= 0) return '—';
  const u = ['B','KB','MB','GB','TB']; let i = 0;
  while (n >= 1024 && i < u.length-1) { n /= 1024; i++; }
  return n.toFixed(1)+' '+u[i];
}
function fmtSpeed(b) { return b > 0 ? fmtBytes(b)+'/s' : '—'; }
function fmtETA(s) {
  if (!s || s <= 0) return '—';
  if (s < 60) return s+'s';
  if (s < 3600) return Math.round(s/60)+'m';
  return (s/3600).toFixed(1)+'h';
}
function fmtPct(p) { return ((p||0)*100).toFixed(1)+'%'; }

function srcBadge(s) {
  if (s==='decypharr') return '<span class="badge badge-xs badge-warning gap-0.5"><i class="bi bi-lightning-charge-fill"></i>RD</span>';
  if (s==='both')      return '<span class="badge badge-xs badge-info gap-0.5"><i class="bi bi-layers-fill"></i>BT+RD</span>';
  return '<span class="badge badge-xs badge-ghost gap-0.5"><i class="bi bi-magnet-fill"></i>BT</span>';
}
function stateBadge(s) {
  if (!s) return '<span class="badge badge-xs">—</span>';
  if (s==='uploading'||s==='stalledUP') return '<span class="badge badge-xs badge-success">Cached</span>';
  if (s==='downloading') return '<span class="badge badge-xs badge-primary">Downloading</span>';
  return '<span class="badge badge-xs badge-warning">'+s+'</span>';
}
function pctColor(s) {
  if (s==='uploading'||s==='stalledUP') return 'progress-success';
  if (s==='downloading') return 'progress-primary';
  return 'progress-warning';
}

// ── Dashboard ────────────────────────────────────────────────
const PROVIDER_COLORS = {
  realdebrid: {badge:'badge-warning', icon:'bi-lightning-charge-fill', label:'Real-Debrid', pill:'badge-warning'},
  torbox:     {badge:'badge-info',    icon:'bi-box-fill',               label:'TorBox',      pill:'badge-info'},
  alldebrid:  {badge:'badge-accent',  icon:'bi-stars',                  label:'AllDebrid',   pill:'badge-accent'},
  debridlink: {badge:'badge-secondary',icon:'bi-link-45deg',            label:'DebridLink',  pill:'badge-secondary'},
};
function providerMeta(name) {
  const key = (name||'').toLowerCase().replace(/[^a-z]/g,'');
  return PROVIDER_COLORS[key] || {badge:'badge-ghost', icon:'bi-cloud-fill', label:name||'Debrid', pill:'badge-ghost'};
}

// Panel collapse state stored in localStorage
const _panelCollapsed = {};
function togglePanel(key) {
  _panelCollapsed[key] = !_panelCollapsed[key];
  localStorage.setItem('panel-collapsed-'+key, _panelCollapsed[key] ? '1' : '0');
  _applyPanelCollapse(key);
}
function _applyPanelCollapse(key) {
  const listEl = document.getElementById(key==='bt' ? 'bt-list' : 'dc-list-'+key);
  const chevEl = document.getElementById(key==='bt' ? 'bt-chevron' : 'dc-chevron-'+key);
  if (listEl) listEl.classList.toggle('hidden', !!_panelCollapsed[key]);
  if (chevEl) chevEl.style.transform = _panelCollapsed[key] ? 'rotate(-90deg)' : '';
}
function _initPanelCollapse(key) {
  _panelCollapsed[key] = localStorage.getItem('panel-collapsed-'+key) === '1';
  _applyPanelCollapse(key);
}

function renderDashboard(items) {
  let dl=0,up=0,stall=0,spd=0,seeds=0;
  const byProvider={}, btItems=[];
  (items||[]).forEach(t => {
    const s = t.state||'';
    if (s==='downloading') dl++;
    else if (s==='uploading') up++;
    else stall++;
    spd += t.dlspeed||0;
    seeds += t.num_seeds||0;
    if (t.source==='decypharr'||t.source==='both') {
      const key = (t.debrid||'debrid').toLowerCase();
      if (!byProvider[key]) byProvider[key]=[];
      byProvider[key].push(t);
    }
    if (t.source==='flowarr'||t.source==='both') btItems.push(t);
  });

  document.getElementById('s-dl').textContent = dl;
  document.getElementById('s-up').textContent = up;
  document.getElementById('s-stall').textContent = stall;
  document.getElementById('s-total').textContent = (items||[]).length;
  document.getElementById('s-speed').textContent = fmtSpeed(spd);
  document.getElementById('s-seeds').textContent = seeds;
  document.getElementById('bt-count').textContent = btItems.length;

  // Per-provider stats row
  const statsRow = document.getElementById('provider-stats-row');
  const provKeys = Object.keys(byProvider);
  if (statsRow && provKeys.length > 0) {
    statsRow.classList.remove('hidden');
    statsRow.classList.add('flex');
    statsRow.innerHTML = provKeys.map(k => {
      const m = providerMeta(k);
      const provItems = byProvider[k];
      const pDl    = provItems.filter(t=>t.state==='downloading').length;
      const pUp    = provItems.filter(t=>t.state==='uploading'||t.state==='stalledUP').length;
      const pSpd   = provItems.reduce((a,t)=>a+(t.dlspeed||0),0);
      return '<div class="bg-base-100 border border-base-300 rounded-xl p-3 flex-1 min-w-[140px]">'
        +'<div class="flex items-center gap-1.5 mb-2">'
        +'<i class="bi '+m.icon+' text-xs '+m.pill.replace('badge-','text-')+'"></i>'
        +'<span class="text-[11px] font-semibold">'+escHtml(m.label)+'</span>'
        +'</div>'
        +'<div class="grid grid-cols-3 gap-1">'
        +'<div class="text-center"><div class="text-lg font-bold text-primary">'+pDl+'</div><div class="text-[9px] uppercase opacity-40">DL</div></div>'
        +'<div class="text-center"><div class="text-lg font-bold text-success">'+pUp+'</div><div class="text-[9px] uppercase opacity-40">Cached</div></div>'
        +'<div class="text-center"><div class="text-sm font-bold text-accent">'+fmtSpeed(pSpd)+'</div><div class="text-[9px] uppercase opacity-40">Speed</div></div>'
        +'</div>'
        +'</div>';
    }).join('');
  } else if (statsRow) {
    statsRow.classList.add('hidden');
    statsRow.classList.remove('flex');
  }

  // Render per-provider pills in top bar
  const pillsEl = document.getElementById('debrid-pills');
  if (pillsEl) {
    pillsEl.innerHTML = provKeys.map(k => {
      const m = providerMeta(k);
      return '<span class="badge badge-sm '+m.pill+' gap-1"><i class="bi '+m.icon+' text-[10px]"></i>'+m.label+'</span>';
    }).join('');
  }

  // Render one collapsible debrid panel per provider, keeping BT panel last
  const panelsEl = document.getElementById('source-panels');
  const btPanel = document.getElementById('bt-panel');
  panelsEl.querySelectorAll('.debrid-panel').forEach(el=>el.remove());

  const renderList = (el, list) => {
    if (!list.length) {
      el.innerHTML = '<div class="text-center text-base-content/25 py-6 text-xs">Nothing here</div>';
      return;
    }
    el.innerHTML = list.map(t => {
      const pct = ((t.progress||0)*100);
      return '<div class="px-4 py-2.5 flex items-center gap-3 hover:bg-base-200/50 transition-colors">'
        +'<div class="flex-1 min-w-0">'
        +'<div class="text-sm truncate font-medium" title="'+escHtml(t.name||'…')+'">'+escHtml(t.name||'…')+'</div>'
        +'<div class="flex items-center gap-2 mt-1">'
        +'<progress class="progress '+pctColor(t.state)+' h-1 flex-1" value="'+pct+'" max="100"></progress>'
        +'<span class="text-[10px] text-base-content/40 w-9 text-right">'+pct.toFixed(0)+'%</span>'
        +'</div>'
        +'</div>'
        +'<div class="flex flex-col items-end gap-1 flex-shrink-0">'
        +stateBadge(t.state)
        +(t.dlspeed>0?'<span class="text-[10px] text-accent">'+fmtSpeed(t.dlspeed)+'</span>':'')
        +'</div>'
        +'</div>';
    }).join('');
  };

  provKeys.forEach(key => {
    const provItems = byProvider[key];
    const m = providerMeta(key);
    const card = document.createElement('div');
    card.className = 'bg-base-100 border border-base-300 rounded-xl debrid-panel';
    const countId   = 'dc-count-'+key;
    const listId    = 'dc-list-'+key;
    const chevId    = 'dc-chevron-'+key;
    card.innerHTML =
      '<div class="flex items-center gap-2 px-4 py-3 border-b border-base-300 cursor-pointer select-none" onclick="togglePanel(\''+key+'\')">'
      +'<i class="bi '+m.icon+' text-warning"></i>'
      +'<span class="font-semibold text-sm">'+escHtml(m.label)+'</span>'
      +'<span class="badge badge-sm '+m.badge+' ml-auto" id="'+countId+'">'+provItems.length+'</span>'
      +'<i class="bi bi-chevron-down text-xs ml-1 transition-transform" id="'+chevId+'"></i>'
      +'</div>'
      +'<div id="'+listId+'" class="divide-y divide-base-300 max-h-64 overflow-y-auto"></div>';
    panelsEl.insertBefore(card, btPanel);
    renderList(card.querySelector('#'+listId), provItems);
    _initPanelCollapse(key);
  });

  renderList(document.getElementById('bt-list'), btItems);
  _initPanelCollapse('bt');
}

// ── Downloads table ──────────────────────────────────────────
// ── Downloads — collapsible per-provider sections ─────────────
const _dlCollapsed = {};

function _dlToggleSection(key) {
  _dlCollapsed[key] = !_dlCollapsed[key];
  localStorage.setItem('dl-collapsed-'+key, _dlCollapsed[key] ? '1' : '0');
  _dlApplyCollapse(key);
}
function _dlApplyCollapse(key) {
  const body = document.getElementById('dl-body-'+key);
  const chev = document.getElementById('dl-chev-'+key);
  if (body) body.classList.toggle('hidden', !!_dlCollapsed[key]);
  if (chev) chev.style.transform = _dlCollapsed[key] ? 'rotate(-90deg)' : '';
}
function _dlRowHtml(t) {
  const pct = ((t.progress||0)*100);
  return '<tr class="hover:bg-base-200/40 transition-colors">'
    +'<td class="pl-4 max-w-xs"><div class="text-sm font-medium truncate" title="'+escHtml(t.name||'...')+'">'+escHtml(t.name||'...')+'</div>'
    +(t.save_path?'<div class="text-[10px] text-base-content/30 truncate font-mono">'+escHtml(t.save_path)+'</div>':'')
    +'</td>'
    +'<td>'+stateBadge(t.state)+'</td>'
    +'<td><div class="flex items-center gap-1.5"><progress class="progress '+pctColor(t.state)+' h-1.5 w-20" value="'+pct+'" max="100"></progress><span class="text-xs">'+pct.toFixed(0)+'%</span></div></td>'
    +'<td class="text-xs text-base-content/60">'+fmtBytes(t.downloaded)+'</td>'
    +'<td class="text-xs text-base-content/60">'+fmtBytes(t.size)+'</td>'
    +'<td class="text-xs text-accent font-medium">'+fmtSpeed(t.dlspeed)+'</td>'
    +'<td class="text-xs text-base-content/60">'+fmtETA(t.eta)+'</td>'
    +'<td class="text-xs text-base-content/60">'+(t.num_seeds||0)+'</td>'
    +'<td>'+(t.category?'<span class="badge badge-xs badge-ghost">'+escHtml(t.category)+'</span>':'')+'</td>'
    +'<td><button class="btn btn-ghost btn-xs text-error" onclick="delTorrent(\''+t.hash+'\')"><i class="bi bi-trash3"></i></button></td>'
    +'</tr>';
}

function renderDownloads() {
  const search = (document.getElementById('dl-search')?.value||'').toLowerCase();
  const stateF = document.getElementById('dl-state')?.value||'';

  // Bucket items into provider groups
  const groups = {};
  const addGroup = (key, meta, t) => {
    if (!groups[key]) groups[key] = {meta, items:[]};
    groups[key].items.push(t);
  };
  (lastQueue||[]).forEach(t => {
    if (search && !(t.name||'').toLowerCase().includes(search)) return;
    if (stateF) {
      if (stateF==='uploading' && t.state!=='uploading'&&t.state!=='stalledUP') return;
      if (stateF!=='uploading' && t.state!==stateF) return;
    }
    if (t.source==='flowarr'||t.source==='both') {
      addGroup('bt', {icon:'bi-magnet-fill',label:'BitTorrent',iconColor:'text-success'}, t);
    }
    if (t.source==='decypharr'||t.source==='both') {
      const k = (t.debrid||'debrid').toLowerCase();
      const m = providerMeta(k);
      addGroup(k, {icon:m.icon, label:m.label, iconColor:'text-warning'}, t);
    }
  });

  const sectionsEl = document.getElementById('dl-sections');
  const emptyEl   = document.getElementById('dl-empty');
  if (!sectionsEl) return;
  const keys = Object.keys(groups).sort((a,b)=>(a==='bt'?1:-1)-(b==='bt'?1:-1));
  if (!keys.length) { sectionsEl.innerHTML=''; emptyEl.classList.remove('hidden'); return; }
  emptyEl.classList.add('hidden');

  // Remove stale sections
  [...sectionsEl.querySelectorAll('[data-dlsec]')].forEach(el => {
    if (!groups[el.dataset.dlsec]) el.remove();
  });

  const thead = '<thead class="bg-base-200"><tr class="text-[11px] uppercase tracking-wider text-base-content/50">'
    +_th('Name','name',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('State','state',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('Progress','progress',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('Downloaded','downloaded',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('Size','size',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('Speed','dlspeed',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('ETA','eta',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('Seeds','num_seeds',_dlSortCol,_dlSortDir,'_dlSort')
    +_th('Category','category',_dlSortCol,_dlSortDir,'_dlSort')
    +'<th></th>'
    +'</tr></thead>';

  keys.forEach(key => {
    const {meta, items} = groups[key];
    if (!(key in _dlCollapsed)) {
      _dlCollapsed[key] = localStorage.getItem('dl-collapsed-'+key) !== '0';
    }
    const collapsed = !!_dlCollapsed[key];
    let card = sectionsEl.querySelector('[data-dlsec="'+key+'"]');
    if (!card) {
      card = document.createElement('div');
      card.dataset.dlsec = key;
      card.className = 'bg-base-100 border border-base-300 rounded-xl overflow-hidden';
      sectionsEl.appendChild(card);
    }
    const sortedDlItems = _dlSortItems(items);
    card.innerHTML =
      '<div class="flex items-center gap-2 px-4 py-3 border-b border-base-300 cursor-pointer select-none" onclick="_dlToggleSection(\''+key+'\')">'
      +'<i class="bi '+meta.icon+' '+meta.iconColor+'"></i>'
      +'<span class="font-semibold text-sm">'+escHtml(meta.label)+'</span>'
      +'<span class="badge badge-xs ml-1">'+items.length+'</span>'
      +'<i id="dl-chev-'+key+'" class="bi bi-chevron-down ml-auto text-base-content/40 transition-transform" style="transform:'+(collapsed?'rotate(-90deg)':'')+'"></i>'
      +'</div>'
      +'<div id="dl-body-'+key+'" class="overflow-x-auto'+(collapsed?' hidden':'')+'"><table class="table table-sm table-pin-rows">'
      +thead+'<tbody>'+sortedDlItems.map(_dlRowHtml).join('')+'</tbody></table></div>';
  });
}

// ── Queue fetch ──────────────────────────────────────────────
async function refresh() {
  try {
    const r = await fetch('/api/flowarr/queue');
    lastQueue = (await r.json())||[];
    renderDashboard(lastQueue);
    renderDownloads();
  } catch(e) { console.error(e); }
}

// ── Add / Remove ─────────────────────────────────────────────
async function addTorrent() {
  const urls = document.getElementById('magnet-in').value.trim();
  if (!urls) return;
  const body = new URLSearchParams({urls});
  const sp = document.getElementById('savepath-in').value.trim();
  const cat = document.getElementById('cat-in').value.trim();
  if (sp) body.set('savepath', sp);
  if (cat) body.set('category', cat);
  const res = await fetch('/api/v2/torrents/add', {method:'POST', body});
  const txt = await res.text();
  if (txt.startsWith('Ok')) {
    document.getElementById('magnet-in').value='';
    toast('<i class="bi bi-check-circle-fill"></i> Added — forwarding to RD/Decypharr','success');
    setTimeout(refresh, 1200);
  } else {
    toast('<i class="bi bi-exclamation-triangle-fill"></i> Failed: '+txt,'error');
  }
}
async function addTorrent2() {
  const urls = document.getElementById('magnet-in2').value.trim();
  if (!urls) return;
  const body = new URLSearchParams({urls});
  const sp = document.getElementById('savepath-in2').value.trim();
  const cat = document.getElementById('cat-in2').value.trim();
  if (sp) body.set('savepath',sp);
  if (cat) body.set('category',cat);
  await fetch('/api/v2/torrents/add',{method:'POST',body});
  document.getElementById('magnet-in2').value='';
  toast('<i class="bi bi-check-circle-fill"></i> Added','success');
  setTimeout(refresh,1200);
}
async function delTorrent(hash) {
  if (!confirm('Remove this torrent?')) return;
  await fetch('/api/v2/torrents/delete',{method:'POST',body:new URLSearchParams({hashes:hash,deleteFiles:'false'})});
  toast('<i class="bi bi-trash3-fill"></i> Removed','warning');
  setTimeout(refresh,400);
}

// ── Browse ───────────────────────────────────────────────────
const VIDEO_EXTS = new Set(['.mp4','.m4v','.mov','.mkv','.webm','.avi','.ts','.m2ts']);

function isVideo(name) {
  const dot = name.lastIndexOf('.');
  return dot >= 0 && VIDEO_EXTS.has(name.slice(dot).toLowerCase());
}

function streamUrl(path, download) {
  return '/api/flowarr/stream?path='+encodeURIComponent(path)+(download?'&download=1':'');
}

function openVideo(path, name) {
  const player = document.getElementById('video-player');
  player.pause();
  player.src = streamUrl(path, false);
  document.getElementById('video-title').textContent = name;
  const dlLink = document.getElementById('video-dl-link');
  dlLink.href = streamUrl(path, true);
  dlLink.setAttribute('download', name);
  document.getElementById('video-modal').showModal();
  player.load();
}

function closeVideo() {
  const player = document.getElementById('video-player');
  player.pause();
  player.src = '';
  document.getElementById('video-modal').close();
}

async function initBrowse() {
  try {
    const r = await fetch('/api/flowarr/browse');
    if (!r.ok) { showBrowseError(await r.text()); return; }
    const data = await r.json();
    browseCurrentPath = data.path;
    renderRoots(data.roots||[]);
    renderBrowse(data);
  } catch(e) { showBrowseError(e.message); }
}

async function browse(path) {
  try {
    const r = await fetch('/api/flowarr/browse?path='+encodeURIComponent(path));
    if (!r.ok) { showBrowseError(await r.text()); return; }
    const data = await r.json();
    browseCurrentPath = data.path;
    renderBrowse(data);
  } catch(e) { showBrowseError(e.message); }
}

function renderRoots(roots) {
  document.getElementById('browse-roots').innerHTML = roots.map(r =>
    '<button class="btn btn-xs btn-outline gap-1" onclick="browse(\''+escAttr(r)+'\')">'
    +'<i class="bi bi-hdd-rack text-xs"></i>'+escHtml(r)+'</button>'
  ).join('');
}

function renderBrowse(data) {
  document.getElementById('browse-error').classList.add('hidden');
  const items = data.items||[];
  // Breadcrumb
  const parts = data.path.split('/').filter(Boolean);
  let crumbs = '<li><a onclick="browse(\'/\')"><i class="bi bi-house-fill text-xs"></i></a></li>';
  let acc = '';
  parts.forEach((p,i) => {
    acc += '/'+p;
    const path = acc;
    if (i < parts.length-1) {
      crumbs += '<li><a onclick="browse(\''+escAttr(path)+'\')">'+escHtml(p)+'</a></li>';
    } else {
      crumbs += '<li><span class="text-base-content/60">'+escHtml(p)+'</span></li>';
    }
  });
  document.getElementById('breadcrumb').innerHTML = crumbs;

  // Stats
  const statsEl = document.getElementById('browse-stats');
  const dirs = items.filter(i => i.is_dir);
  const files = items.filter(i => !i.is_dir);
  const videos = files.filter(i => isVideo(i.name));
  const totalSize = files.reduce((s, i) => s + (i.size||0), 0);
  if (items.length) {
    document.querySelector('#stat-folders span').textContent = dirs.length + ' folder' + (dirs.length===1?'':'s');
    document.querySelector('#stat-files span').textContent = files.length + ' file' + (files.length===1?'':'s');
    document.querySelector('#stat-videos span').textContent = videos.length + ' video' + (videos.length===1?'':'s');
    document.querySelector('#stat-size span').textContent = fmtBytes(totalSize);
    document.getElementById('stat-videos').classList.toggle('hidden', videos.length===0);
    statsEl.classList.remove('hidden');
  } else {
    statsEl.classList.add('hidden');
  }

  const list = document.getElementById('browse-list');
  const empty = document.getElementById('browse-empty');
  if (!items.length) { list.innerHTML=''; empty.classList.remove('hidden'); return; }
  empty.classList.add('hidden');

  let html = '';
  if (data.parent && data.parent !== data.path) {
    html += '<div class="flex items-center gap-3 px-4 py-2.5 hover:bg-base-200/50 cursor-pointer transition-colors" onclick="browse(\''+escAttr(data.parent)+'\')">'
      +'<i class="bi bi-arrow-up-circle text-base-content/40 text-lg w-5"></i>'
      +'<span class="text-sm text-base-content/50">..</span>'
      +'</div>';
  }
  html += items.map(it => {
    if (it.is_dir) {
      return '<div class="flex items-center gap-3 px-4 py-2.5 hover:bg-base-200/50 cursor-pointer transition-colors" onclick="browse(\''+escAttr(it.path)+'\')">'
        +'<i class="bi bi-folder-fill text-warning text-lg w-5"></i>'
        +'<span class="text-sm flex-1 truncate" title="'+escHtml(it.path)+'">'+escHtml(it.name)+'</span>'
        +'<i class="bi bi-chevron-right text-base-content/20 text-xs"></i>'
        +'</div>';
    }
    const vid = isVideo(it.name);
    const icon = vid
      ? '<i class="bi bi-play-circle-fill text-primary text-lg w-5"></i>'
      : '<i class="bi bi-file-earmark text-base-content/40 text-lg w-5"></i>';
    const mainAction = vid
      ? 'onclick="openVideo(\''+escAttr(it.path)+'\',\''+escAttr(it.name)+'\')"'
      : 'onclick="location.href=\''+escAttr(streamUrl(it.path,true))+'\'"';
    const dlBtn = vid
      ? '<a href="'+escAttr(streamUrl(it.path,true))+'" download="'+escAttr(it.name)+'" onclick="event.stopPropagation()" class="btn btn-xs btn-ghost opacity-0 group-hover:opacity-100 transition-opacity" title="Download"><i class="bi bi-download"></i></a>'
      : '';
    return '<div class="group flex items-center gap-3 px-4 py-2.5 hover:bg-base-200/50 cursor-pointer transition-colors" '+mainAction+'>'
      +icon
      +'<span class="text-sm flex-1 truncate" title="'+escHtml(it.path)+'">'+escHtml(it.name)+'</span>'
      +dlBtn
      +'<span class="text-xs text-base-content/30 font-mono">'+fmtBytes(it.size)+'</span>'
      +'</div>';
  }).join('');
  list.innerHTML = html;
}

function showBrowseError(msg) {
  document.getElementById('browse-list').innerHTML='';
  document.getElementById('browse-empty').classList.add('hidden');
  const errEl = document.getElementById('browse-error');
  document.getElementById('browse-error-msg').textContent = msg;
  errEl.classList.remove('hidden');
}

// ── Logs ─────────────────────────────────────────────────────
let localLogs = [];

async function refreshLogs() {
  try {
    const r = await fetch('/api/flowarr/logs');
    localLogs = (await r.json())||[];
    renderLogs();
  } catch(e) {}
}

function renderLogs() {
  const level = document.getElementById('log-level')?.value||'';
  const view = document.getElementById('log-view');
  const filtered = localLogs.filter(l => {
    const low = l.toLowerCase();
    if (level==='error') return low.includes('error')||low.includes('fatal');
    if (level==='warn') return low.includes('error')||low.includes('fatal')||low.includes('warn');
    return true;
  });
  document.getElementById('log-count').textContent = filtered.length+' lines';
  view.innerHTML = filtered.map(l => {
    const low = l.toLowerCase();
    let cls = '';
    if (low.includes('error')||low.includes('fatal')) cls='log-error';
    else if (low.includes('warn')) cls='log-warn';
    else if (low.includes('fuse')||low.includes('mount')||low.includes('start')) cls='log-info';
    else cls='log-debug';
    return '<div class="'+cls+' whitespace-pre-wrap break-all">'+escHtml(l)+'</div>';
  }).join('');
  if (document.getElementById('log-autoscroll')?.checked) {
    view.scrollTop = view.scrollHeight;
  }
}

function clearLogs() { localLogs=[]; renderLogs(); }

// ── Settings ─────────────────────────────────────────────────
let currentArrs = [];
let currentMappings = {};

async function loadSettings() {
  const r = await fetch('/api/settings');
  const s = await r.json();
  document.getElementById('cfg-addr').value    = s.addr||'';
  document.getElementById('cfg-data').value    = s.data_dir||'';
  document.getElementById('cfg-mount').value   = s.mount_dir||'';
  document.getElementById('cfg-library').value = s.library_dir||'';
  document.getElementById('cfg-dc-url').value   = s.decypharr_url||'';
  document.getElementById('cfg-dc-mount').value = s.decypharr_mount||'';
  document.getElementById('cfg-dc-poll').value  = s.decypharr_poll||'30s';
  document.getElementById('cfg-pl-url').value   = s.prowlarr_url||'';
  document.getElementById('cfg-pl-key').value   = s.prowlarr_api_key||'';
  document.getElementById('cfg-pl-indexers').value = (s.streaming?.prowlarr_indexer_ids||[]).join(', ');
  document.getElementById('cfg-seed-threshold').value = s.streaming?.seed_threshold||5;
  document.getElementById('cfg-health-delay').value   = s.streaming?.health_check_delay||'45s';
  document.getElementById('cfg-readahead').value      = s.streaming?.readahead_mb||5;
  document.getElementById('cfg-trackers').value       = (s.streaming?.extra_trackers||[]).join('\n');
  document.getElementById('cfg-ver').textContent = s.version||'—';
  const fs = document.getElementById('fuse-status-badge');
  if (fs) fs.innerHTML = s.fuse_ok
    ? '<span class="badge badge-xs badge-success gap-1"><i class="bi bi-check-circle-fill"></i>FUSE mounted</span>'
    : '<span class="badge badge-xs badge-error gap-1"><i class="bi bi-x-circle-fill"></i>FUSE not mounted</span>';
  const rs = document.getElementById('rd-status-badge');
  if (rs) rs.textContent = s.decypharr_url||'Not configured';
  const dk = s.debrid_api_keys||{};
  document.getElementById('cfg-rd-key').value = dk.realdebrid||'';
  document.getElementById('cfg-tb-key').value = dk.torbox||'';
  currentArrs = s.arrs||[];
  currentMappings = s.category_mappings||{};
  renderArrs();
  renderMappings();
}

async function saveSettings() {
  const mappings = {};
  document.querySelectorAll('.mapping-row').forEach(row => {
    const k = row.querySelector('.map-key').value.trim();
    const v = row.querySelector('.map-val').value.trim();
    if (k) mappings[k] = v;
  });
  currentMappings = mappings;
  const indexerRaw = document.getElementById('cfg-pl-indexers').value.trim();
  const indexerIDs = indexerRaw ? indexerRaw.split(',').map(s=>parseInt(s.trim())).filter(n=>!isNaN(n)) : [];
  const trackers = document.getElementById('cfg-trackers').value.split('\n').map(s=>s.trim()).filter(Boolean);
  const body = {
    decypharr_url:    document.getElementById('cfg-dc-url').value.trim(),
    decypharr_mount:  document.getElementById('cfg-dc-mount').value.trim(),
    decypharr_poll:   document.getElementById('cfg-dc-poll').value.trim(),
    prowlarr_url:     document.getElementById('cfg-pl-url').value.trim(),
    prowlarr_api_key: document.getElementById('cfg-pl-key').value.trim(),
    arrs:             currentArrs,
    category_mappings: currentMappings,
    debrid_api_keys: {
      realdebrid: document.getElementById('cfg-rd-key').value.trim(),
      torbox:     document.getElementById('cfg-tb-key').value.trim(),
    },
    streaming: {
      seed_threshold:       parseInt(document.getElementById('cfg-seed-threshold').value)||5,
      health_check_delay:   document.getElementById('cfg-health-delay').value.trim()||'45s',
      readahead_mb:         parseInt(document.getElementById('cfg-readahead').value)||5,
      extra_trackers:       trackers,
      prowlarr_indexer_ids: indexerIDs,
    },
  };
  await fetch('/api/settings',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  toast('<i class="bi bi-floppy-fill"></i> Saved','success');
}

function renderArrs() {
  const tbody = document.getElementById('arrs-tbody');
  if (!currentArrs.length) {
    tbody.innerHTML = '<tr id="arrs-empty"><td colspan="4" class="text-center text-xs opacity-25 py-4">No arr instances configured</td></tr>';
    return;
  }
  tbody.innerHTML = currentArrs.map(function(a,i){
    const cats = (a.categories||[]).map(function(c){return '<span class="badge badge-xs badge-ghost">'+escHtml(c)+'</span>';}).join(' ');
    return '<tr class="hover:bg-base-200/40">'
      +'<td class="font-medium text-sm">'+escHtml(a.name||'')+'</td>'
      +'<td class="font-mono text-xs opacity-60">'+escHtml(a.url||'')+'</td>'
      +'<td>'+cats+'</td>'
      +'<td class="flex gap-1 justify-end">'
      +'<button class="btn btn-ghost btn-xs" onclick="openArrModal('+i+')"><i class="bi bi-pencil"></i></button>'
      +'<button class="btn btn-ghost btn-xs text-error" onclick="deleteArr('+i+')"><i class="bi bi-trash3"></i></button>'
      +'</td></tr>';
  }).join('');
}

function openArrModal(idx) {
  document.getElementById('arr-edit-idx').value = idx;
  document.getElementById('arr-modal-title').textContent = idx === -1 ? 'Add Arr Instance' : 'Edit Arr Instance';
  const a = idx >= 0 ? currentArrs[idx] : {};
  document.getElementById('arr-name').value   = a.name||'';
  document.getElementById('arr-url').value    = a.url||'';
  document.getElementById('arr-apikey').value = a.api_key||'';
  document.getElementById('arr-cats').value   = (a.categories||[]).join(', ');
  document.getElementById('arr-modal').showModal();
}

function saveArrInstance() {
  const idx = parseInt(document.getElementById('arr-edit-idx').value);
  const instance = {
    name:       document.getElementById('arr-name').value.trim(),
    url:        document.getElementById('arr-url').value.trim(),
    api_key:    document.getElementById('arr-apikey').value.trim(),
    categories: document.getElementById('arr-cats').value.split(',').map(s=>s.trim()).filter(Boolean),
  };
  if (!instance.name || !instance.url) { toast('Name and URL are required','error'); return; }
  if (idx === -1) currentArrs.push(instance);
  else currentArrs[idx] = instance;
  document.getElementById('arr-modal').close();
  renderArrs();
}

function deleteArr(idx) { currentArrs.splice(idx,1); renderArrs(); }

function renderMappings() {
  const list = document.getElementById('mappings-list');
  list.innerHTML = '';
  Object.entries(currentMappings).forEach(([k,v]) => addMappingRow(k,v));
}

function addMappingRow(key='', val='') {
  const list = document.getElementById('mappings-list');
  const row = document.createElement('div');
  row.className = 'mapping-row flex items-center gap-2';
  row.innerHTML = '<input class="map-key input input-bordered input-xs flex-1 font-mono" placeholder="movies-hd" value="'+escHtml(key)+'"/>'
    +'<i class="bi bi-arrow-right opacity-30 flex-shrink-0"></i>'
    +'<input class="map-val input input-bordered input-xs flex-1" placeholder="Movies HD" value="'+escHtml(val)+'"/>'
    +'<button class="btn btn-ghost btn-xs text-error flex-shrink-0" onclick="this.parentElement.remove()"><i class="bi bi-x-lg"></i></button>';
  list.appendChild(row);
}

let _lastSeederStats = null;
async function checkStatus() {
  const [sr, sdr] = await Promise.all([
    fetch('/api/settings').catch(()=>null),
    fetch('/api/flowarr/seeder').catch(()=>null),
  ]);
  if (sr) {
    const s = await sr.json();
    document.getElementById('ver').textContent = 'v'+(s.version||'?');
    document.getElementById('fuse-pill').classList.toggle('hidden',!s.fuse_ok);
  }
  if (sdr) {
    const sd = await sdr.json();
    updateDLToggle(sd.downloads_enabled !== false);
    const st = sd.stats || {};
    _lastSeederStats = st;
    const upEl = document.getElementById('s-upload');
    if (upEl) upEl.textContent = fmtSpeed(st.upload_bps||0);
    renderSeederDashCard(st);
  }
}
function renderSeederDashCard(st) {
  const statsRow = document.getElementById('provider-stats-row');
  if (!statsRow) return;
  let card = document.getElementById('seeder-dash-card');
  if ((st.active||0) === 0 && (st.total||0) === 0) {
    if (card) card.remove();
    return;
  }
  if (!card) {
    card = document.createElement('div');
    card.id = 'seeder-dash-card';
    card.className = 'bg-base-100 border border-success/30 rounded-xl p-3 flex-1 min-w-[140px]';
    statsRow.appendChild(card);
    statsRow.classList.remove('hidden');
    statsRow.classList.add('flex');
  }
  card.innerHTML =
    '<div class="flex items-center gap-1.5 mb-2">'
    +'<i class="bi bi-upload text-xs text-success"></i>'
    +'<span class="text-[11px] font-semibold">Community Seeder</span>'
    +'</div>'
    +'<div class="grid grid-cols-3 gap-1">'
    +'<div class="text-center"><div class="text-lg font-bold text-success">'+(st.active||0)+'</div><div class="text-[9px] uppercase opacity-40">Seeding</div></div>'
    +'<div class="text-center"><div class="text-sm font-bold text-success">'+fmtSpeed(st.upload_bps||0)+'</div><div class="text-[9px] uppercase opacity-40">↑ Speed</div></div>'
    +'<div class="text-center"><div class="text-sm font-bold text-primary">'+fmtBytes((st.session_uploaded_gb||0)*1024*1024*1024)+'</div><div class="text-[9px] uppercase opacity-40">Session</div></div>'
    +'</div>';
}

// ── Util ─────────────────────────────────────────────────────
function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
function escAttr(s) {
  return String(s).replace(/'/g,"\\'");
}

// ── Seeder ───────────────────────────────────────────────────
let _seederData = [];
let _seederEnabled = false;
function seedStateBadge(s) {
  if (s === 'seeding')   return '<span class="badge badge-xs badge-success gap-0.5"><i class="bi bi-upload"></i>Seeding</span>';
  if (s === 'queued')    return '<span class="badge badge-xs badge-warning">Queued</span>';
  if (s === 'paused')    return '<span class="badge badge-xs badge-ghost">Paused</span>';
  if (s === 'verifying') return '<span class="badge badge-xs badge-info">Verifying</span>';
  if (s === 'rd-only')   return '<span class="badge badge-xs badge-ghost opacity-40">RD only</span>';
  return '<span class="badge badge-xs">' + escHtml(s) + '</span>';
}
// ── Downloads toggle ─────────────────────────────────────────
function updateDLToggle(enabled) {
  const btn = document.getElementById('dl-toggle-btn');
  const lbl = document.getElementById('dl-toggle-label');
  const ico = document.getElementById('dl-toggle-icon');
  if (!btn) return;
  if (enabled) {
    btn.className = 'btn btn-xs gap-1 font-mono btn-success';
    lbl.textContent = 'DL: ON';
    ico.className = 'bi bi-cloud-download-fill';
  } else {
    btn.className = 'btn btn-xs gap-1 font-mono btn-ghost opacity-50';
    lbl.textContent = 'DL: OFF';
    ico.className = 'bi bi-cloud-download';
  }
}
async function toggleDownloads() {
  const cur = document.getElementById('dl-toggle-label').textContent.includes('ON');
  const body = { downloads_enabled: !cur };
  const r = await fetch('/api/flowarr/seeder/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)}).catch(()=>null);
  if (r && r.ok) {
    updateDLToggle(!cur);
    toast('Downloads ' + (!cur ? 'enabled' : 'disabled'), !cur ? 'success' : 'warning');
  }
}
function fmtDuration(sec) {
  if (!sec || sec <= 0) return '—';
  if (sec < 60) return Math.round(sec) + 's';
  if (sec < 3600) return Math.round(sec/60) + 'm';
  return (sec/3600).toFixed(1) + 'h';
}
let _tbItems = [], _tbQueueItems = [];
async function refreshSeeder() {
  const [sr, lr, tbr, tbqr] = await Promise.all([
    fetch('/api/flowarr/seeder').catch(()=>null),
    fetch('/api/flowarr/rdlibrary').catch(()=>null),
    fetch('/api/flowarr/tblibrary').catch(()=>null),
    fetch('/api/flowarr/tbqueue').catch(()=>null),
  ]);
  if (!sr) return;
  const d = await sr.json();

  const enabled = d.enabled;
  _seederEnabled = enabled;
  document.getElementById('seeder-enabled-badge').classList.toggle('hidden', !enabled);
  document.getElementById('seeder-disabled-badge').classList.toggle('hidden', enabled);
  document.getElementById('seed-all-btn').classList.toggle('hidden', !enabled);

  const st = d.stats || {};
  document.getElementById('sd-active').textContent           = st.active    || 0;
  document.getElementById('sd-queued').textContent           = st.queued    || 0;
  document.getElementById('sd-paused').textContent           = st.paused    || 0;
  document.getElementById('sd-verifying').textContent        = st.verifying || 0;
  document.getElementById('sd-peers').textContent            = st.peers     || 0;
  document.getElementById('sd-uploaded').textContent         = fmtBytes((st.uploaded_gb||0)*1024*1024*1024);
  document.getElementById('sd-session-uploaded').textContent = fmtBytes((st.session_uploaded_gb||0)*1024*1024*1024);
  document.getElementById('sd-upload-speed').textContent     = fmtSpeed(st.upload_bps||0);
  const upEl = document.getElementById('s-upload');
  if (upEl) upEl.textContent = fmtSpeed(st.upload_bps||0);

  const cfg = d.settings || {};
  document.getElementById('sd-cfg-upload').value          = cfg.upload_limit_mbps   ?? 5;
  document.getElementById('sd-cfg-max').value             = cfg.max_active          ?? 5;
  document.getElementById('sd-cfg-rotate').value          = cfg.rotate_after_hours  ?? 24;
  document.getElementById('sd-cfg-yearfrom').value        = cfg.favor_year_from     || '';
  document.getElementById('sd-cfg-yearto').value          = cfg.favor_year_to       || '';
  document.getElementById('sd-cfg-downloads').checked     = d.downloads_enabled !== false;
  document.getElementById('sd-cfg-lowseeders').checked    = cfg.prioritize_low_seeders === true;
  document.getElementById('sd-campaign-min-seed').value   = cfg.campaign_min_seed_mb       ?? '';
  document.getElementById('sd-campaign-max-seed').value   = cfg.campaign_max_seed_mb       ?? '';
  document.getElementById('sd-campaign-min-time').value   = cfg.campaign_min_time_minutes  ?? '';
  document.getElementById('sd-campaign-max-time').value   = cfg.campaign_max_time_minutes  ?? '';
  document.getElementById('sd-campaign-loop').checked     = cfg.campaign_loop !== false;
  const sfp = cfg.seed_from_providers||[];
  document.getElementById('sd-prov-rd').checked = sfp.length===0 || sfp.includes('realdebrid');
  document.getElementById('sd-prov-tb').checked = sfp.includes('torbox');
  // sd-campaign-peer-wait removed from UI (peer-wait logic replaced by proactive chunk-pull)
  document.getElementById('sd-campaign-chunk-size').value = cfg.campaign_chunk_size_mb     ?? '';
  document.getElementById('sd-campaign-chunk-dir').value  = cfg.campaign_chunk_dir         ?? '';
  document.getElementById('sd-campaign-chunk-fallback').value = cfg.campaign_chunk_dir_fallback ?? '';
  document.getElementById('sd-tb-cycle-days').value = cfg.campaign_tb_cycle_days > 0 ? cfg.campaign_tb_cycle_days : '';
  document.getElementById('sd-rd-cycle-days').value = cfg.campaign_rd_cycle_days > 0 ? cfg.campaign_rd_cycle_days : '';

  // Per-provider cycle stats.
  const agg = d.stats || {};
  const tbTotal   = agg.tb_total || 0;
  const tbWarmed  = agg.tb_warmed_cycle || 0;
  const rdTotal   = agg.rd_total || 0;
  const rdWarmed  = agg.rd_warmed_cycle || 0;
  const tbPct = tbTotal > 0 ? Math.round(tbWarmed/tbTotal*100) : 0;
  const rdPct = rdTotal > 0 ? Math.round(rdWarmed/rdTotal*100) : 0;
  const tbMinTime = agg.tb_min_time_min || 0;
  const rdMinTime = agg.rd_min_time_min || 0;
  const tbDaysEst = agg.tb_cycle_days_est || 0;
  const rdDaysEst = agg.rd_cycle_days_est || 0;
  const campaignActive = cfg.campaign_min_seed_mb > 0 || cfg.campaign_min_time_minutes > 0 ||
    cfg.campaign_tb_cycle_days > 0 || cfg.campaign_rd_cycle_days > 0;
  const progressCard = document.getElementById('sd-campaign-progress-card');
  if (progressCard) progressCard.classList.toggle('hidden', !campaignActive);
  // TorBox stats
  const tbCycleLabel = document.getElementById('sd-tb-cycle-label');
  if (tbCycleLabel) tbCycleLabel.textContent = tbWarmed + ' / ' + tbTotal + ' (' + tbPct + '%)';
  const tbCycleBar = document.getElementById('sd-tb-cycle-bar');
  if (tbCycleBar) tbCycleBar.value = tbPct;
  const tbEta = document.getElementById('sd-tb-cycle-eta');
  if (tbEta) {
    const slotMin = tbMinTime > 0 ? '~' + (tbMinTime < 60 ? tbMinTime.toFixed(0)+'min' : (tbMinTime/60).toFixed(1)+'h') + ' per item' : '';
    const eta = tbDaysEst > 0 ? ' · est. ' + tbDaysEst.toFixed(1) + ' days remaining' : (tbTotal > 0 && tbWarmed >= tbTotal ? ' · cycle complete ✓' : '');
    tbEta.textContent = [slotMin, eta].filter(Boolean).join('') || (tbTotal > 0 ? 'Set TorBox cycle target to enable auto-timing' : 'No TorBox items');
  }
  // RD stats
  const rdCycleLabel = document.getElementById('sd-rd-cycle-label');
  if (rdCycleLabel) rdCycleLabel.textContent = rdWarmed + ' / ' + rdTotal + ' (' + rdPct + '%)';
  const rdCycleBar = document.getElementById('sd-rd-cycle-bar');
  if (rdCycleBar) rdCycleBar.value = rdPct;
  const rdEta = document.getElementById('sd-rd-cycle-eta');
  if (rdEta) {
    const slotMin = rdMinTime > 0 ? '~' + (rdMinTime < 60 ? rdMinTime.toFixed(0)+'min' : (rdMinTime/60).toFixed(1)+'h') + ' per item' : '';
    const eta = rdDaysEst > 0 ? ' · est. ' + rdDaysEst.toFixed(1) + ' days remaining' : (rdTotal > 0 && rdWarmed >= rdTotal ? ' · cycle complete ✓' : '');
    rdEta.textContent = [slotMin, eta].filter(Boolean).join('') || (rdTotal > 0 ? 'Using global Min Time setting' : 'No RD items');
  }
  // Calculated slot-time hints in settings UI
  const tbCalc = document.getElementById('sd-tb-calc-label');
  if (tbCalc && tbMinTime > 0) tbCalc.textContent = 'Each item gets ~' + (tbMinTime < 60 ? tbMinTime.toFixed(0)+' min' : (tbMinTime/60).toFixed(1)+' h') + ' · ' + tbTotal + ' total items';
  const rdCalc = document.getElementById('sd-rd-calc-label');
  if (rdCalc && rdMinTime > 0) rdCalc.textContent = 'Each item gets ~' + (rdMinTime < 60 ? rdMinTime.toFixed(0)+' min' : (rdMinTime/60).toFixed(1)+' h') + ' · ' + rdTotal + ' total items';

  // The table shows torrents currently loaded in the seeder's rotation queue
  // (anacrolix client). The full library backlog lives in pending and is shown
  // via the Total / Pending stat cards above, not in the table (16k rows would
  // be unusably slow).
  _seederData = (d.torrents||[]).slice().sort((a,b) => {
    if (a.state === 'seeding' && b.state !== 'seeding') return -1;
    if (b.state === 'seeding' && a.state !== 'seeding') return 1;
    return (a.queue_pos||99999) - (b.queue_pos||99999);
  });

  // Use seeder's authoritative total (includes pending backlog) for the counter.
  document.getElementById('sd-total').textContent = st.total || _seederData.length;
  // Show pending backlog count if it exists.
  const pendingEl = document.getElementById('sd-pending');
  if (pendingEl) pendingEl.textContent = st.pending || 0;
  renderSeederLive();
  renderSeederTable();
  updateDLToggle(d.downloads_enabled !== false);

  // TorBox library and queue
  if (tbr && tbr.ok) _tbItems = await tbr.json().catch(()=>[]);
  if (tbqr && tbqr.ok) _tbQueueItems = await tbqr.json().catch(()=>[]);
  renderTBTable();
}
function renderSeederLive() {
  const now = Date.now() / 1000;
  const active = _seederData.filter(t => t.state === 'seeding');
  const liveEmpty = document.getElementById('sd-live-empty');
  const liveList  = document.getElementById('sd-live-list');
  liveEmpty.classList.toggle('hidden', active.length > 0);
  liveList.innerHTML = active.map(t => {
    const activeSec = t.active_since ? (now - new Date(t.active_since).getTime()/1000) : 0;
    return '<div class="px-4 py-3 flex items-center gap-3 flex-wrap">' +
      '<div class="flex-1 min-w-0">' +
        '<div class="font-medium text-sm truncate" title="' + escAttr(t.name||t.hash) + '">' + escHtml(t.name||t.hash.slice(0,16)+'…') + '</div>' +
        '<div class="text-[11px] opacity-40 mt-0.5">'+(t.upload_bps>0?'<span class="text-success opacity-100">↑ '+fmtSpeed(t.upload_bps)+'</span> &nbsp;·&nbsp; ':'')+
        'Session ' + fmtBytes(t.session_uploaded||0) + ' &nbsp;·&nbsp; Total ' + fmtBytes(t.uploaded) + ' &nbsp;·&nbsp; ' + (t.peers||0) + ' peers &nbsp;·&nbsp; active ' + fmtDuration(activeSec) + (t.year?' &nbsp;·&nbsp; '+t.year:'') + '</div>' +
      '</div>' +
      '<div class="flex gap-1 flex-shrink-0">' +
        '<button class="btn btn-xs btn-warning gap-1" onclick="seederPause(\'' + escAttr(t.hash) + '\')"><i class="bi bi-pause-fill"></i>Pause</button>' +
        '<button class="btn btn-xs btn-error gap-1" onclick="seederRemove(\'' + escAttr(t.hash) + '\')"><i class="bi bi-stop-fill"></i>Stop</button>' +
      '</div>' +
    '</div>';
  }).join('');
}
// ── Seeder queue — collapsible per-provider sections ──────────
const _sdCollapsed = {};
// Sort state for seeder queue and downloads tables
let _sdSortCol = 'queue_pos', _sdSortDir = 1;
let _dlSortCol = 'name',      _dlSortDir = 1;
let _tbSortCol = 'name',      _tbSortDir = 1;
function _sdSort(col) {
  if (_sdSortCol === col) _sdSortDir *= -1; else { _sdSortCol = col; _sdSortDir = 1; }
  renderSeederTable();
}
function _dlSort(col) {
  if (_dlSortCol === col) _dlSortDir *= -1; else { _dlSortCol = col; _dlSortDir = 1; }
  renderDownloads();
}
function _tbSort(col) {
  if (_tbSortCol === col) _tbSortDir *= -1; else { _tbSortCol = col; _tbSortDir = 1; }
  renderTBTable();
}
function _th(label, col, sortCol, sortDir, fn, tip) {
  const active = col === sortCol;
  const arrow = active ? (sortDir > 0 ? ' <span class="text-primary">↑</span>' : ' <span class="text-primary">↓</span>') : ' <span class="opacity-20 text-[9px]">⇅</span>';
  const titleAttr = tip ? ' title="'+escHtml(tip)+'"' : '';
  return '<th class="cursor-pointer select-none whitespace-nowrap hover:text-base-content' + (active?' text-primary':'')+'"'+titleAttr+' onclick="'+fn+'(\''+col+'\')">' + label + arrow + '</th>';
}
function _sdSortItems(items) {
  const col = _sdSortCol, dir = _sdSortDir;
  return [...items].sort((a,b) => {
    let av = a[col], bv = b[col];
    if (col === 'active_since') { av = a.active_since ? new Date(a.active_since).getTime() : 0; bv = b.active_since ? new Date(b.active_since).getTime() : 0; }
    if (typeof av === 'string') return dir * av.localeCompare(bv||''||a.hash||b.hash);
    return dir * ((av||0) - (bv||0));
  });
}
function _dlSortItems(items) {
  const col = _dlSortCol, dir = _dlSortDir;
  return [...items].sort((a,b) => {
    let av = a[col], bv = b[col];
    if (typeof av === 'string') return dir * (av||'').localeCompare(bv||'');
    return dir * ((av||0) - (bv||0));
  });
}
function _tbSortItems(items) {
  const col = _tbSortCol, dir = _tbSortDir;
  return [...items].sort((a,b) => {
    let av = a[col], bv = b[col];
    if (typeof av === 'string') return dir * (av||'').localeCompare(bv||'');
    return dir * ((av||0) - (bv||0));
  });
}
function _sdToggleSection(key) {
  _sdCollapsed[key] = !_sdCollapsed[key];
  localStorage.setItem('sd-collapsed-'+key, _sdCollapsed[key] ? '1' : '0');
  // Re-render so rows are injected on expand and removed on collapse (perf)
  renderSeederTable();
}

function _sdRowHtml(t) {
  const now = Date.now() / 1000;
  const activeSec = t.active_since ? (now - new Date(t.active_since).getTime()/1000) : 0;
  const actionBtns =
    (t.state==='seeding'
      ? '<button class="btn btn-ghost btn-xs text-warning" title="Pause" onclick="seederPause(\''+escAttr(t.hash)+'\')"><i class="bi bi-pause-fill"></i></button>'
      : '') +
    (t.state==='paused'
      ? '<button class="btn btn-ghost btn-xs text-success" title="Resume" onclick="seederResume(\''+escAttr(t.hash)+'\')"><i class="bi bi-play-fill"></i></button>'
      : '') +
    ((t.state==='queued'||t.state==='paused')
      ? '<button class="btn btn-ghost btn-xs text-primary" title="Seed now" onclick="seederPrioritize(\''+escAttr(t.hash)+'\')"><i class="bi bi-skip-start-fill"></i></button>'
      : '') +
    (t.state==='rd-only'
      ? (_seederEnabled
          ? '<button class="btn btn-ghost btn-xs text-primary" title="Add to seeder" onclick="seederAddHash(\''+escAttr(t.hash)+'\',\''+escAttr(t.name||'')+'\')"><i class="bi bi-upload"></i></button>'
          : '<button class="btn btn-ghost btn-xs opacity-30" disabled><i class="bi bi-upload"></i></button>')
      : '<button class="btn btn-ghost btn-xs text-error" title="Remove" onclick="seederRemove(\''+escAttr(t.hash)+'\')"><i class="bi bi-trash3"></i></button>');
  let cycleBadge = '';
  if (t.cycle_done) {
    cycleBadge = '<span class="badge badge-xs badge-success gap-0.5"><i class="bi bi-check-lg"></i>Done</span>';
  } else if (t.state==='seeding' && (t.cycle_read_mb>0||t.cycle_time_sec>0)) {
    cycleBadge = '<span class="text-[10px] opacity-60">'+(t.cycle_read_mb>0?fmtBytes(t.cycle_read_mb*1024*1024)+' read':'')+
      (t.cycle_time_sec>0?(t.cycle_read_mb>0?' · ':'')+fmtDuration(t.cycle_time_sec):'')+'</span>';
  }
  const provBadge = t.provider==='torbox'
    ? '<span class="badge badge-xs badge-warning gap-0.5 ml-1" title="TorBox — 30-day cache">TB</span>'
    : t.provider==='realdebrid'
      ? '<span class="badge badge-xs badge-primary gap-0.5 ml-1" title="Real-Debrid">RD</span>'
      : '';
  const warmedStr = t.warmed_at && t.warmed_at !== '0001-01-01T00:00:00Z'
    ? 'Last warmed: '+new Date(t.warmed_at).toLocaleString() : '';
  return '<tr class="hover">'
    +'<td class="opacity-30 font-mono">'+(t.queue_pos>=99999?'—':t.queue_pos)+'</td>'
    +'<td class="max-w-[260px] truncate" title="'+escAttr((t.name||t.hash)+(warmedStr?' · '+warmedStr:''))+'">'+escHtml(t.name||t.hash.slice(0,16)+'...')+provBadge+'</td>'
    +'<td class="opacity-50">'+(t.year||'—')+'</td>'
    +'<td>'+seedStateBadge(t.state)+'</td>'
    +'<td>'+fmtBytes(t.size)+'</td>'
    +'<td class="text-primary">'+fmtBytes(t.session_uploaded||0)+'</td>'
    +'<td class="opacity-50">'+fmtBytes(t.uploaded||0)+'</td>'
    +'<td class="text-success font-medium">'+(t.upload_bps>0?fmtSpeed(t.upload_bps):'—')+'</td>'
    +'<td>'+(t.peers||0)+'</td>'
    +'<td>'+(activeSec>0?fmtDuration(activeSec):'—')+'</td>'
    +'<td>'+cycleBadge+'</td>'
    +'<td class="text-right whitespace-nowrap">'+actionBtns+'</td>'
    +'</tr>';
}

function renderSeederTable() {
  const filter = (document.getElementById('sd-filter')?.value||'').toLowerCase();
  const stateFilter = document.getElementById('sd-filter-state')?.value||'';

  const groups = {};
  const addGroup = (key, meta, t) => {
    if (!groups[key]) groups[key] = {meta, items:[]};
    groups[key].items.push(t);
  };

  const rdChecked = document.getElementById('sd-prov-rd')?.checked !== false;
  const tbChecked = document.getElementById('sd-prov-tb')?.checked === true;
  (_seederData||[]).forEach(t => {
    if (filter && !(t.name||t.hash).toLowerCase().includes(filter)) return;
    if (stateFilter && t.state !== stateFilter) return;
    const prov = (t.debrid||'').toLowerCase() || 'realdebrid';
    if (prov === 'realdebrid' && !rdChecked) return;
    if (prov === 'torbox' && !tbChecked) return;
    const m = providerMeta(prov);
    addGroup(prov, m, t);
  });

  const sectionsEl = document.getElementById('sd-queue-sections');
  if (!sectionsEl) return;
  const keys = Object.keys(groups);

  if (!keys.length) {
    sectionsEl.innerHTML = '<div class="text-center opacity-25 py-6 text-sm">'+
      (_seederData.length===0 ? 'No torrents in library' : 'No results for current filter')+'</div>';
    return;
  }

  [...sectionsEl.querySelectorAll('[data-sdsec]')].forEach(el => { if (!groups[el.dataset.sdsec]) el.remove(); });

  const thead = '<thead class="bg-base-200 text-[11px] uppercase tracking-wider text-base-content/50"><tr>'
    +_th('#','queue_pos',_sdSortCol,_sdSortDir,'_sdSort','Rotation position. Lower = seeds sooner when a slot opens up.')
    +_th('Name','name',_sdSortCol,_sdSortDir,'_sdSort')
    +_th('Year','year',_sdSortCol,_sdSortDir,'_sdSort','Release year, used when Favour Year range is set.')
    +_th('State','state',_sdSortCol,_sdSortDir,'_sdSort','Seeding = actively uploading to peers. Queued = waiting for a free slot. Verifying = resolving torrent metadata from DHT/trackers before it can seed. Paused = manually paused.')
    +_th('Size','size',_sdSortCol,_sdSortDir,'_sdSort','Total torrent size.')
    +_th('Session ↑','session_uploaded',_sdSortCol,_sdSortDir,'_sdSort','Uploaded since the last "Clear Stats" click or since Flowarr last restarted. Starts at zero — will rise once peers connect and download pieces.')
    +_th('All-Time ↑','uploaded',_sdSortCol,_sdSortDir,'_sdSort','Uploaded since this torrent was last loaded into Flowarr. Resets to zero when Flowarr restarts (not persisted to disk).')
    +_th('↑ Speed','upload_bps',_sdSortCol,_sdSortDir,'_sdSort','Current upload speed to peers.')
    +_th('Peers','peers',_sdSortCol,_sdSortDir,'_sdSort','Number of peers currently connected to this torrent.')
    +_th('Active For','active_since',_sdSortCol,_sdSortDir,'_sdSort','Time spent in the active seeding slot this rotation.')
    +'<th title="Campaign progress. Shows how much has been uploaded / time spent this cycle. ✓ Done = met the minimum targets.">Cycle</th>'
    +'<th class="text-right">Actions</th></tr></thead>';

  keys.forEach(key => {
    const {meta, items} = groups[key];
    if (!(key in _sdCollapsed)) {
      _sdCollapsed[key] = localStorage.getItem('sd-collapsed-'+key) !== '0';
    }
    const collapsed = !!_sdCollapsed[key];
    let card = sectionsEl.querySelector('[data-sdsec="'+key+'"]');
    if (!card) {
      card = document.createElement('div');
      card.dataset.sdsec = key;
      card.className = 'bg-base-100 border border-base-300 rounded-xl overflow-hidden';
      sectionsEl.appendChild(card);
    }
    const sortedItems = _sdSortItems(items);
    const bodyHtml = collapsed
      ? ''
      : '<div class="overflow-x-auto"><table class="table table-xs text-xs">'+thead+'<tbody>'+sortedItems.map(_sdRowHtml).join('')+'</tbody></table></div>';
    const sectionDesc = {
      realdebrid: 'Your full Real-Debrid library. Flowarr seeds these back to the P2P swarm so RD keeps them cached. Session ↑ and All-Time ↑ start at 0 and accumulate as peers connect — both reset when Flowarr restarts.',
      torbox: 'TorBox items that are fully cached and being seeded back to the swarm via Flowarr.',
    }[key] || '';
    card.innerHTML =
      '<div class="flex items-center gap-2 px-4 py-3 border-b border-base-300 cursor-pointer select-none" onclick="_sdToggleSection(\''+key+'\')">'+
        '<i class="bi '+meta.icon+' text-warning"></i>'+
        '<div class="flex-1 min-w-0">'+
          '<span class="font-semibold text-sm">'+escHtml(meta.label)+'</span>'+
          (sectionDesc ? '<div class="text-[10px] opacity-40 truncate mt-0.5">'+escHtml(sectionDesc)+'</div>' : '')+
        '</div>'+
        '<span class="badge badge-xs ml-2 flex-shrink-0">'+items.length+'</span>'+
        '<i class="bi bi-chevron-down ml-2 text-base-content/40 transition-transform flex-shrink-0" style="transform:'+(collapsed?'rotate(-90deg)':'')+'"></i>'+
      '</div>'+
      bodyHtml;
  });
}
function _tbStateBadge(state) {
  const s = (state||'').toLowerCase();
  if (s==='cached'||s==='completed') return '<span class="badge badge-xs badge-success">Cached</span>';
  if (s==='downloading') return '<span class="badge badge-xs badge-info">Downloading</span>';
  if (s==='error'||s==='failed') return '<span class="badge badge-xs badge-error">Error</span>';
  if (s==='stalled') return '<span class="badge badge-xs badge-warning">Stalled</span>';
  return '<span class="badge badge-xs badge-ghost">'+escHtml(state||'—')+'</span>';
}
function renderTBTable() {
  const sectionsEl = document.getElementById('sd-queue-sections');
  if (!sectionsEl) return;

  // Remove old TB sections
  sectionsEl.querySelectorAll('[data-tbsec]').forEach(el => el.remove());

  const tbKey = 'torbox-library';
  const tbQueueKey = 'torbox-queue';

  const filter = (document.getElementById('sd-filter')?.value||'').toLowerCase();

  // TorBox library section
  const libItems = (_tbItems||[]).filter(t => !filter || (t.name||'').toLowerCase().includes(filter));
  if (libItems.length > 0) {
    if (!(tbKey in _sdCollapsed)) _sdCollapsed[tbKey] = localStorage.getItem('sd-collapsed-'+tbKey)!=='0';
    const collapsed = !!_sdCollapsed[tbKey];
    const libThead = '<thead class="bg-base-200 text-[11px] uppercase tracking-wider text-base-content/50"><tr>'
      +_th('Name','name',_tbSortCol,_tbSortDir,'_tbSort')
      +_th('State','download_state',_tbSortCol,_tbSortDir,'_tbSort')
      +_th('Size','size',_tbSortCol,_tbSortDir,'_tbSort')
      +_th('Progress','progress',_tbSortCol,_tbSortDir,'_tbSort')
      +_th('↓ Speed','download_speed',_tbSortCol,_tbSortDir,'_tbSort')
      +_th('Seeds','seeds',_tbSortCol,_tbSortDir,'_tbSort')
      +_th('ETA','eta',_tbSortCol,_tbSortDir,'_tbSort')
      +'</tr></thead>';
    const sortedLib = _tbSortItems(libItems);
    const libRows = sortedLib.map(t => '<tr class="hover">'
      +'<td class="max-w-[300px] truncate text-xs" title="'+escAttr(t.name||t.hash||'')+'">'+escHtml(t.name||t.hash||'—')+'</td>'
      +'<td>'+_tbStateBadge(t.download_state)+'</td>'
      +'<td class="text-xs opacity-60">'+fmtBytes(t.size)+'</td>'
      +'<td><div class="flex items-center gap-1"><progress class="progress progress-success h-1.5 w-20" value="'+Math.round((t.progress||0)*100)+'" max="100"></progress><span class="text-xs">'+Math.round((t.progress||0)*100)+'%</span></div></td>'
      +'<td class="text-xs text-accent">'+(t.download_speed>0?fmtSpeed(t.download_speed):'—')+'</td>'
      +'<td class="text-xs opacity-60">'+(t.seeds||0)+'</td>'
      +'<td class="text-xs opacity-60">'+(t.eta>0?fmtETA(t.eta):'—')+'</td>'
      +'</tr>').join('');
    const bodyHtml = collapsed ? '' : '<div class="overflow-x-auto"><table class="table table-xs text-xs">'+libThead+'<tbody>'+libRows+'</tbody></table></div>';
    const card = document.createElement('div');
    card.dataset.tbsec = tbKey;
    card.className = 'bg-base-100 border border-base-300 rounded-xl overflow-hidden';
    card.innerHTML =
      '<div class="flex items-center gap-2 px-4 py-3 border-b border-base-300 cursor-pointer select-none" onclick="_sdCollapsed[\''+tbKey+'\'] = !_sdCollapsed[\''+tbKey+'\']; localStorage.setItem(\'sd-collapsed-'+tbKey+'\',_sdCollapsed[\''+tbKey+'\']?\'1\':\'0\'); renderTBTable();">'
      +'<i class="bi bi-cloud-fill text-info"></i>'
      +'<div class="flex-1 min-w-0">'
      +'<span class="font-semibold text-sm">TorBox Library</span>'
      +'<div class="text-[10px] opacity-40 truncate mt-0.5">Everything in your TorBox account. "Cached" items are fully downloaded by TorBox and ready to stream or seed. Others are still being downloaded by TorBox\'s servers.</div>'
      +'</div>'
      +'<span class="badge badge-xs ml-2 flex-shrink-0">'+libItems.length+'</span>'
      +'<i class="bi bi-chevron-down ml-2 text-base-content/40 transition-transform flex-shrink-0" style="transform:'+(collapsed?'rotate(-90deg)':'')+'"></i>'
      +'</div>'+bodyHtml;
    sectionsEl.appendChild(card);
  }

  // TorBox queue section (items waiting, not yet downloading)
  const queueItems = (_tbQueueItems||[]).filter(t => !filter || (t.name||'').toLowerCase().includes(filter));
  if (queueItems.length > 0) {
    if (!(tbQueueKey in _sdCollapsed)) _sdCollapsed[tbQueueKey] = localStorage.getItem('sd-collapsed-'+tbQueueKey)!=='0';
    const collapsed = !!_sdCollapsed[tbQueueKey];
    const qThead = '<thead class="bg-base-200 text-[11px] uppercase tracking-wider text-base-content/50"><tr>'
      +'<th title="Torrent name">Name</th><th title="Truncated info-hash">Hash</th><th title="When the item entered the queue">Queued At</th></tr></thead>';
    const qRows = queueItems.map(t => '<tr class="hover">'
      +'<td class="max-w-[300px] truncate text-xs" title="'+escAttr(t.name||t.hash||'')+'">'+escHtml(t.name||t.hash?.slice(0,16)+'…'||'—')+'</td>'
      +'<td class="font-mono text-[10px] opacity-40">'+escHtml((t.hash||'').slice(0,16))+'…</td>'
      +'<td class="text-xs opacity-60">'+escHtml(t.created_at?.slice(0,16)||'—')+'</td>'
      +'</tr>').join('');
    const bodyHtml = collapsed ? '' : '<div class="overflow-x-auto"><table class="table table-xs text-xs">'+qThead+'<tbody>'+qRows+'</tbody></table></div>';
    const card = document.createElement('div');
    card.dataset.tbsec = tbQueueKey;
    card.className = 'bg-base-100 border border-base-300 rounded-xl overflow-hidden';
    card.innerHTML =
      '<div class="flex items-center gap-2 px-4 py-3 border-b border-base-300 cursor-pointer select-none" onclick="_sdCollapsed[\''+tbQueueKey+'\'] = !_sdCollapsed[\''+tbQueueKey+'\']; localStorage.setItem(\'sd-collapsed-'+tbQueueKey+'\',_sdCollapsed[\''+tbQueueKey+'\']?\'1\':\'0\'); renderTBTable();">'
      +'<i class="bi bi-hourglass-split text-warning"></i>'
      +'<div class="flex-1 min-w-0">'
      +'<span class="font-semibold text-sm">TorBox Pending Queue</span>'
      +'<div class="text-[10px] opacity-40 truncate mt-0.5">Items submitted to TorBox but not yet being downloaded — TorBox is waiting to find the torrent on the P2P network before it starts caching.</div>'
      +'</div>'
      +'<span class="badge badge-xs ml-2 flex-shrink-0">'+queueItems.length+'</span>'
      +'<i class="bi bi-chevron-down ml-2 text-base-content/40 transition-transform flex-shrink-0" style="transform:'+(collapsed?'rotate(-90deg)':'')+'"></i>'
      +'</div>'+bodyHtml;
    sectionsEl.appendChild(card);
  }
}
async function saveSeederSettings() {
  const fv = id => document.getElementById(id)?.value;
  const body = {
    upload_limit_mbps:              parseInt(fv('sd-cfg-upload'))         || 0,
    max_active:                     parseInt(fv('sd-cfg-max'))            || 0,
    rotate_after_hours:             parseFloat(fv('sd-cfg-rotate'))       || 0,
    downloads_enabled:              document.getElementById('sd-cfg-downloads').checked,
    prioritize_low_seeders:         document.getElementById('sd-cfg-lowseeders').checked,
    favor_year_from:                parseInt(fv('sd-cfg-yearfrom'))       || 0,
    favor_year_to:                  parseInt(fv('sd-cfg-yearto'))         || 0,
    campaign_min_seed_mb:           parseFloat(fv('sd-campaign-min-seed'))   || 0,
    campaign_max_seed_mb:           parseFloat(fv('sd-campaign-max-seed'))   || 0,
    campaign_min_time_minutes:      parseFloat(fv('sd-campaign-min-time'))   || 0,
    campaign_max_time_minutes:      parseFloat(fv('sd-campaign-max-time'))   || 0,
    campaign_peer_wait_seconds:     parseInt(fv('sd-campaign-peer-wait'))    || 0,
    campaign_chunk_size_mb:         parseFloat(fv('sd-campaign-chunk-size')) || 0,
    campaign_chunk_dir:             fv('sd-campaign-chunk-dir')?.trim()      || '',
    campaign_chunk_dir_fallback:    fv('sd-campaign-chunk-fallback')?.trim() || '',
    campaign_loop:                  document.getElementById('sd-campaign-loop').checked,
    campaign_tb_cycle_days:         parseFloat(fv('sd-tb-cycle-days')) || 0,
    campaign_rd_cycle_days:         parseFloat(fv('sd-rd-cycle-days')) || 0,
    seed_from_providers: [
      ...(document.getElementById('sd-prov-rd').checked?['realdebrid']:[]),
      ...(document.getElementById('sd-prov-tb').checked?['torbox']:[]),
    ],
  };
  const r = await fetch('/api/flowarr/seeder/settings', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)}).catch(()=>null);
  if (r && r.ok) { toast('Seeder settings saved','success'); refreshSeeder(); }
  else           { toast('Failed to save seeder settings','error'); }
}
async function resetCycle() {
  const r = await fetch('/api/flowarr/seeder/reset-cycle', {method:'POST'}).catch(()=>null);
  if (r && r.ok) { toast('Campaign cycle reset', 'info'); refreshSeeder(); }
  else { toast('Failed to reset cycle', 'error'); }
}
async function resetUploadStats() {
  const r = await fetch('/api/flowarr/seeder/reset-stats', {method:'POST'}).catch(()=>null);
  if (r && r.ok) { toast('Upload stats cleared', 'info'); refreshSeeder(); }
  else { toast('Failed to clear stats', 'error'); }
}
async function mirrorToProvider() {
  const src = document.getElementById('mirror-src-select')?.value || 'realdebrid';
  const dst = document.getElementById('mirror-dst-select')?.value || 'torbox';
  const resultEl = document.getElementById('mirror-result');
  const btn = document.getElementById('mirror-btn');
  if (src === dst) { toast('Source and destination must be different', 'error'); return; }
  if (btn) { btn.disabled = true; btn.innerHTML = '<span class="loading loading-spinner loading-xs"></span>Running…'; }
  if (resultEl) { resultEl.classList.remove('hidden'); resultEl.innerHTML = '<span class="opacity-50">Connecting…</span>'; }

  const showProgress = (ev) => {
    if (!resultEl) return;
    if (ev.phase === 'done') {
      const notCached = ev.not_cached || 0;
      const etaH = ev.eta_hours;
      resultEl.innerHTML =
        '<span class="text-success font-semibold">Done</span>'+
        '<span class="text-info">Instant (cached): <b>'+(ev.submitted_instant||0)+'</b></span>'+
        '<span class="opacity-60">Queued (P2P): <b>'+(ev.submitted_queued||0)+'</b></span>'+
        '<span class="opacity-40">Already in library: <b>'+(ev.already_in||0)+'</b></span>'+
        '<span class="opacity-40">Skipped: <b>'+(ev.skipped||0)+'</b></span>'+
        (ev.errors>0 ? '<span class="text-error">Errors: <b>'+ev.errors+'</b></span>' : '')+
        (notCached>0
          ? '<button class="btn btn-warning btn-xs gap-1 ml-2" onclick="transferViaRD()"><i class="bi bi-cloud-arrow-down-fill"></i>Transfer '+notCached+' uncached via RD</button>'
          : '');
      toast('Mirror complete', 'success');
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-arrow-repeat"></i>Mirror Now'; }
    } else if (ev.phase === 'error') {
      resultEl.innerHTML = '<span class="text-error">'+escHtml(ev.msg||'Error')+'</span>';
      if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-arrow-repeat"></i>Mirror Now'; }
    } else {
      const pct = Math.round((ev.progress||0)*100);
      const etaStr = ev.eta_hours > 0 ? ' · ETA ~'+ev.eta_hours.toFixed(1)+'h' : '';
      resultEl.innerHTML =
        (pct > 0 ? '<progress class="progress progress-info w-32" value="'+pct+'" max="100"></progress>' : '')+
        '<span class="opacity-70">'+escHtml(ev.msg||ev.phase||'Working…')+escHtml(etaStr)+'</span>'+
        (ev.not_cached>0 ? '<span class="opacity-40 text-xs">(TorBox rate: 60 uncached/hr)</span>' : '');
    }
  };

  try {
    const resp = await fetch('/api/flowarr/mirror', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({source: src, provider: dst})
    });
    if (!resp.ok || !resp.body) throw new Error('HTTP '+resp.status);
    const reader = resp.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buf += dec.decode(value, {stream:true});
      const parts = buf.split('\n\n');
      buf = parts.pop();
      for (const part of parts) {
        const line = part.trim();
        if (line.startsWith('data: ')) {
          try { showProgress(JSON.parse(line.slice(6))); } catch(_) {}
        }
      }
    }
  } catch(e) {
    if (resultEl) resultEl.innerHTML = '<span class="text-error">Mirror failed — '+escHtml(String(e))+'</span>';
    if (btn) { btn.disabled = false; btn.innerHTML = '<i class="bi bi-arrow-repeat"></i>Mirror Now'; }
    toast('Mirror failed', 'error');
  }
}
async function transferViaRD(hashes) {
  const resultEl = document.getElementById('transfer-result');
  if (resultEl) { resultEl.classList.remove('hidden'); resultEl.innerHTML = '<span class="opacity-50">Connecting to RD & TorBox…</span>'; }

  const showProgress = (ev) => {
    if (!resultEl) return;
    if (ev.phase === 'done') {
      resultEl.innerHTML =
        '<span class="text-success font-semibold">Transfer Done</span>'+
        '<span class="text-info">Submitted to TorBox: <b>'+(ev.submitted||0)+'</b></span>'+
        '<span class="opacity-40">Not in RD: <b>'+(ev.no_links||0)+'</b></span>'+
        (ev.errors>0 ? '<span class="text-error">Errors: <b>'+ev.errors+'</b></span>' : '');
      toast('RD→TorBox transfer complete', 'success');
    } else if (ev.phase === 'error') {
      resultEl.innerHTML = '<span class="text-error">'+escHtml(ev.msg||'Error')+'</span>';
    } else {
      const pct = Math.round((ev.progress||0)*100);
      resultEl.innerHTML =
        (pct > 0 ? '<progress class="progress progress-warning w-32" value="'+pct+'" max="100"></progress>' : '')+
        '<span class="opacity-70">'+escHtml(ev.msg||ev.phase||'Working…')+'</span>';
    }
  };

  try {
    const body = hashes && hashes.length ? {hashes} : {};
    const resp = await fetch('/api/flowarr/mirror/transfer', {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify(body)
    });
    if (!resp.ok || !resp.body) {
      const txt = await resp.text().catch(()=>'');
      throw new Error('HTTP '+resp.status+(txt?' — '+txt:''));
    }
    const reader = resp.body.getReader();
    const dec = new TextDecoder();
    let buf = '';
    while (true) {
      const {done, value} = await reader.read();
      if (done) break;
      buf += dec.decode(value, {stream:true});
      const parts = buf.split('\n\n');
      buf = parts.pop();
      for (const part of parts) {
        const line = part.trim();
        if (line.startsWith('data: ')) {
          try { showProgress(JSON.parse(line.slice(6))); } catch(_) {}
        }
      }
    }
  } catch(e) {
    if (resultEl) resultEl.innerHTML = '<span class="text-error">Transfer failed — '+escHtml(String(e))+'</span>';
    toast('Transfer failed', 'error');
  }
}

async function seederAddMagnet() {
  const magnet = document.getElementById('sd-magnet-input').value.trim();
  if (!magnet) return;
  const r = await fetch('/api/flowarr/seeder/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({magnet})}).catch(()=>null);
  if (r && r.ok) { document.getElementById('sd-magnet-input').value=''; toast('Magnet added','success'); refreshSeeder(); }
  else { toast('Failed to add magnet','error'); }
}
async function seederRemove(hash) {
  const r = await fetch('/api/flowarr/seeder/remove', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({hash})}).catch(()=>null);
  if (r && r.ok) { toast('Removed from seeder','success'); refreshSeeder(); }
  else { toast('Failed to remove','error'); }
}
async function seederPause(hash) {
  const r = await fetch('/api/flowarr/seeder/pause', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({hash})}).catch(()=>null);
  if (r && r.ok) { toast('Paused','success'); refreshSeeder(); }
  else { toast('Failed to pause','error'); }
}
async function seederResume(hash) {
  const r = await fetch('/api/flowarr/seeder/resume', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({hash})}).catch(()=>null);
  if (r && r.ok) { toast('Resumed','success'); refreshSeeder(); }
  else { toast('Failed to resume','error'); }
}
async function seederPrioritize(hash) {
  const r = await fetch('/api/flowarr/seeder/prioritize', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({hash})}).catch(()=>null);
  if (r && r.ok) { toast('Moved to front of queue','success'); refreshSeeder(); }
  else { toast('Failed to prioritize','error'); }
}
async function seederAddHash(hash, name) {
  const magnet = 'magnet:?xt=urn:btih:' + hash + (name ? '&dn=' + encodeURIComponent(name) : '');
  const r = await fetch('/api/flowarr/seeder/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({magnet})}).catch(()=>null);
  if (r && r.ok) { toast('Added to seeder queue','success'); refreshSeeder(); }
  else { toast('Failed to add','error'); }
}

async function seedAll() {
  const rdOnly = _seederData.filter(t => t.state === 'rd-only');
  if (!rdOnly.length) { toast('No new items to add — all already in seeder queue', 'info'); return; }
  let added = 0, failed = 0;
  for (const t of rdOnly) {
    const magnet = 'magnet:?xt=urn:btih:' + t.hash + (t.name ? '&dn=' + encodeURIComponent(t.name) : '');
    const r = await fetch('/api/flowarr/seeder/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({magnet})}).catch(()=>null);
    if (r && r.ok) added++; else failed++;
  }
  if (added) toast('<i class="bi bi-upload"></i> Added ' + added + ' torrent' + (added===1?'':'s') + ' to seeder queue', 'success');
  if (failed) toast(failed + ' failed to add', 'error');
  refreshSeeder();
}

// ── Boot ─────────────────────────────────────────────────────
nav(location.hash.slice(1) || 'dashboard');
refresh();
checkStatus();
setInterval(refresh, 4000);
setInterval(checkStatus, 20000);
setInterval(()=>{ if(document.getElementById('page-logs')&&!document.getElementById('page-logs').classList.contains('hidden')) refreshLogs(); }, 3000);
setInterval(()=>{ if(document.getElementById('page-seeder')&&!document.getElementById('page-seeder').classList.contains('hidden')) refreshSeeder(); }, 5000);
</script>
</body>
</html>`

func uiHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, uiHTML)
}

func main() {
	cfgPath := flag.String("config", "flowarr.yaml", "path to config file")
	flag.Parse()

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	activeCfg = cfg
	cfgFilePath = *cfgPath

	mountPath, err = filepath.Abs(cfg.FUSE.MountDir)
	if err != nil {
		log.Fatalf("resolve mount path: %v", err)
	}

	mgr, err = torrent.NewManager(cfg.Torrent.DataDir, cfg.Streaming.ExtraTrackers)
	if err != nil {
		log.Fatalf("init torrent manager: %v", err)
	}

	fuseServer, err := vfs.Mount(mountPath, mgr, cfg.Streaming.ReadaheadMB)
	if err != nil {
		log.Printf("FUSE mount failed (continuing without it): %v", err)
	} else {
		fuseOK = true
		log.Printf("FUSE mounted at %s", mountPath)
	}

	if cfg.Prowlarr.URL != "" && cfg.Prowlarr.APIKey != "" {
		fallbackSearcher = fallback.New(cfg.Prowlarr.URL, cfg.Prowlarr.APIKey)
		log.Printf("Prowlarr fallback searcher enabled (%s)", cfg.Prowlarr.URL)
	} else {
		log.Printf("Prowlarr not configured — seed-based fallback disabled")
	}

	if cfg.Seeder.Enabled {
		mountAll := cfg.Decypharr.MountDir + "/__all__"
		sm, err := seeder.New(seeder.Config{
			MountDir:                 mountAll,
			StateDir:                 cfg.Seeder.StateDir,
			UploadLimitMBPS:          cfg.Seeder.UploadLimitMBPS,
			MaxActive:                cfg.Seeder.MaxActive,
			RotateAfter:              time.Duration(float64(time.Hour) * cfg.Seeder.RotateAfterHours),
			FavorYearFrom:            cfg.Seeder.FavorYearFrom,
			FavorYearTo:              cfg.Seeder.FavorYearTo,
			PrioritizeLowSeeders:     cfg.Seeder.PrioritizeLowSeeders,
			CampaignMinSeedMB:        cfg.Seeder.CampaignMinSeedMB,
			CampaignMaxSeedMB:        cfg.Seeder.CampaignMaxSeedMB,
			CampaignMinTime:          time.Duration(float64(time.Minute) * cfg.Seeder.CampaignMinTimeMinutes),
			CampaignMaxTime:          time.Duration(float64(time.Minute) * cfg.Seeder.CampaignMaxTimeMinutes),
			CampaignPeerWait:         time.Duration(cfg.Seeder.CampaignPeerWaitSeconds) * time.Second,
			CampaignChunkDir:         cfg.Seeder.CampaignChunkDir,
			CampaignChunkDirFallback: cfg.Seeder.CampaignChunkDirFallback,
			CampaignChunkSizeMB:      cfg.Seeder.CampaignChunkSizeMB,
			CampaignLoop:             cfg.Seeder.CampaignLoop,
			CampaignTBCycleDays:      cfg.Seeder.CampaignTBCycleDays,
			CampaignRDCycleDays:      cfg.Seeder.CampaignRDCycleDays,
		})
		if err != nil {
			log.Printf("seeder init failed: %v — seeding disabled", err)
		} else {
			seedMgr = sm
			log.Printf("seeder started: mount=%s max_active=%d upload=%dMB/s rotate=%.1fh",
				mountAll, cfg.Seeder.MaxActive, cfg.Seeder.UploadLimitMBPS, cfg.Seeder.RotateAfterHours)
		}
	}

	// Background goroutine: sync full debrid libraries to seeder queue every 15 min.
	// We pull from Decypharr (managed items), RD API, and TorBox API so that
	// everything on the user's debrid accounts is eligible for seeding, not just
	// the subset Decypharr actively manages.
	if seedMgr != nil {
		go func() {
			var dc *decypharr.Client
			if cfg.Decypharr.URL != "" {
				dc = decypharr.New(cfg.Decypharr.URL, cfg.Decypharr.MountDir)
			}
			rdKey := cfg.DebridAPIKeys["realdebrid"]
			tbKey := cfg.DebridAPIKeys["torbox"]
			seedProviders := cfg.Seeder.SeedFromProviders

			wantProvider := func(p string) bool {
				if len(seedProviders) == 0 {
					return true
				}
				for _, sp := range seedProviders {
					if sp == p {
						return true
					}
				}
				return false
			}

			syncSeeder := func() {
				// seenIdx maps lowercase hash → index in si for O(1) provider updates.
				seenIdx := make(map[string]int, 20000)
				si := make([]seeder.SeedItem, 0, 20000)

				add := func(hash, name, provider string) {
					h := strings.ToLower(hash)
					if h == "" {
						return
					}
					if idx, dup := seenIdx[h]; dup {
						// Upgrade provider if the existing entry has none.
						if provider != "" && si[idx].Provider == "" {
							si[idx].Provider = provider
						}
						return
					}
					seenIdx[h] = len(si)
					si = append(si, seeder.SeedItem{Hash: h, Name: name, Provider: provider})
				}

				// RD and TorBox first so their provider tags take precedence.
				// Full RD library (paginated; may be 15k+ items).
				if rdKey != "" && wantProvider("realdebrid") {
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
					rdClient := realdebrid.New(rdKey)
					if torrents, err := rdClient.ListTorrents(ctx); err == nil {
						for _, t := range torrents {
							add(t.Hash, t.Name, "realdebrid")
						}
						log.Printf("seeder sync: rd=%d", len(torrents))
					} else {
						log.Printf("seeder sync rd: %v", err)
					}
					cancel()
				}

				// Full TorBox library.
				if tbKey != "" && wantProvider("torbox") {
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					tbClient := torbox.New(tbKey)
					if torrents, err := tbClient.ListTorrents(ctx); err == nil {
						for _, t := range torrents {
							add(t.Hash, t.Name, "torbox")
						}
						log.Printf("seeder sync: torbox=%d", len(torrents))
					} else {
						log.Printf("seeder sync torbox: %v", err)
					}
					cancel()
				}

				// Decypharr last — catches any items not in the RD/TorBox API results.
				if dc != nil {
					ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					if items, err := dc.RawList(ctx); err == nil {
						for _, it := range items {
							add(it.Hash, it.Name, "")
						}
					} else {
						log.Printf("seeder sync decypharr: %v", err)
					}
					cancel()
				}

				log.Printf("seeder sync: total=%d unique items", len(si))
				seedMgr.Sync(si)
			}

			syncSeeder() // initial sync immediately
			ticker := time.NewTicker(15 * time.Minute)
			defer ticker.Stop()
			for range ticker.C {
				syncSeeder()
			}
		}()
	}

	handoffCtx, handoffCancel := context.WithCancel(context.Background())
	if cfg.Decypharr.URL != "" && cfg.Decypharr.MountDir != "" {
		dcClient = decypharr.New(cfg.Decypharr.URL, cfg.Decypharr.MountDir)
		w := handoff.New(mgr, dcClient, cfg.Decypharr.PollInterval)
		go w.Run(handoffCtx)
		log.Printf("Decypharr handoff watcher started (polling %s every %s)",
			cfg.Decypharr.URL, cfg.Decypharr.PollInterval)
	} else {
		log.Printf("Decypharr not configured — set url + mount_dir in flowarr.yaml to enable handoff")
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		uiHandler(w, r)
	})
	mux.HandleFunc("/api/v2/auth/login", authLogin)
	mux.HandleFunc("/api/v2/app/version", appVersion)
	mux.HandleFunc("/api/v2/app/webapiVersion", apiVersion)
	mux.HandleFunc("/api/v2/app/preferences", appPreferences)
	mux.HandleFunc("/api/v2/torrents/categories", torrentsCategories)
	mux.HandleFunc("/api/v2/torrents/createCategory", torrentsCreateCategory)
	mux.HandleFunc("/api/v2/torrents/editCategory", torrentsEditCategory)
	mux.HandleFunc("/api/v2/torrents/removeCategories", torrentsRemoveCategories)
	mux.HandleFunc("/api/v2/torrents/info", torrentsInfo)
	mux.HandleFunc("/api/v2/torrents/add", torrentsAdd)
	mux.HandleFunc("/api/v2/torrents/delete", torrentsDelete)
	mux.HandleFunc("/api/health", health)
	mux.HandleFunc("/api/settings", apiSettings)
	mux.HandleFunc("/api/flowarr/queue", apiQueue)
	mux.HandleFunc("/api/flowarr/logs", apiLogs)
	mux.HandleFunc("/api/flowarr/rdlibrary", apiRDLibrary)
	mux.HandleFunc("/api/flowarr/tblibrary", apiTBLibrary)
	mux.HandleFunc("/api/flowarr/tbqueue", apiTBQueue)
	mux.HandleFunc("/api/flowarr/browse", apiBrowse)
	mux.HandleFunc("/api/flowarr/stream", apiStream)
	mux.HandleFunc("/api/flowarr/seeder", apiSeederList)
	mux.HandleFunc("/api/flowarr/seeder/settings", apiSeederSettings)
	mux.HandleFunc("/api/flowarr/seeder/add", apiSeederAdd)
	mux.HandleFunc("/api/flowarr/seeder/remove", apiSeederRemove)
	mux.HandleFunc("/api/flowarr/seeder/pause", apiSeederPause)
	mux.HandleFunc("/api/flowarr/seeder/resume", apiSeederResume)
	mux.HandleFunc("/api/flowarr/seeder/prioritize", apiSeederPrioritize)
	mux.HandleFunc("/api/flowarr/seeder/reset-cycle", apiSeederResetCycle)
	mux.HandleFunc("/api/flowarr/seeder/reset-stats", apiSeederResetStats)
	mux.HandleFunc("/api/flowarr/mirror", apiMirrorToProvider)
	mux.HandleFunc("/api/flowarr/mirror/transfer", apiMirrorTransferRD)

	logged := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Printf("%s %s %s", r.RemoteAddr, r.Method, r.URL.Path)
		mux.ServeHTTP(w, r)
	})
	srv := &http.Server{Addr: cfg.Server.Addr, Handler: logged}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		log.Println("shutting down...")
		handoffCancel()
		srv.Shutdown(context.Background())
		if fuseServer != nil {
			fuseServer.Unmount()
		}
		mgr.Close()
		if seedMgr != nil {
			seedMgr.Close()
		}
		os.Exit(0)
	}()

	log.SetOutput(logTee{w: os.Stderr})
	log.Printf("Flowarr v%s starting on %s", version, cfg.Server.Addr)
	log.Fatal(srv.ListenAndServe())
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
