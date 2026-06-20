package config

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server           ServerConfig      `yaml:"server"`
	Torrent          TorrentConfig     `yaml:"torrent"`
	FUSE             FUSEConfig        `yaml:"fuse"`
	Library          LibraryConfig     `yaml:"library"`
	Decypharr        DecypharrConfig   `yaml:"decypharr"`
	Prowlarr         ProwlarrConfig    `yaml:"prowlarr"`
	Arrs             []ArrInstance     `yaml:"arrs"`
	CategoryMappings map[string]string `yaml:"category_mappings"`
	Streaming        StreamingConfig   `yaml:"streaming"`
	Seeder           SeederConfig      `yaml:"seeder"`
	Download         DownloadConfig    `yaml:"download"`
}

type ServerConfig struct {
	Addr string `yaml:"addr"`
}

type TorrentConfig struct {
	DataDir string `yaml:"data_dir"`
}

type FUSEConfig struct {
	MountDir string `yaml:"mount_dir"`
}

type LibraryConfig struct {
	BaseDir string `yaml:"base_dir"`
}

type DecypharrConfig struct {
	URL          string        `yaml:"url"`
	MountDir     string        `yaml:"mount_dir"`
	PollInterval time.Duration `yaml:"poll_interval"`
}

type ProwlarrConfig struct {
	URL    string `yaml:"url"`
	APIKey string `yaml:"api_key"`
}

// ArrInstance describes a Radarr or Sonarr instance that Flowarr interacts with.
type ArrInstance struct {
	Name       string   `yaml:"name"`
	URL        string   `yaml:"url"`
	APIKey     string   `yaml:"api_key"`
	Categories []string `yaml:"categories"`
}

// SeederConfig controls seeding and the integrated cache-campaign.
// The campaign cycles through every torrent, seeding each one until it hits
// CampaignMinSeedMB of upload AND CampaignMinTimeMinutes has passed (or either
// hard cap is reached). Torrents that attract no peers after
// CampaignPeerWaitSeconds get chunks pulled directly from the Decypharr FUSE
// mount to keep them warm in RD's cache.
type SeederConfig struct {
	Enabled              bool    `yaml:"enabled"`
	StateDir             string  `yaml:"state_dir"`
	UploadLimitMBPS      int     `yaml:"upload_limit_mbps"`
	MaxActive            int     `yaml:"max_active"`
	RotateAfterHours     float64 `yaml:"rotate_after_hours"`
	FavorYearFrom        int     `yaml:"favor_year_from"`
	FavorYearTo          int     `yaml:"favor_year_to"`
	PrioritizeLowSeeders bool    `yaml:"prioritize_low_seeders"`

	// Campaign settings — all zero means campaign disabled.
	CampaignMinSeedMB        float64 `yaml:"campaign_min_seed_mb"`
	CampaignMaxSeedMB        float64 `yaml:"campaign_max_seed_mb"`
	CampaignMinTimeMinutes   float64 `yaml:"campaign_min_time_minutes"`
	CampaignMaxTimeMinutes   float64 `yaml:"campaign_max_time_minutes"`
	CampaignPeerWaitSeconds  int     `yaml:"campaign_peer_wait_seconds"`
	CampaignChunkDir         string  `yaml:"campaign_chunk_dir"`
	CampaignChunkDirFallback string  `yaml:"campaign_chunk_dir_fallback"`
	CampaignChunkSizeMB      float64 `yaml:"campaign_chunk_size_mb"`
	CampaignLoop             bool    `yaml:"campaign_loop"`
}

// AllowDownloads controls whether Flowarr accepts torrent download requests
// from arr apps. Set false to use Flowarr purely for seeding.
type DownloadConfig struct {
	Enabled bool `yaml:"enabled"`
}

// StreamingConfig controls on-demand streaming behaviour and fallback search.
type StreamingConfig struct {
	SeedThreshold    int           `yaml:"seed_threshold"`
	HealthCheckDelay time.Duration `yaml:"health_check_delay"`
	ReadaheadMB      int           `yaml:"readahead_mb"`
	ExtraTrackers    []string      `yaml:"extra_trackers"`
	ProwlarrIndexIDs []int         `yaml:"prowlarr_indexer_ids"`
}

func Defaults() *Config {
	return &Config{
		Server:           ServerConfig{Addr: ":8888"},
		Torrent:          TorrentConfig{DataDir: "./downloads"},
		FUSE:             FUSEConfig{MountDir: "./flowarr-mount"},
		Library:          LibraryConfig{BaseDir: "/mnt/library"},
		CategoryMappings: map[string]string{},
		Decypharr: DecypharrConfig{
			PollInterval: 30 * time.Second,
		},
		Streaming: StreamingConfig{
			SeedThreshold:    5,
			HealthCheckDelay: 45 * time.Second,
			ReadaheadMB:      5,
		},
		Seeder: SeederConfig{
			Enabled:                 false,
			StateDir:                "./seed-state",
			UploadLimitMBPS:         5,
			MaxActive:               5,
			RotateAfterHours:        24,
			CampaignMinSeedMB:       50,
			CampaignMaxSeedMB:       500,
			CampaignMinTimeMinutes:  5,
			CampaignMaxTimeMinutes:  30,
			CampaignPeerWaitSeconds: 60,
			CampaignChunkDir:        "/dev/shm",
			CampaignChunkSizeMB:     100,
		},
		Download: DownloadConfig{
			Enabled: true,
		},
	}
}

// Load reads path, merges over defaults, then applies env-var overrides.
// If the file doesn't exist the defaults are returned with no error.
func Load(path string) (*Config, error) {
	cfg := Defaults()

	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err == nil {
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("parse config %s: %w", path, err)
		}
	}
	if cfg.CategoryMappings == nil {
		cfg.CategoryMappings = map[string]string{}
	}

	applyEnv(cfg)
	return cfg, nil
}

// ResolvedSavePath returns the library path for symlink creation. If a
// category mapping is defined for the given category, the mapped folder name
// under LibraryBaseDir is used; otherwise savePath is returned unchanged.
func (cfg *Config) ResolvedSavePath(savePath, category string) string {
	if mapped, ok := cfg.CategoryMappings[category]; ok && mapped != "" {
		return filepath.Join(cfg.Library.BaseDir, mapped)
	}
	return savePath
}

// applyEnv overlays environment variables on top of file-based config.
func applyEnv(cfg *Config) {
	if v := os.Getenv("FLOWARR_ADDR"); v != "" {
		cfg.Server.Addr = v
	}
	if v := os.Getenv("FLOWARR_DATA_DIR"); v != "" {
		cfg.Torrent.DataDir = v
	}
	if v := os.Getenv("FLOWARR_MOUNT_DIR"); v != "" {
		cfg.FUSE.MountDir = v
	}
	if v := os.Getenv("FLOWARR_LIBRARY_DIR"); v != "" {
		cfg.Library.BaseDir = v
	}
	if v := os.Getenv("DECYPHARR_URL"); v != "" {
		cfg.Decypharr.URL = v
	}
	if v := os.Getenv("DECYPHARR_MOUNT"); v != "" {
		cfg.Decypharr.MountDir = v
	}
	if v := os.Getenv("DECYPHARR_POLL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.Decypharr.PollInterval = d
		}
	}
	if v := os.Getenv("PROWLARR_URL"); v != "" {
		cfg.Prowlarr.URL = v
	}
	if v := os.Getenv("PROWLARR_API_KEY"); v != "" {
		cfg.Prowlarr.APIKey = v
	}
}
