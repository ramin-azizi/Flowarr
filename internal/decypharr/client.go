// Package decypharr polls a Decypharr instance (which exposes a
// qBittorrent-compatible API) to discover which infohashes are available
// on Real-Debrid and ready for handoff.
package decypharr

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Torrent is the subset of qBittorrent torrent info we care about.
type Torrent struct {
	Hash     string  `json:"hash"`
	Name     string  `json:"name"`
	Progress float64 `json:"progress"`
	State    string  `json:"state"`
}

// Item is the full Decypharr torrent record used for UI display.
type Item struct {
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
}

// Client queries a Decypharr instance.
type Client struct {
	baseURL  string
	mountDir string // e.g. /mnt/decypharr — where rclone/Decypharr serves files
	http     *http.Client
}

func New(baseURL, mountDir string) *Client {
	return &Client{
		baseURL:  baseURL,
		mountDir: mountDir,
		http:     &http.Client{Timeout: 10 * time.Second},
	}
}

func (c *Client) MountDir() string { return c.mountDir }

// Forward sends a magnet link to Decypharr so RD caches it in parallel with
// Flowarr's BitTorrent download. Errors are non-fatal — Flowarr still streams
// locally even if the forward fails.
func (c *Client) Forward(ctx context.Context, magnet, category, savePath string) error {
	endpoint := c.baseURL + "/api/v2/torrents/add"
	body := url.Values{}
	body.Set("urls", magnet)
	if category != "" {
		body.Set("category", category)
	}
	if savePath != "" {
		body.Set("savepath", savePath)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
		strings.NewReader(body.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("decypharr forward: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("decypharr forward: status %d", resp.StatusCode)
	}
	return nil
}

// ForwardToProvider submits a magnet to Decypharr targeting a specific debrid provider.
// Used to mirror RD library items to a secondary provider (e.g. TorBox).
func (c *Client) ForwardToProvider(ctx context.Context, magnet, category, provider string) error {
	endpoint := c.baseURL + "/api/v2/torrents/add"
	body := url.Values{}
	body.Set("urls", magnet)
	if category != "" {
		body.Set("category", category)
	}
	if provider != "" {
		body.Set("debrid", provider)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint,
		strings.NewReader(body.Encode()))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("decypharr forward to %s: %w", provider, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("decypharr forward to %s: status %d", provider, resp.StatusCode)
	}
	return nil
}

// RawList fetches the full torrent list from Decypharr for UI display.
func (c *Client) RawList(ctx context.Context) ([]Item, error) {
	url := c.baseURL + "/api/v2/torrents/info"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("decypharr GET %s: %w", url, err)
	}
	defer resp.Body.Close()
	var items []Item
	if err := json.NewDecoder(resp.Body).Decode(&items); err != nil {
		return nil, fmt.Errorf("decypharr decode: %w", err)
	}
	return items, nil
}

// CachedHashes returns all torrents Decypharr considers complete (cached on RD),
// keyed by lowercase infohash hex.
func (c *Client) CachedHashes(ctx context.Context) (map[string]Torrent, error) {
	url := c.baseURL + "/api/v2/torrents/info"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("decypharr GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("decypharr GET %s: status %d", url, resp.StatusCode)
	}

	var torrents []Torrent
	if err := json.NewDecoder(resp.Body).Decode(&torrents); err != nil {
		return nil, fmt.Errorf("decypharr decode: %w", err)
	}

	out := make(map[string]Torrent, len(torrents))
	for _, t := range torrents {
		// Consider a torrent cached when it's fully downloaded or seeding.
		if t.Progress >= 1.0 || t.State == "uploading" || t.State == "stalledUP" {
			out[t.Hash] = t
		}
	}
	return out, nil
}
