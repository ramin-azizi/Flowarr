// Package torbox provides a minimal TorBox API client.
package torbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"strings"
	"time"
)

const baseURL = "https://api.torbox.app/v1"

// ErrRateLimit is returned when TorBox responds with 429.
type ErrRateLimit struct{ RetryAfter time.Duration }

func (e ErrRateLimit) Error() string { return "torbox: rate limited" }

type Client struct {
	apiKey string
	http   *http.Client
}

func New(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		http:   &http.Client{Timeout: 30 * time.Second},
	}
}

// TorrentItem is a rich torrent entry from the TorBox library.
type TorrentItem struct {
	ID            int     `json:"id"`
	Hash          string  `json:"hash"`
	Name          string  `json:"name"`
	Size          int64   `json:"size"`
	DownloadState string  `json:"download_state"`
	Progress      float64 `json:"progress"`
	DownloadSpeed int64   `json:"download_speed"`
	UploadSpeed   int64   `json:"upload_speed"`
	Seeds         int     `json:"seeds"`
	Peers         int     `json:"peers"`
	Ratio         float64 `json:"ratio"`
	ETA           int64   `json:"eta"`
	FilesCount    int     `json:"files_count"`
	CreatedAt     string  `json:"created_at"`
}

// ListTorrents returns all torrents in the TorBox library with full metadata.
func (c *Client) ListTorrents(ctx context.Context) ([]TorrentItem, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		baseURL+"/api/torrents/mylist?bypass_cache=true", nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("torbox mylist: %w", err)
	}
	defer resp.Body.Close()
	var body struct {
		Success bool          `json:"success"`
		Data    []TorrentItem `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("torbox mylist decode: %w", err)
	}
	return body.Data, nil
}

// LibraryHashes returns the set of lowercase infohashes already in the user's TorBox library.
func (c *Client) LibraryHashes(ctx context.Context) (map[string]struct{}, error) {
	items, err := c.ListTorrents(ctx)
	if err != nil {
		return nil, err
	}
	out := make(map[string]struct{}, len(items))
	for _, it := range items {
		out[strings.ToLower(it.Hash)] = struct{}{}
	}
	return out, nil
}

// CheckCachedBatch accepts any number of lowercase infohashes and returns the subset
// that TorBox has globally cached. Uses POST to send all hashes in one request.
func (c *Client) CheckCachedBatch(ctx context.Context, hashes []string) (map[string]struct{}, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	body, _ := json.Marshal(map[string]interface{}{
		"hashes": hashes,
		"format": "object",
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/api/torrents/checkcached", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("torbox checkcached: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var result struct {
		Success bool                       `json:"success"`
		Data    map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, fmt.Errorf("torbox checkcached decode: %w", err)
	}
	out := make(map[string]struct{}, len(result.Data))
	for h := range result.Data {
		out[strings.ToLower(h)] = struct{}{}
	}
	return out, nil
}

// CheckCached is kept for compatibility; delegates to CheckCachedBatch.
func (c *Client) CheckCached(ctx context.Context, hashes []string) (map[string]struct{}, error) {
	return c.CheckCachedBatch(ctx, hashes)
}

// QueuedItem is an entry in the TorBox download queue (not yet active).
type QueuedItem struct {
	ID        int    `json:"id"`
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	Magnet    string `json:"magnet"`
	Type      string `json:"type"`
	CreatedAt string `json:"created_at"`
}

// ListQueued returns torrents waiting in the TorBox download queue.
func (c *Client) ListQueued(ctx context.Context) ([]QueuedItem, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		baseURL+"/api/queued/getqueued?type=torrent", nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("torbox getqueued: %w", err)
	}
	defer resp.Body.Close()
	var body struct {
		Success bool         `json:"success"`
		Data    []QueuedItem `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("torbox getqueued decode: %w", err)
	}
	return body.Data, nil
}

// AddByHash submits a hash-only magnet to TorBox using asynccreatetorrent (fire-and-forget).
// Pass asQueued=true for uncached items to skip auto-download check.
// Returns ErrRateLimit on 429.
func (c *Client) AddByHash(ctx context.Context, hash string, asQueued bool) error {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("magnet", "magnet:?xt=urn:btih:"+hash)
	if asQueued {
		_ = mw.WriteField("as_queued", "true")
	}
	mw.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/api/torrents/asynccreatetorrent", &buf)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("torbox add %s: %w", hash, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return ErrRateLimit{RetryAfter: 65 * time.Second}
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("torbox add %s: status %d: %s", hash, resp.StatusCode, string(b))
	}
	return nil
}

// AddByURL submits an HTTP URL to TorBox's web-download endpoint.
// Used to transfer content from RD's CDN directly into TorBox.
func (c *Client) AddByURL(ctx context.Context, url string) error {
	body, _ := json.Marshal(map[string]string{"link": url})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/api/webdl/createwebdownload", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("torbox webdl: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return ErrRateLimit{RetryAfter: 10 * time.Second}
	}
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("torbox webdl: status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}
