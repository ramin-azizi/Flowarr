// Package torbox provides a minimal TorBox API client for the fast mirror feature.
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

// LibraryHashes returns the set of lowercase infohashes already in the user's TorBox library.
func (c *Client) LibraryHashes(ctx context.Context) (map[string]struct{}, error) {
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet,
		baseURL+"/api/torrents/mylist?bypass_cache=true", nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("torbox mylist: %w", err)
	}
	defer resp.Body.Close()
	var body struct {
		Success bool `json:"success"`
		Data    []struct {
			Hash string `json:"hash"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, fmt.Errorf("torbox mylist decode: %w", err)
	}
	out := make(map[string]struct{}, len(body.Data))
	for _, it := range body.Data {
		out[strings.ToLower(it.Hash)] = struct{}{}
	}
	return out, nil
}

// CheckCached accepts up to 100 lowercase infohashes and returns the subset
// that TorBox has globally cached (i.e. an instant add, no download needed).
func (c *Client) CheckCached(ctx context.Context, hashes []string) (map[string]struct{}, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	url := baseURL + "/api/torrents/checkcached?type=torrent&list_files=false&hash=" +
		strings.Join(hashes, ",")
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("torbox checkcached: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var body struct {
		Success bool                       `json:"success"`
		Data    map[string]json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("torbox checkcached decode: %w", err)
	}
	out := make(map[string]struct{}, len(body.Data))
	for h := range body.Data {
		out[strings.ToLower(h)] = struct{}{}
	}
	return out, nil
}

// AddByHash submits a hash-only magnet to TorBox. If the content is cached,
// TorBox resolves it instantly without any download.
func (c *Client) AddByHash(ctx context.Context, hash string) error {
	var buf bytes.Buffer
	mw := multipart.NewWriter(&buf)
	_ = mw.WriteField("magnet", "magnet:?xt=urn:btih:"+hash)
	_ = mw.WriteField("seed", "3") // long-term seeding
	mw.Close()
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/api/torrents/createtorrent", &buf)
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("torbox add %s: %w", hash, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("torbox add %s: status %d: %s", hash, resp.StatusCode, string(b))
	}
	return nil
}
