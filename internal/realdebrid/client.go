// Package realdebrid provides a minimal Real-Debrid API client.
package realdebrid

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const baseURL = "https://api.real-debrid.com/rest/1.0"

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

// Torrent is a single RD library entry.
type Torrent struct {
	ID     string   `json:"id"`
	Hash   string   `json:"hash"`
	Name   string   `json:"filename"`
	Status string   `json:"status"`
	Links  []string `json:"links"` // RD-restricted download links
}

// ListTorrents returns all torrents in the user's RD library (pages automatically).
func (c *Client) ListTorrents(ctx context.Context) ([]Torrent, error) {
	var all []Torrent
	offset := 0
	for {
		u := fmt.Sprintf("%s/torrents?limit=500&offset=%d", baseURL, offset)
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, fmt.Errorf("rd list torrents: %w", err)
		}
		var page []Torrent
		if err := json.NewDecoder(resp.Body).Decode(&page); err != nil {
			resp.Body.Close()
			return nil, fmt.Errorf("rd list torrents decode: %w", err)
		}
		resp.Body.Close()
		all = append(all, page...)
		if len(page) < 500 {
			break
		}
		offset += 500
	}
	return all, nil
}

// UnrestrictLink converts an RD-restricted link to a direct download URL.
func (c *Client) UnrestrictLink(ctx context.Context, link string) (string, error) {
	body := url.Values{}
	body.Set("link", link)
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
		baseURL+"/unrestrict/link", strings.NewReader(body.Encode()))
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := c.http.Do(req)
	if err != nil {
		return "", fmt.Errorf("rd unrestrict: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("rd unrestrict: status %d", resp.StatusCode)
	}
	var out struct {
		Download string `json:"download"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", fmt.Errorf("rd unrestrict decode: %w", err)
	}
	if out.Download == "" {
		return "", fmt.Errorf("rd unrestrict: empty download URL")
	}
	return out.Download, nil
}
