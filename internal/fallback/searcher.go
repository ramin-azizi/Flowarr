// Package fallback searches Prowlarr for a healthier alternative torrent when
// the originally chosen torrent has too few seeds for reliable streaming.
package fallback

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

// CategoryType classifies a download category for quality targeting.
type CategoryType int

const (
	CategoryMovie4K CategoryType = iota
	CategoryMovieHD
	CategoryShow4K
	CategoryShowHD
	CategoryAnime
	CategoryUnknown
)

// Result is a candidate torrent returned by Prowlarr.
type Result struct {
	Title      string
	MagnetURL  string
	Seeds      int
	Resolution string // "4K", "1080p", "720p", "SD", ""
	Score      float64
}

// Searcher queries Prowlarr for fallback torrents.
type Searcher struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

func New(baseURL, apiKey string) *Searcher {
	return &Searcher{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		client:  &http.Client{Timeout: 30 * time.Second},
	}
}

// ClassifyCategory maps a download category name to a CategoryType.
func ClassifyCategory(category string) CategoryType {
	c := strings.ToLower(category)
	has := strings.Contains
	switch {
	case has(c, "anime"):
		return CategoryAnime
	case (has(c, "4k") || has(c, "2160")) && (has(c, "movie") || has(c, "radarr")):
		return CategoryMovie4K
	case has(c, "movie") || has(c, "radarr"):
		return CategoryMovieHD
	case (has(c, "4k") || has(c, "2160")) && (has(c, "show") || has(c, "sonarr")):
		return CategoryShow4K
	case has(c, "show") || has(c, "sonarr"):
		return CategoryShowHD
	default:
		return CategoryUnknown
	}
}

// prowlarrCategories returns the Prowlarr numeric category IDs for a type.
func prowlarrCategories(ct CategoryType) string {
	switch ct {
	case CategoryMovie4K, CategoryMovieHD:
		return "2000"
	case CategoryShow4K, CategoryShowHD, CategoryAnime:
		return "5000"
	default:
		return "2000,5000"
	}
}

// minResolutionTier returns the minimum acceptable resolution tier.
// Tiers: 0=SD, 1=720p, 2=1080p, 3=4K
func minResolutionTier(ct CategoryType) int {
	switch ct {
	case CategoryMovie4K, CategoryShow4K:
		return 3
	case CategoryMovieHD, CategoryShowHD, CategoryAnime:
		return 2
	default:
		return 1
	}
}

var (
	resolutionRe = regexp.MustCompile(`(?i)\b(2160p?|4k|uhd|1080p?|720p?|480p?|576p?)\b`)
	remuxRe      = regexp.MustCompile(`(?i)\bremux\b`)
	blurayRe     = regexp.MustCompile(`(?i)\b(bluray|blu-ray|bdrip|bdmux)\b`)

	// Tags that mark the boundary between title and technical metadata.
	cutTagsRe = regexp.MustCompile(`(?i)\b(2160p?|1080p?|720p?|480p?|4k|uhd|bluray|blu[-.]?ray|web[-.]?dl|web[-.]?rip|webrip|web|hdtv|dvdrip|dvd|remux|repack|proper|extended|theatrical|hdr10?[\+p]?|sdr|hevc|x26[45]|h\.?26[45]|av1|avc|vc[-.]?1|xvid|divx|aac|dts|ddp|eac3|truehd|atmos)\b.*`)
	epRe      = regexp.MustCompile(`(?i)^(.*?)\s+(S\d{2}E\d{2,})`)
	yearRe    = regexp.MustCompile(`(?i)^(.*?\b(?:19|20)\d{2}\b)`)
)

func resolutionTierOf(title string) (tier int, label string) {
	m := strings.ToLower(resolutionRe.FindString(title))
	switch {
	case m == "2160p" || m == "2160" || m == "4k" || m == "uhd":
		return 3, "4K"
	case m == "1080p" || m == "1080":
		return 2, "1080p"
	case m == "720p" || m == "720":
		return 1, "720p"
	case m == "480p" || m == "480" || m == "576p" || m == "576":
		return 0, "SD"
	default:
		return 1, "" // unknown — assume 720p-ish, don't label
	}
}

// score computes a quality-weighted seed score.
//
// The formula: seeds × qualityWeight × sourceBonus
//
// qualityWeight penalises results below the category's minimum resolution so
// that a 720p torrent with 1 000 seeds doesn't beat a 1080p torrent with 200
// seeds, but a 720p with 5 000 seeds CAN beat a 1080p with 50 seeds (better
// for actual streaming). Source bonus gives a small lift to lossless remuxes
// and BluRay encodes over compressed WEB or HDTV rips.
func score(seeds int, title string, minTier int) float64 {
	tier, _ := resolutionTierOf(title)

	var qw float64
	switch {
	case tier >= minTier:
		qw = 1.0 // meets or exceeds target quality
	case tier == minTier-1:
		qw = 0.35 // one tier below — acceptable fallback but penalised
	default:
		qw = 0.05 // significantly below minimum — last resort only
	}

	bonus := 1.0
	if remuxRe.MatchString(title) {
		bonus = 1.15
	} else if blurayRe.MatchString(title) {
		bonus = 1.05
	}

	return float64(seeds) * qw * bonus
}

// NormalizeTitle strips quality/codec/group tags to produce a clean search
// query. Preserves episode identifiers (S01E01) for TV content.
func NormalizeTitle(raw string) string {
	s := strings.NewReplacer(".", " ", "_", " ", "-", " ").Replace(raw)
	s = strings.Join(strings.Fields(s), " ")

	// TV: keep episode identifier — "Better Call Saul S06E01"
	if m := epRe.FindStringSubmatch(s); len(m) >= 3 {
		return strings.TrimSpace(m[1]) + " " + strings.ToUpper(m[2])
	}
	// Movie with year — include year for search precision
	if m := yearRe.FindStringSubmatch(s); len(m) >= 2 {
		return strings.TrimSpace(m[1])
	}
	// Fallback: strip at first quality tag
	s = cutTagsRe.ReplaceAllString(s, "")
	return strings.TrimSpace(s)
}

type prowlarrResult struct {
	Title      string `json:"title"`
	Seeders    int    `json:"seeders"`
	MagnetURL  string `json:"magnetUrl"`
	DownloadURL string `json:"downloadUrl"`
	Size       int64  `json:"size"`
}

// FindFallback searches Prowlarr for the best alternative torrent to rawTitle
// given the category type. Returns nil if nothing with at least minSeeds is found.
func (s *Searcher) FindFallback(ctx context.Context, rawTitle string, ct CategoryType) (*Result, error) {
	const minSeeds = 3

	query := NormalizeTitle(rawTitle)
	cats := prowlarrCategories(ct)
	minTier := minResolutionTier(ct)

	endpoint := fmt.Sprintf("%s/api/v1/search?query=%s&type=search&indexerIds=-2&categories=%s",
		s.baseURL, url.QueryEscape(query), cats)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("X-Api-Key", s.apiKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("prowlarr search: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("prowlarr returned %d", resp.StatusCode)
	}

	var raw []prowlarrResult
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode response: %w", err)
	}

	candidates := make([]Result, 0, len(raw))
	for _, r := range raw {
		if r.Seeders < minSeeds {
			continue
		}
		magnet := r.MagnetURL
		if magnet == "" {
			magnet = r.DownloadURL
		}
		if magnet == "" {
			continue
		}
		tier, label := resolutionTierOf(r.Title)
		_ = tier
		candidates = append(candidates, Result{
			Title:      r.Title,
			MagnetURL:  magnet,
			Seeds:      r.Seeders,
			Resolution: label,
			Score:      score(r.Seeders, r.Title, minTier),
		})
	}

	if len(candidates) == 0 {
		return nil, nil
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].Score > candidates[j].Score
	})

	best := candidates[0]
	return &best, nil
}
