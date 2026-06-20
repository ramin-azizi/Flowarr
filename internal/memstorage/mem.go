// Package memstorage provides an in-memory anacrolix/torrent storage backend.
// Pieces are stored in RAM only; when total usage exceeds the configured cap,
// the oldest fully-downloaded pieces are evicted. Evicted pieces are re-fetched
// from peers if Plex seeks back into them.
package memstorage

import (
	"context"
	"sync"
	"sync/atomic"

	g "github.com/anacrolix/generics"
	"github.com/anacrolix/torrent/metainfo"
	"github.com/anacrolix/torrent/storage"
)

// Client is an in-memory storage backend with an optional byte cap.
// Pass cap ≤ 0 to disable the cap (unlimited RAM).
type Client struct {
	capBytes int64 // 0 = unlimited

	mu        sync.Mutex
	totalUsed int64 // bytes currently allocated across all pieces
	// eviction queue: pieces in insertion order; front = oldest
	queue []*piece
}

// New returns a Client that evicts completed pieces when total RAM exceeds capBytes.
func New(capBytes int64) *Client {
	return &Client{capBytes: capBytes}
}

func (c *Client) OpenTorrent(_ context.Context, info *metainfo.Info, _ metainfo.Hash) (storage.TorrentImpl, error) {
	n := info.NumPieces()
	pieces := make([]piece, n)
	for i := 0; i < n; i++ {
		pieces[i].c = c
		pieces[i].size = info.Piece(i).Length()
	}
	return storage.TorrentImpl{
		Piece: func(p metainfo.Piece) storage.PieceImpl {
			return &pieces[p.Index()]
		},
		PieceWithHash: func(p metainfo.Piece, _ g.Option[[]byte]) storage.PieceImpl {
			return &pieces[p.Index()]
		},
		Close: func() error {
			// Release all memory held by this torrent.
			c.mu.Lock()
			for i := range pieces {
				if pieces[i].data != nil {
					c.totalUsed -= int64(len(pieces[i].data))
					pieces[i].data = nil
				}
			}
			c.mu.Unlock()
			return nil
		},
	}, nil
}

func (c *Client) Close() error { return nil }

// evict removes the oldest complete piece (no-op if nothing is evictable).
// Must be called with c.mu held.
func (c *Client) evict() {
	for i, p := range c.queue {
		p.mu.Lock()
		if p.complete && p.data != nil {
			freed := int64(len(p.data))
			p.data = nil
			p.complete = false
			c.totalUsed -= freed
			p.mu.Unlock()
			// Remove from queue.
			c.queue = append(c.queue[:i], c.queue[i+1:]...)
			return
		}
		p.mu.Unlock()
	}
}

type piece struct {
	c        *Client
	mu       sync.RWMutex
	data     []byte
	size     int64
	complete bool
	// queued tracks whether this piece is in the eviction queue.
	queued atomic.Bool
}

func (p *piece) ReadAt(buf []byte, off int64) (int, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.data == nil {
		// Piece was evicted or never written — return zeros so anacrolix
		// can detect it is incomplete via Completion().
		clear(buf)
		return len(buf), nil
	}
	n := copy(buf, p.data[off:])
	return n, nil
}

func (p *piece) WriteAt(buf []byte, off int64) (int, error) {
	p.mu.Lock()
	if p.data == nil {
		p.data = make([]byte, p.size)
		p.c.mu.Lock()
		p.c.totalUsed += int64(p.size)
		// Evict until we're under cap.
		for p.c.capBytes > 0 && p.c.totalUsed > p.c.capBytes {
			p.c.evict()
		}
		p.c.mu.Unlock()
	}
	n := copy(p.data[off:], buf)
	p.mu.Unlock()
	return n, nil
}

func (p *piece) MarkComplete() error {
	p.mu.Lock()
	p.complete = true
	p.mu.Unlock()
	// Register in eviction queue (once).
	if p.queued.CompareAndSwap(false, true) {
		p.c.mu.Lock()
		p.c.queue = append(p.c.queue, p)
		p.c.mu.Unlock()
	}
	return nil
}

func (p *piece) MarkNotComplete() error {
	p.mu.Lock()
	p.complete = false
	p.mu.Unlock()
	return nil
}

func (p *piece) Completion() storage.Completion {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return storage.Completion{Complete: p.complete, Ok: true}
}
