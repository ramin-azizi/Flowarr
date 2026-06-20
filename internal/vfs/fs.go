package vfs

import (
	"context"
	"io"
	"strings"
	"sync"
	"syscall"

	libt "github.com/anacrolix/torrent"
	gofuse "github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/ramin-azizi/flowarr/internal/torrent"
)

// Mount mounts the Flowarr virtual filesystem at mountpoint and returns the
// FUSE server. readaheadMB controls how far ahead of Plex's read position
// pieces are pre-fetched; 0 uses the default (5 MB).
func Mount(mountpoint string, mgr *torrent.Manager, readaheadMB int) (*fuse.Server, error) {
	if readaheadMB <= 0 {
		readaheadMB = 5
	}
	opts := &fsOpts{readaheadBytes: int64(readaheadMB) << 20}
	root := &rootNode{mgr: mgr, opts: opts}
	return gofuse.Mount(mountpoint, root, &gofuse.Options{})
}

// fsOpts is shared (read-only) across all nodes in the FUSE tree.
type fsOpts struct {
	readaheadBytes int64
}

// ── root ─────────────────────────────────────────────────────────────────────

type rootNode struct {
	gofuse.Inode
	mgr  *torrent.Manager
	opts *fsOpts
}

func (r *rootNode) Getattr(_ context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0755
	return 0
}

func (r *rootNode) Readdir(_ context.Context) (gofuse.DirStream, syscall.Errno) {
	list := r.mgr.List()
	entries := make([]fuse.DirEntry, 0, len(list))
	for _, t := range list {
		if t.Info() == nil {
			continue
		}
		entries = append(entries, fuse.DirEntry{
			Name: t.Name(),
			Mode: syscall.S_IFDIR,
			Ino:  torrentIno(t),
		})
	}
	return gofuse.NewListDirStream(entries), 0
}

func (r *rootNode) Lookup(ctx context.Context, name string, _ *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	for _, t := range r.mgr.List() {
		if t.Info() == nil || t.Name() != name {
			continue
		}
		child := r.NewInode(ctx, &torrentDir{t: t, prefix: "", opts: r.opts}, gofuse.StableAttr{
			Mode: syscall.S_IFDIR,
			Ino:  torrentIno(t),
		})
		return child, 0
	}
	return nil, syscall.ENOENT
}

var _ = (gofuse.NodeGetattrer)((*rootNode)(nil))
var _ = (gofuse.NodeReaddirer)((*rootNode)(nil))
var _ = (gofuse.NodeLookuper)((*rootNode)(nil))

// ── torrent directory ─────────────────────────────────────────────────────────

type torrentDir struct {
	gofuse.Inode
	t      *libt.Torrent
	prefix string
	opts   *fsOpts
}

func (d *torrentDir) Getattr(_ context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = syscall.S_IFDIR | 0755
	return 0
}

func (d *torrentDir) Readdir(_ context.Context) (gofuse.DirStream, syscall.Errno) {
	seen := make(map[string]struct{})
	var entries []fuse.DirEntry

	for i, f := range d.t.Files() {
		rel := f.DisplayPath()

		var remainder string
		if d.prefix == "" {
			remainder = rel
		} else {
			pfx := d.prefix + "/"
			if !strings.HasPrefix(rel, pfx) {
				continue
			}
			remainder = rel[len(pfx):]
		}

		slash := strings.IndexByte(remainder, '/')
		var name string
		isDir := slash != -1
		if isDir {
			name = remainder[:slash]
		} else {
			name = remainder
		}

		if _, dup := seen[name]; dup {
			continue
		}
		seen[name] = struct{}{}

		if isDir {
			entries = append(entries, fuse.DirEntry{
				Name: name,
				Mode: syscall.S_IFDIR,
				Ino:  pathIno(d.t, joinPath(d.prefix, name)),
			})
		} else {
			entries = append(entries, fuse.DirEntry{
				Name: name,
				Mode: syscall.S_IFREG,
				Ino:  fileIno(d.t, i),
			})
		}
	}
	return gofuse.NewListDirStream(entries), 0
}

func (d *torrentDir) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*gofuse.Inode, syscall.Errno) {
	childRel := joinPath(d.prefix, name)

	for i, f := range d.t.Files() {
		rel := f.DisplayPath()

		if rel == childRel {
			out.Size = uint64(f.Length())
			out.Mode = syscall.S_IFREG | 0444
			return d.NewInode(ctx, &torrentFile{file: f, opts: d.opts}, gofuse.StableAttr{
				Mode: syscall.S_IFREG,
				Ino:  fileIno(d.t, i),
			}), 0
		}

		if strings.HasPrefix(rel, childRel+"/") {
			out.Mode = syscall.S_IFDIR | 0755
			return d.NewInode(ctx, &torrentDir{t: d.t, prefix: childRel, opts: d.opts}, gofuse.StableAttr{
				Mode: syscall.S_IFDIR,
				Ino:  pathIno(d.t, childRel),
			}), 0
		}
	}
	return nil, syscall.ENOENT
}

var _ = (gofuse.NodeGetattrer)((*torrentDir)(nil))
var _ = (gofuse.NodeReaddirer)((*torrentDir)(nil))
var _ = (gofuse.NodeLookuper)((*torrentDir)(nil))

// ── virtual file ─────────────────────────────────────────────────────────────

type torrentFile struct {
	gofuse.Inode
	file *libt.File
	opts *fsOpts
}

func (f *torrentFile) Getattr(_ context.Context, _ gofuse.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Size = uint64(f.file.Length())
	out.Mode = syscall.S_IFREG | 0444
	return 0
}

func (f *torrentFile) Open(_ context.Context, _ uint32) (gofuse.FileHandle, uint32, syscall.Errno) {
	r := f.file.NewReader()
	r.SetReadahead(f.opts.readaheadBytes)
	r.SetResponsive()
	return &fileHandle{reader: r}, fuse.FOPEN_KEEP_CACHE, 0
}

var _ = (gofuse.NodeGetattrer)((*torrentFile)(nil))
var _ = (gofuse.NodeOpener)((*torrentFile)(nil))

// ── file handle ──────────────────────────────────────────────────────────────

type fileHandle struct {
	mu     sync.Mutex
	reader libt.Reader
}

func (h *fileHandle) Read(_ context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.mu.Lock()
	defer h.mu.Unlock()

	if _, err := h.reader.Seek(off, io.SeekStart); err != nil {
		return nil, syscall.EIO
	}
	n, err := io.ReadFull(h.reader, dest)
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return fuse.ReadResultData(dest[:n]), 0
	}
	if err != nil {
		return nil, syscall.EIO
	}
	return fuse.ReadResultData(dest[:n]), 0
}

func (h *fileHandle) Release(_ context.Context) syscall.Errno {
	h.reader.Close()
	return 0
}

var _ = (gofuse.FileReader)((*fileHandle)(nil))
var _ = (gofuse.FileReleaser)((*fileHandle)(nil))

// ── inode helpers ─────────────────────────────────────────────────────────────

func torrentIno(t *libt.Torrent) uint64 {
	h := t.InfoHash()
	var ino uint64
	for i := 0; i < 8; i++ {
		ino = (ino << 8) | uint64(h[i])
	}
	return ino | (1 << 63)
}

func fileIno(t *libt.Torrent, idx int) uint64 {
	base := torrentIno(t) &^ 0xFFFF
	return base | uint64(idx+1)
}

func pathIno(t *libt.Torrent, path string) uint64 {
	base := torrentIno(t) &^ 0xFFFF_FFFF
	h := uint32(2166136261)
	for i := 0; i < len(path); i++ {
		h ^= uint32(path[i])
		h *= 16777619
	}
	return base | uint64(h|1)
}

func joinPath(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return prefix + "/" + name
}
