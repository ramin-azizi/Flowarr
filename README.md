## Core Components

### 1. Flowarr Daemon (Go)
- HTTP API exposing qBittorrent-compatible endpoints (so Arrs treat it as a download client)
- Internal BitTorrent client (anacrolix/torrent)
- Sequential piece prioritization for streaming playback
- Torrent state manager (active streams, RD handoff tracking)

### 2. Virtual FUSE Filesystem
- Exposes "in-progress" torrents as readable files at `/mnt/flowarr/<title>.mkv`
- Reads block on missing pieces, triggering targeted piece downloads
- Supports random seeks (Plex scrubbing) by reprioritizing pieces

### 3. Decypharr Integration
- Polls Decypharr API to check if a hash is now cached on RD
- When cached, signals handoff: Flowarr removes its symlink, Decypharr's symlink replaces it
- Plex's library scan picks up the file transparently

### 4. *Arr Integration
- Presents as qBittorrent (port 8888) — Arrs add torrents the standard way
- Categories route to library folders: `/mnt/library/movies-hd`, etc.
- Symlinks point into the Flowarr virtual mount initially

### 5. Web UI
- Active streams dashboard (peers, speed, progress)
- Manual control: pause, prioritize, drop
- RD handoff status per torrent

## Tech Stack

- **Language**: Go — single binary, good concurrency
- **Torrent engine**: anacrolix/torrent (Go, supports streaming)
- **FUSE**: hanwen/go-fuse
- **Web UI**: SvelteKit or React
- **Database**: SQLite
- **Config**: YAML

## Roadmap

### Phase 1 — MVP
- Go project skeleton
- qBittorrent-compatible HTTP API
- anacrolix/torrent integration with sequential download
- Basic FUSE mount
- Symlink creation matching qBittorrent download_folder semantics

### Phase 2 — Decypharr Integration
- Decypharr API client
- State machine for handoff (streaming → cached → handoff complete)
- Atomic symlink swap

### Phase 3 — Polish
- Web UI dashboard
- Stats and logging
- Docker container with FUSE permissions
- Documentation

### Phase 4 — Stretch
- Sonarr-specific season/episode handling
- Bandwidth limiting, scheduling
- VPN integration (Gluetun-compatible)
- Multiple debrid provider support
- Webhook from Decypharr instead of polling

## Risks & Open Questions

1. **Legal**: P2P streaming exposes the host IP. Recommend VPN.
2. **Performance**: Plex seeks may overwhelm sequential download. Need adaptive piece priority.
3. **Memory**: Buffering large 4K files needs careful management.
4. **Race conditions**: Decypharr and Flowarr both managing the same library folder. Atomic symlink swaps required.
5. **Plex behavior**: Need to test how Plex reacts to file source switching mid-play.

## Contributing

This is a community-driven proposal. Architecture is opinionated but flexible.

## License

GPL-3.0
