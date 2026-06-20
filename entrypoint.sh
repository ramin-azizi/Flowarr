#!/bin/sh
# Clean up any stale FUSE mount left over from a previous crash before starting.
MOUNT="${FLOWARR_MOUNT_DIR:-/mnt/flowarr}"
fusermount3 -uz "$MOUNT" 2>/dev/null || true
exec flowarr "$@"
