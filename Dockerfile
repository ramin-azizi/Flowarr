# syntax=docker/dockerfile:1

# ── build stage ───────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /flowarr ./cmd/flowarr/

# ── runtime stage ─────────────────────────────────────────────────────────────
FROM alpine:3.20

# fuse3 provides the FUSE kernel interface; ca-certificates for HTTPS to trackers
RUN apk add --no-cache fuse3 ca-certificates && \
    # Allow non-root users to mount FUSE filesystems
    echo "user_allow_other" >> /etc/fuse.conf

COPY --from=builder /flowarr /usr/local/bin/flowarr
COPY flowarr.yaml /etc/flowarr/flowarr.yaml
COPY entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

VOLUME ["/data", "/mnt/flowarr", "/mnt/library"]

EXPOSE 8888

# FUSE requires /dev/fuse and CAP_SYS_ADMIN (or --privileged).
# Run with: docker run --device /dev/fuse --cap-add SYS_ADMIN ...
ENTRYPOINT ["entrypoint.sh", "-config", "/etc/flowarr/flowarr.yaml"]
