package main

import (
	"context"
	"flag"
	"log"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// HelloRoot is the root directory of our virtual filesystem
type HelloRoot struct {
	fs.Inode
}

// OnAdd is called when the filesystem is mounted - we create a fake file here
func (r *HelloRoot) OnAdd(ctx context.Context) {
	content := []byte("Hello from Flowarr FUSE!\nThis file is virtual - it doesn't exist on disk.\n")
	
	child := r.NewPersistentInode(
		ctx,
		&fs.MemRegularFile{
			Data: content,
			Attr: fuse.Attr{Mode: 0444},
		},
		fs.StableAttr{Ino: 2},
	)
	r.AddChild("hello.txt", child, false)
}

func (r *HelloRoot) Getattr(ctx context.Context, fh fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	out.Mode = 0755
	return 0
}

var _ = (fs.NodeGetattrer)((*HelloRoot)(nil))
var _ = (fs.NodeOnAdder)((*HelloRoot)(nil))

func main() {
	mountpoint := flag.String("mount", "./flowarr-mount", "Mount point")
	flag.Parse()

	opts := &fs.Options{}
	opts.Debug = false

	log.Printf("Mounting at %s", *mountpoint)
	server, err := fs.Mount(*mountpoint, &HelloRoot{}, opts)
	if err != nil {
		log.Fatalf("Mount failed: %v", err)
	}

	log.Printf("Mounted. Try: cat %s/hello.txt", *mountpoint)
	log.Printf("Unmount with: umount %s  (or Ctrl+C)", *mountpoint)
	server.Wait()
}
