package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/anacrolix/torrent"
)

func main() {
	magnet := flag.String("magnet", "", "Magnet URI to download")
	flag.Parse()

	if *magnet == "" {
		fmt.Println("Usage: torrent-test --magnet '<magnet uri>'")
		fmt.Println("Try the Big Buck Bunny test torrent:")
		fmt.Println(`  --magnet "magnet:?xt=urn:btih:dd8255ecdc7ca55fb0bbf81323d87062db1f6d1c&dn=Big+Buck+Bunny"`)
		os.Exit(1)
	}

	cfg := torrent.NewDefaultClientConfig()
	cfg.DataDir = "./downloads"
	cfg.Seed = false

	client, err := torrent.NewClient(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	t, err := client.AddMagnet(*magnet)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Fetching metadata...")
	<-t.GotInfo()
	fmt.Printf("Torrent: %s\n", t.Name())
	fmt.Printf("Size: %.2f MB\n", float64(t.Length())/1024/1024)
	fmt.Printf("Files: %d\n", len(t.Files()))

	// Set all files to download with sequential priority
	for _, f := range t.Files() {
		f.SetPriority(torrent.PiecePriorityNormal)
	}

	t.DownloadAll()

	// Progress loop
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		stats := t.Stats()
		completed := t.BytesCompleted()
		total := t.Length()
		pct := float64(completed) / float64(total) * 100

		fmt.Printf("\r[%.1f%%] %d/%d MB | peers: %d active / %d total",
			pct,
			completed/1024/1024,
			total/1024/1024,
			stats.ActivePeers,
			stats.TotalPeers,
		)

		if completed >= total {
			fmt.Println("\nDone!")
			break
		}
	}

}
