package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

type safeBuffer struct {
	sync.Mutex
	bytes.Buffer
}

func (b *safeBuffer) Write(p []byte) (int, error) {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.Write(p)
}

func (b *safeBuffer) text() string {
	b.Lock()
	defer b.Unlock()
	return b.Buffer.String()
}

// Catch choosing a lower process, failing to hand over after OK, or omitting
// the announcement to any surviving lower process. Use real mangos TCP sockets.
func TestElectionChoosesHighestLiveNode(t *testing.T) {
	for _, highest := range []int{6, 5, 4, 7} {
		t.Run(fmt.Sprintf("highest_%d", highest), func(t *testing.T) {
			base := freePorts(t)
			ctx, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			defer func() { cancel(); wg.Wait() }()
			logs := make([]*safeBuffer, highest+1)
			for id := 0; id <= highest; id++ {
				logs[id] = &safeBuffer{}
				cfg := config{id: id, count: 8, basePort: base, timeout: 150 * time.Millisecond}
				n, err := newNode(cfg, logs[id])
				if err != nil {
					t.Fatal(err)
				}
				wg.Add(1)
				go func() { defer wg.Done(); _ = n.run(ctx, false) }()
			}
			// Trigger the same ELECTION a lower process sends; process 4 must
			// answer and initiate its own election without blocking its server.
			_, err := request(config{count: 8, basePort: base, timeout: time.Second}, 4, message{Type: "ELECTION", From: 3})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.Now().Add(12 * time.Second)
			for time.Now().Before(deadline) {
				complete := strings.Contains(logs[highest].text(), "MENJADI COORDINATOR")
				for id := 0; id < highest; id++ {
					complete = complete && strings.Contains(logs[id].text(), fmt.Sprintf("coordinator sekarang: %d", highest))
				}
				if complete {
					for id := 0; id < highest; id++ {
						if strings.Contains(logs[id].text(), "MENJADI COORDINATOR") {
							t.Fatalf("node %d incorrectly won:\n%s", id, logs[id].text())
						}
					}
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
			for id, log := range logs {
				t.Logf("node %d:\n%s", id, log.text())
			}
			t.Fatal("surviving nodes did not agree on the highest live coordinator")
		})
	}
}

func freePorts(t *testing.T) int {
	t.Helper()
	for base := 32000; base < 60000; base += 8 {
		var listeners []net.Listener
		for id := 0; id < 8; id++ {
			l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", base+id))
			if err != nil {
				break
			}
			listeners = append(listeners, l)
		}
		for _, l := range listeners {
			_ = l.Close()
		}
		if len(listeners) == 8 {
			return base
		}
	}
	t.Fatal("no eight consecutive free TCP ports")
	return 0
}
