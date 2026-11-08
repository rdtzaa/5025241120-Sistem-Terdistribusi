// bully menjalankan satu proses algoritme election by bullying.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"time"

	"go.nanomsg.org/mangos/v3"
	"go.nanomsg.org/mangos/v3/protocol/rep"
	"go.nanomsg.org/mangos/v3/protocol/req"
	_ "go.nanomsg.org/mangos/v3/transport/tcp"
)

type config struct {
	id, count, basePort int
	timeout, startup    time.Duration
}

type message struct {
	Type string `json:"type"`
	From int    `json:"from"`
}

type node struct {
	cfg         config
	sock        mangos.Socket
	log         *log.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	mu          sync.Mutex
	electing    bool
	coordinator int
	announced   chan struct{}
}

func address(cfg config, id int) string {
	return fmt.Sprintf("tcp://127.0.0.1:%d", cfg.basePort+id)
}

func request(cfg config, target int, msg message) (message, error) {
	sock, err := req.NewSocket()
	if err != nil {
		return message{}, err
	}
	defer sock.Close()
	for option, value := range map[string]interface{}{
		mangos.OptionSendDeadline: cfg.timeout,
		mangos.OptionRecvDeadline: cfg.timeout,
		mangos.OptionRetryTime:    time.Duration(0),
	} {
		if err := sock.SetOption(option, value); err != nil {
			return message{}, err
		}
	}
	if err := sock.DialOptions(address(cfg, target), map[string]interface{}{mangos.OptionDialAsynch: true}); err != nil {
		return message{}, err
	}
	body, err := json.Marshal(msg)
	if err != nil {
		return message{}, err
	}
	if err := sock.Send(body); err != nil {
		return message{}, err
	}
	body, err = sock.Recv()
	if err != nil {
		return message{}, err
	}
	var reply message
	if err := json.Unmarshal(body, &reply); err != nil {
		return message{}, err
	}
	if reply.From != target {
		return message{}, fmt.Errorf("balasan bukan dari node %d", target)
	}
	return reply, nil
}

func newNode(cfg config, output io.Writer) (*node, error) {
	sock, err := rep.NewSocket()
	if err != nil {
		return nil, err
	}
	if err := sock.SetOption(mangos.OptionSendDeadline, cfg.timeout); err != nil {
		sock.Close()
		return nil, err
	}
	if err := sock.Listen(address(cfg, cfg.id)); err != nil {
		sock.Close()
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &node{cfg: cfg, sock: sock, ctx: ctx, cancel: cancel,
		log:         log.New(output, fmt.Sprintf("[node %d] ", cfg.id), log.Lmicroseconds),
		coordinator: -1, announced: make(chan struct{}, 1)}, nil
}

func (n *node) run(ctx context.Context, initiate bool) error {
	defer func() { n.cancel(); n.sock.Close(); n.wg.Wait() }()
	go func() {
		select {
		case <-ctx.Done():
			n.cancel()
			n.sock.Close()
		case <-n.ctx.Done():
		}
	}()
	n.log.Printf("LIVE, mendengarkan %s", address(n.cfg, n.cfg.id))
	if initiate {
		n.wg.Add(1)
		go func() {
			defer n.wg.Done()
			timer := time.NewTimer(n.cfg.startup)
			defer timer.Stop()
			select {
			case <-n.ctx.Done():
				return
			case <-timer.C:
			}
			leader := n.cfg.count - 1
			n.log.Printf("PING -> %d (coordinator awal)", leader)
			reply, err := request(n.cfg, leader, message{Type: "PING", From: n.cfg.id})
			if err == nil && reply.Type == "PONG" {
				n.log.Printf("coordinator %d masih hidup; election tidak diperlukan", leader)
				return
			}
			n.log.Printf("coordinator %d tidak merespons; mulai election", leader)
			n.startElection()
		}()
	}
	for {
		body, err := n.sock.Recv()
		if err != nil {
			if n.ctx.Err() != nil {
				return nil
			}
			return err
		}
		var msg message
		reply := message{Type: "ERROR", From: n.cfg.id}
		start := false
		if json.Unmarshal(body, &msg) == nil && msg.From >= 0 && msg.From < n.cfg.count {
			switch {
			case msg.Type == "PING":
				reply.Type = "PONG"
			case msg.Type == "ELECTION" && msg.From < n.cfg.id:
				n.log.Printf("ELECTION <- %d; OK -> %d", msg.From, msg.From)
				reply.Type, start = "OK", true
			case msg.Type == "COORDINATOR" && msg.From > n.cfg.id:
				n.mu.Lock()
				n.coordinator = msg.From
				n.mu.Unlock()
				n.log.Printf("COORDINATOR <- %d; coordinator sekarang: %d", msg.From, msg.From)
				select {
				case n.announced <- struct{}{}:
				default:
				}
				reply.Type = "ACK"
			}
		}
		body, _ = json.Marshal(reply)
		if err := n.sock.Send(body); err != nil {
			if n.ctx.Err() != nil {
				return nil
			}
			n.log.Printf("gagal mengirim balasan: %v", err)
		}
		// Election berjalan terpisah supaya REP tetap dapat menjawab node lain.
		if start {
			n.startElection()
		}
	}
}

func (n *node) startElection() {
	n.mu.Lock()
	if n.electing || n.ctx.Err() != nil {
		n.mu.Unlock()
		return
	}
	n.electing, n.coordinator = true, -1
	select {
	case <-n.announced:
	default:
	}
	n.mu.Unlock()
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		defer func() { n.mu.Lock(); n.electing = false; n.mu.Unlock() }()
		n.elect()
	}()
}

func (n *node) elect() {
	for n.ctx.Err() == nil {
		higherAlive := false
		for target := n.cfg.id + 1; target < n.cfg.count; target++ {
			if n.ctx.Err() != nil {
				return
			}
			n.log.Printf("ELECTION -> %d", target)
			reply, err := request(n.cfg, target, message{Type: "ELECTION", From: n.cfg.id})
			if err == nil && reply.Type == "OK" {
				higherAlive = true
				n.log.Printf("OK <- %d", target)
			} else {
				n.log.Printf("node %d tidak menjawab ELECTION", target)
			}
		}
		n.mu.Lock()
		known := n.coordinator
		if !higherAlive && known < n.cfg.id {
			n.coordinator = n.cfg.id
		}
		n.mu.Unlock()
		if known > n.cfg.id {
			return
		}
		if !higherAlive {
			n.log.Printf("tidak ada OK dari ID lebih tinggi; MENJADI COORDINATOR")
			for target := 0; target < n.cfg.id; target++ {
				if n.ctx.Err() != nil {
					return
				}
				n.log.Printf("COORDINATOR -> %d", target)
				reply, err := request(n.cfg, target, message{Type: "COORDINATOR", From: n.cfg.id})
				if err != nil || reply.Type != "ACK" {
					n.log.Printf("node %d tidak mengakui COORDINATOR", target)
				}
			}
			return
		}
		n.log.Printf("diam menunggu COORDINATOR (sudah menerima OK)")
		// Bila higher-up mati setelah OK
		timer := time.NewTimer(time.Duration(n.cfg.count) * 4 * n.cfg.timeout)
		select {
		case <-n.ctx.Done():
			timer.Stop()
			return
		case <-n.announced:
			timer.Stop()
			return
		case <-timer.C:
			n.log.Printf("COORDINATOR belum diterima; ulangi election")
		}
	}
}

func main() {
	cfg := config{}
	flag.IntVar(&cfg.count, "nodes", 8, "jumlah node, dengan ID 0 sampai N-1")
	flag.IntVar(&cfg.basePort, "base-port", 5000, "port node 0 (node k memakai port ini+k)")
	flag.DurationVar(&cfg.timeout, "timeout", 800*time.Millisecond, "batas waktu kirim/terima pesan")
	flag.DurationVar(&cfg.startup, "startup", 2*time.Second, "waktu tunggu sebelum pengecekan coordinator")
	flag.Usage = func() { fmt.Fprintln(os.Stderr, "Usage: bully [flags] ID [live|live_init|dead]"); flag.PrintDefaults() }
	flag.Parse()
	if flag.NArg() < 1 || flag.NArg() > 2 {
		flag.Usage()
		os.Exit(2)
	}
	id, err := strconv.Atoi(flag.Arg(0))
	if err != nil || cfg.count < 1 || id < 0 || id >= cfg.count || cfg.basePort < 1 || cfg.basePort > 65536-cfg.count || cfg.timeout <= 0 || cfg.startup < 0 {
		fmt.Fprintln(os.Stderr, "ID, jumlah node, port, atau durasi tidak valid")
		os.Exit(2)
	}
	cfg.id = id
	mode := "live"
	if id == 4 {
		mode = "live_init"
	}
	if id == cfg.count-1 {
		mode = "dead"
	}
	if flag.NArg() == 2 {
		mode = flag.Arg(1)
	}
	if mode != "live" && mode != "live_init" && mode != "dead" {
		fmt.Fprintln(os.Stderr, "mode harus live, live_init, atau dead")
		os.Exit(2)
	}
	if mode == "dead" {
		fmt.Printf("[node %d] DEAD: tidak membuka socket dan tidak menjawab pesan\n", id)
		return
	}
	n, err := newNode(cfg, os.Stdout)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if err := n.run(ctx, mode == "live_init"); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
