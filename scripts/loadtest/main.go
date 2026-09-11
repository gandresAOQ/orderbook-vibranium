// Command loadtest is a concurrent "bot" client that hammers the order book API
// and then verifies the fundamental invariant: total COP and total Vibranium
// across all wallets are unchanged. This mirrors how the evaluators validate
// balances at the end of their tests.
//
// Usage:
//
//	go run ./scripts/loadtest -url http://localhost:3000 -pairs 5000 -concurrency 200
//
// Each "pair" seeds one buyer and one seller and submits a matching order from
// each, so a fully-matched run produces exactly `pairs` trades.
//
// Settlement is asynchronous (it consumes the event log), and with the Kafka
// adapter that log crosses the network. The verifier therefore WAITS for the
// trade count to reach the expected total and settle down, instead of assuming a
// fixed delay.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type wallet struct {
	UserID             string `json:"userId"`
	COPAvailable       int64  `json:"copAvailable"`
	COPLocked          int64  `json:"copLocked"`
	VibraniumAvailable int64  `json:"vibraniumAvailable"`
	VibraniumLocked    int64  `json:"vibraniumLocked"`
}

type health struct {
	Status string `json:"status"`
	Trades int    `json:"trades"`
}

func main() {
	url := flag.String("url", "http://localhost:3000", "base URL of the order book API")
	pairs := flag.Int("pairs", 5000, "number of buyer/seller pairs")
	concurrency := flag.Int("concurrency", 200, "number of concurrent workers")
	price := flag.Int64("price", 100, "limit price for all orders")
	qty := flag.Int64("qty", 1, "quantity per order")
	settleWait := flag.Duration("settle-wait", 60*time.Second, "max time to wait for settlement to drain")
	flag.Parse()

	// Reuse connections aggressively; otherwise the client, not the server,
	// becomes the bottleneck.
	transport := &http.Transport{
		MaxIdleConns:        *concurrency * 2,
		MaxIdleConnsPerHost: *concurrency * 2,
		MaxConnsPerHost:     *concurrency * 2,
		IdleConnTimeout:     90 * time.Second,
	}
	client := &http.Client{Timeout: 30 * time.Second, Transport: transport}

	waitForAPI(client, *url, 60*time.Second)

	// 1) Seed wallets (concurrently: with Postgres this is 2*pairs round trips).
	log.Printf("seeding %d buyer/seller pairs...", *pairs)
	seedStart := time.Now()
	seedAll(client, *url, *pairs, *price, *qty, *concurrency)
	log.Printf("seeded in %s", time.Since(seedStart).Round(time.Millisecond))

	// Baseline AFTER seeding. The invariant is that trading conserves value, so
	// we compare totals before and after — which stays correct even if the
	// store already held wallets from an earlier run.
	baseCOP, baseVib, _, err := totals(client, *url)
	if err != nil {
		log.Fatalf("baseline snapshot: %v", err)
	}
	baseTrades := 0
	if h, err := fetchHealth(client, *url); err == nil {
		baseTrades = h.Trades
	}
	log.Printf("baseline: totalCOP=%d totalVibranium=%d trades=%d", baseCOP, baseVib, baseTrades)

	// 2) Fire orders concurrently.
	var placed, failed int64
	jobs := make(chan int, *pairs)
	var wg sync.WaitGroup
	start := time.Now()

	for w := 0; w < *concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				if postOrder(client, *url, fmt.Sprintf("seller-%d", i), "SELL", *price, *qty) {
					atomic.AddInt64(&placed, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
				if postOrder(client, *url, fmt.Sprintf("buyer-%d", i), "BUY", *price, *qty) {
					atomic.AddInt64(&placed, 1)
				} else {
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}
	for i := 0; i < *pairs; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	elapsed := time.Since(start)
	rate := float64(placed) / elapsed.Seconds()
	log.Printf("placed=%d failed=%d in %s (%.0f orders/sec, ~%.0f trades/sec)",
		placed, failed, elapsed.Round(time.Millisecond), rate, rate/2)

	// 3) Wait for settlement to drain, then verify the invariant.
	settled := waitForSettlement(client, *url, baseTrades+*pairs, *settleWait)
	verify(client, *url, baseCOP, baseVib, settled-baseTrades, *pairs)
}

// waitForAPI blocks until /health answers, so the script can be run right after
// `docker compose up` without a manual sleep.
func waitForAPI(c *http.Client, url string, wait time.Duration) {
	deadline := time.Now().Add(wait)
	for {
		if _, err := fetchHealth(c, url); err == nil {
			return
		}
		if time.Now().After(deadline) {
			log.Fatalf("API at %s not reachable after %s", url, wait)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func seedAll(c *http.Client, url string, pairs int, price, qty int64, concurrency int) {
	jobs := make(chan int, pairs)
	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				seed(c, url, fmt.Sprintf("buyer-%d", i), price*qty, 0)
				seed(c, url, fmt.Sprintf("seller-%d", i), 0, qty)
			}
		}()
	}
	for i := 0; i < pairs; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

func seed(c *http.Client, url, user string, cop, vib int64) {
	body, _ := json.Marshal(map[string]any{
		"userId": user, "copAvailable": cop, "vibraniumAvailable": vib,
	})
	resp, err := c.Post(url+"/wallets", "application/json", bytes.NewReader(body))
	if err != nil {
		log.Fatalf("seed %s: %v", user, err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusCreated {
		log.Fatalf("seed %s: unexpected status %d", user, resp.StatusCode)
	}
}

func postOrder(c *http.Client, url, user, side string, price, qty int64) bool {
	body, _ := json.Marshal(map[string]any{
		"userId": user, "side": side, "price": price, "quantity": qty,
	})
	resp, err := c.Post(url+"/orders", "application/json", bytes.NewReader(body))
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusCreated
}

func fetchHealth(c *http.Client, url string) (health, error) {
	resp, err := c.Get(url + "/health")
	if err != nil {
		return health{}, err
	}
	defer resp.Body.Close()
	var h health
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return health{}, err
	}
	if resp.StatusCode != http.StatusOK {
		return h, fmt.Errorf("health status %d", resp.StatusCode)
	}
	return h, nil
}

// waitForSettlement polls the trade count until it reaches `expected`, or until
// it stops advancing for long enough that we can call it done.
func waitForSettlement(c *http.Client, url string, expected int, wait time.Duration) int {
	deadline := time.Now().Add(wait)
	last := -1
	stableFor := 0

	for time.Now().Before(deadline) {
		h, err := fetchHealth(c, url)
		if err == nil {
			if h.Trades >= expected {
				log.Printf("settlement drained: %d/%d trades", h.Trades, expected)
				// Trailing release events (refunds) are applied just after the
				// trade; give them a beat before reading balances.
				time.Sleep(300 * time.Millisecond)
				return h.Trades
			}
			if h.Trades == last {
				stableFor++
			} else {
				stableFor = 0
				last = h.Trades
			}
			// ~3s with no progress: settlement is done (or stuck) — report it.
			if stableFor >= 12 {
				log.Printf("settlement stalled at %d/%d trades", h.Trades, expected)
				return h.Trades
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	log.Printf("settlement still incomplete after %s (last seen %d/%d)", wait, last, expected)
	return last
}

// totals sums available+locked across every wallet, plus what is still locked.
func totals(c *http.Client, url string) (cop, vib, locked int64, err error) {
	resp, err := c.Get(url + "/wallets")
	if err != nil {
		return 0, 0, 0, err
	}
	defer resp.Body.Close()

	var wallets []wallet
	if err := json.NewDecoder(resp.Body).Decode(&wallets); err != nil {
		return 0, 0, 0, err
	}
	for _, w := range wallets {
		cop += w.COPAvailable + w.COPLocked
		vib += w.VibraniumAvailable + w.VibraniumLocked
		locked += w.COPLocked + w.VibraniumLocked
	}
	return cop, vib, locked, nil
}

func verify(c *http.Client, url string, expectedCOP, expectedVib int64, settled, expectedTrades int) {
	totalCOP, totalVib, stillLocked, err := totals(c, url)
	if err != nil {
		log.Fatalf("list wallets: %v", err)
	}

	log.Printf("TRADES:    settled=%d expected=%d", settled, expectedTrades)
	log.Printf("INVARIANT: totalCOP=%d (expected %d) totalVibranium=%d (expected %d) stillLocked=%d",
		totalCOP, expectedCOP, totalVib, expectedVib, stillLocked)

	conserved := totalCOP == expectedCOP && totalVib == expectedVib
	switch {
	case conserved && settled == expectedTrades && stillLocked == 0:
		log.Println("RESULT: OK — value conserved, every order matched and settled")
	case conserved:
		log.Println("RESULT: OK (value conserved) — but not everything settled/unlocked; see counts above")
	default:
		log.Println("RESULT: FAIL — balances do not reconcile")
	}
}
