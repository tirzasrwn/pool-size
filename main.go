package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/sync/errgroup"
)

const dsn = "host=localhost port=5432 user=postgres password=postgres dbname=pool-size sslmode=disable"

func main() {
	cores := runtime.NumCPU()
	recommended := cores*2 + 1

	fmt.Println("HikariCP Pool Sizing - PostgreSQL Demo")
	fmt.Println()

	setup()
	defer cleanup()
	warmup()

	poolSizes := []int{2, 4, 8, 12, 16, 20, 24, 28, 32, 40, 50, 60, 70, 80, 90}

	fmt.Println("PK Lookup - 2000 queries")

	type result struct {
		size int
		dur  time.Duration
		tps  float64
		avg  time.Duration
	}
	var results []result
	peakTps := 0.0

	for _, size := range poolSizes {
		fmt.Printf("Benchmarking pool size %d ...\n", size)
		dur, avg := benchmark(size, 2000)
		tps := float64(2000) / dur.Seconds()
		results = append(results, result{size, dur, tps, avg})
		if tps > peakTps {
			peakTps = tps
		}
	}

	fmt.Printf("\n%-10s %-14s %-14s %-14s\n", "Pool Size", "Duration", "Throughput/s", "Avg Latency")
	for _, r := range results {
		marker := ""
		if r.size == recommended {
			marker = "  <- recommended"
		} else if r.tps == peakTps {
			marker = "  <- peak throughput"
		}
		fmt.Printf("%-10d %-14s %-14.0f %-14s%s\n",
			r.size, r.dur.Round(time.Millisecond), r.tps,
			r.avg.Round(time.Millisecond), marker)
	}

	fmt.Println(strings.Repeat("─", 56))

	fmt.Println()
	fmt.Printf("Recommended pool size for %d cores: ((%d × 2) + 1) = %d\n", cores, cores, recommended)
}

func setup() {
	db := mustOpen()
	defer db.Close()
	db.SetMaxOpenConns(2)

	db.Exec(`DROP TABLE IF EXISTS bench`)
	db.Exec(`
		CREATE TABLE bench (
			id SERIAL PRIMARY KEY,
			val INT NOT NULL,
			padding TEXT NOT NULL
		)
	`)

	_, err := db.Exec(`
		INSERT INTO bench (val, padding)
		SELECT (random() * 1000)::int, lpad('', 100, 'x')
		FROM generate_series(1, 50000)
	`)
	if err != nil {
		panic(err)
	}
	fmt.Println("Setup: 50,000 rows inserted")
}

func cleanup() {
	db := mustOpen()
	defer db.Close()
	db.Exec(`DROP TABLE IF EXISTS bench`)
}

func warmup() {
	db := mustOpen()
	defer db.Close()
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)

	var g errgroup.Group
	for range 4 {
		g.Go(func() error {
			for range 10 {
				var cnt int
				db.QueryRow(`SELECT COUNT(*) FROM bench WHERE val BETWEEN $1 AND $2`,
					rand.Intn(500), rand.Intn(500)+500).Scan(&cnt)
			}
			return nil
		})
	}
	g.Wait()
	fmt.Println("Warmup: done")
}

func mustOpen() *sql.DB {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		panic(err)
	}
	return db
}

func benchmark(poolSize, n int) (time.Duration, time.Duration) {
	db := mustOpen()
	defer db.Close()
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)

	var g errgroup.Group
	start := time.Now()
	var latSum atomic.Int64

	for range n {
		g.Go(func() error {
			id := rand.Intn(50000) + 1

			t0 := time.Now()
			var val int
			var padding string
			err := db.QueryRow(`SELECT val, padding FROM bench WHERE id = $1`, id).Scan(&val, &padding)
			if err != nil {
				return err
			}
			latSum.Add(int64(time.Since(t0)))
			return nil
		})
	}

	g.Wait()
	return time.Since(start), time.Duration(latSum.Load() / int64(n))
}
