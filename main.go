package main

import (
	"database/sql"
	"fmt"
	"math/rand"
	"runtime"
	"sort"
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

	poolSizes := generatePoolSizes(recommended)

	runBench("SELECT PK lookup", poolSizes, 2000, recommended, benchmarkSelect)
	fmt.Println()
	runBench("INSERT single row", poolSizes, 2000, recommended, benchmarkInsert)
	fmt.Println()
	runBench("UPDATE PK lookup", poolSizes, 2000, recommended, benchmarkUpdate)
	fmt.Println()
	runBench("DELETE PK lookup", poolSizes, 2000, recommended, benchmarkDelete)
	fmt.Println()
	runBench("JOIN + aggregation", poolSizes, 2000, recommended, benchmarkJoin)

	fmt.Println()
	fmt.Printf("Recommended pool size for %d cores: ((%d \u00d7 2) + 1) = %d\n", cores, cores, recommended)
}

func setup() {
	db := mustOpen()
	defer db.Close()
	db.SetMaxOpenConns(2)

	db.Exec(`DROP TABLE IF EXISTS categories`)
	db.Exec(`DROP TABLE IF EXISTS bench`)

	db.Exec(`
		CREATE TABLE bench (
			id SERIAL PRIMARY KEY,
			val INT NOT NULL,
			cat_id INT NOT NULL,
			padding TEXT NOT NULL
		)
	`)
	db.Exec(`CREATE INDEX ON bench(cat_id)`)

	_, err := db.Exec(`
		INSERT INTO bench (val, cat_id, padding)
		SELECT (random() * 1000)::int, (random() * 9 + 1)::int, lpad('', 100, 'x')
		FROM generate_series(1, 50000)
	`)
	if err != nil {
		panic(err)
	}

	db.Exec(`
		CREATE TABLE categories (
			id SERIAL PRIMARY KEY,
			name TEXT NOT NULL
		)
	`)
	_, err = db.Exec(`
		INSERT INTO categories (name)
		SELECT 'cat_' || g FROM generate_series(1, 10) g
	`)
	if err != nil {
		panic(err)
	}
	fmt.Println("Setup: 50,000 rows + 10 categories")
}

func cleanup() {
	db := mustOpen()
	defer db.Close()
	db.Exec(`DROP TABLE IF EXISTS categories`)
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

func generatePoolSizes(rec int) []int {
	seen := map[int]bool{}
	var sizes []int

	add := func(v int) {
		if v >= 2 && v <= 90 && !seen[v] {
			sizes = append(sizes, v)
			seen[v] = true
		}
	}

	add(2)
	add(4)
	add(8)
	add(12)
	add(16)
	add(rec)

	for s := 20; s <= 90; s += 10 {
		add(s)
	}

	sort.Ints(sizes)
	return sizes
}

func runBench(label string, poolSizes []int, n, recommended int, fn func(*sql.DB, int, int) (time.Duration, time.Duration)) {
	fmt.Printf("%s - %d queries\n", label, n)

	type result struct {
		size int
		dur  time.Duration
		tps  float64
		avg  time.Duration
	}
	var results []result
	peakTps := 0.0

	for _, size := range poolSizes {
		db := mustOpen()
		dur, avg := fn(db, size, n)
		db.SetMaxIdleConns(0)
		db.Close()
		time.Sleep(time.Second)
		tps := float64(n) / dur.Seconds()
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
	fmt.Println(strings.Repeat("-", 56))
}

func benchmarkSelect(db *sql.DB, poolSize, n int) (time.Duration, time.Duration) {
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

	if err := g.Wait(); err != nil {
		panic(err)
	}
	return time.Since(start), time.Duration(latSum.Load() / int64(n))
}

func benchmarkUpdate(db *sql.DB, poolSize, n int) (time.Duration, time.Duration) {
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)

	var g errgroup.Group
	start := time.Now()
	var latSum atomic.Int64

	for range n {
		g.Go(func() error {
			id := rand.Intn(50000) + 1

			t0 := time.Now()
			res, err := db.Exec(`UPDATE bench SET padding = $1 WHERE id = $2`, randSeq(20), id)
			if err != nil {
				return err
			}
			n, _ := res.RowsAffected()
			if n == 0 {
				return fmt.Errorf("row %d not found", id)
			}
			latSum.Add(int64(time.Since(t0)))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}
	return time.Since(start), time.Duration(latSum.Load() / int64(n))
}

func benchmarkInsert(db *sql.DB, poolSize, n int) (time.Duration, time.Duration) {
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)

	var g errgroup.Group
	start := time.Now()
	var latSum atomic.Int64

	for range n {
		g.Go(func() error {
			t0 := time.Now()
			_, err := db.Exec(`INSERT INTO bench (val, cat_id, padding) VALUES ($1, $2, $3)`, rand.Intn(1000), rand.Intn(10)+1, randSeq(20))
			if err != nil {
				return err
			}
			latSum.Add(int64(time.Since(t0)))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}
	return time.Since(start), time.Duration(latSum.Load() / int64(n))
}

func benchmarkDelete(db *sql.DB, poolSize, n int) (time.Duration, time.Duration) {
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)

	var g errgroup.Group
	start := time.Now()
	var latSum atomic.Int64

	for range n {
		g.Go(func() error {
			id := rand.Intn(50000) + 1

			t0 := time.Now()
			_, err := db.Exec(`DELETE FROM bench WHERE id = $1`, id)
			if err != nil {
				return err
			}
			latSum.Add(int64(time.Since(t0)))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}
	return time.Since(start), time.Duration(latSum.Load() / int64(n))
}

func benchmarkJoin(db *sql.DB, poolSize, n int) (time.Duration, time.Duration) {
	db.SetMaxOpenConns(poolSize)
	db.SetMaxIdleConns(poolSize)

	var g errgroup.Group
	start := time.Now()
	var latSum atomic.Int64

	for range n {
		g.Go(func() error {
			catID := rand.Intn(10) + 1

			t0 := time.Now()
			var cnt int
			var avg sql.NullFloat64
			err := db.QueryRow(`
				SELECT COUNT(*), AVG(b.val)
				FROM bench b
				JOIN categories c ON b.cat_id = c.id
				WHERE c.id = $1
			`, catID).Scan(&cnt, &avg)
			if err != nil {
				return err
			}
			latSum.Add(int64(time.Since(t0)))
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		panic(err)
	}
	return time.Since(start), time.Duration(latSum.Load() / int64(n))
}

func randSeq(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte(rand.Intn(26) + 97)
	}
	return string(b)
}
