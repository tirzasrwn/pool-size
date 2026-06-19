# HikariCP Pool Sizing — PostgreSQL Demo

A Go application that demonstrates [HikariCP's "About Pool Sizing"](https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing) principles using a real PostgreSQL database.

## The Core Idea

Database connection pools are often **over-provisioned**. More connections does not mean more throughput — after a certain point, additional connections **degrade** performance due to context switching overhead on the database server.

### The Formula

```
connections = (core_count × 2) + effective_spindle_count
```

- `core_count` — physical CPU cores (not hyperthreads)
- `effective_spindle_count` — 0 if the dataset is fully cached, otherwise the number of disk spindles

> "A formula which has held up pretty well across a lot of benchmarks for years is that for optimal throughput the number of active connections should be somewhere near ((core_count * 2) + effective_spindle_count)."

## How the Demo Works

1. **Setup** — creates a `bench` table with 50,000 rows (PK, random integer, 100-char text padding)
2. **Warmup** — runs sequential scans in parallel to load data into PostgreSQL's buffer cache
3. **Benchmark** — for each pool size (2–90), fires concurrent goroutines performing database operations:
   - `SetMaxOpenConns` caps the number of concurrent database connections
   - Excess goroutines queue up inside Go's `database/sql` pool (simulating real-world pool saturation)
   - Measures wall-clock duration and average per-query latency (including queue wait time)

Each pool size gets a fresh `sql.DB` so results are independent. Goroutines are managed with `errgroup.Group` which handles `Add`/`Done`/error propagation automatically.

### Four CRUD Benchmarks

| Benchmark | Query | What it tests |
|-----------|-------|---------------|
| SELECT PK lookup | `SELECT val, padding FROM bench WHERE id = $1` | CPU-bound index scan |
| INSERT single row | `INSERT INTO bench (val, padding) VALUES ($1, $2)` | Write path + WAL flush |
| UPDATE PK lookup | `UPDATE bench SET padding = $1 WHERE id = $2` | Write path + row locking |
| DELETE PK lookup | `DELETE FROM bench WHERE id = $1` | Write path + MVCC cleanup |

Operations are ordered from least to most destructive: SELECT → INSERT → UPDATE → DELETE.

## Requirements

- Go 1.21+
- PostgreSQL (any recent version)
- A database named `pool-size`

## Quick Start

```bash
# Set up the database
createdb pool-size

# Run the demo (creates table, warms cache, runs all benchmarks)
go run .
```

Or configure a custom DSN by editing the `dsn` constant in `main.go`:

```go
const dsn = "host=localhost port=5432 user=postgres password=postgres dbname=pool-size sslmode=disable"
```

## Interpreting the Results

On an **8-core** machine, the four benchmarks show distinct curves:

```
SELECT PK lookup — 2000 queries       INSERT single row — 2000 queries

Pool Size  Throughput/s   Avg Latency  Pool Size  Throughput/s   Avg Latency
    2        17,993          57ms           2         1,142         892ms
    4        28,164          40ms           4         2,348         430ms
    8        26,693          46ms           8         4,435         231ms
   12        29,612          41ms  <- peak  12         6,520         161ms
   16        29,430          43ms          16         8,188         133ms
   20        25,287          54ms          20         9,095         122ms
   24        24,953          56ms          24         8,861         136ms
   32        22,716          61ms          32         9,082         145ms
   40        20,024          75ms          40        12,324         107ms
   50        13,802          96ms          50        13,708         102ms  <- peak
   60        14,268          88ms          60        12,185         113ms
   70        12,767         110ms          70        11,420         127ms
   80        11,151         127ms          80         9,261         170ms
   90         9,193         160ms          90        10,507         134ms

UPDATE PK lookup — 2000 queries         DELETE PK lookup — 2000 queries

Pool Size  Throughput/s   Avg Latency   Pool Size  Throughput/s   Avg Latency
    2         1,160         865ms            2         1,164         879ms
    4         2,268         435ms            4         2,483         411ms
    8         4,498         228ms            8         5,156         200ms
   12         6,402         165ms           12         7,749         136ms
   16         7,781         141ms           16         9,288         116ms
   20         8,758         126ms           20        11,874          95ms
   24         8,368         138ms           24        12,638          94ms
   28        10,364         120ms           28        14,833          86ms
   32        11,503         100ms           32        15,062          90ms  <- peak
   40        11,666         115ms  <- peak   40        14,625          94ms
   50        10,820         116ms           50        12,591         115ms
   60         7,983         177ms           60        13,055         107ms
   70         7,638         194ms           70         9,486         159ms
   80         8,329         183ms           80         9,209         174ms
   90         7,696         198ms           90        10,459         140ms
```

| Operation | Peak Pool | Throughput | Why |
|-----------|-----------|------------|-----|
| SELECT    | 12–16     | ~29,600/s  | CPU-bound, fast index scan → fewer connections optimal |
| INSERT    | ~50       | ~13,700/s  | WAL flush, disk I/O → more connections hide latency |
| UPDATE    | ~40       | ~11,700/s  | WAL + row locks → similar to INSERT |
| DELETE    | ~32       | ~15,100/s  | MVCC mark-dead (no data moved) → faster than INSERT |

### Why do writes need more connections?

Reads (SELECT) are CPU-bound — PostgreSQL scans the index in memory and returns the row. With fast queries, context switching from too many connections dominates.

Writes (INSERT/UPDATE/DELETE) hit the **WAL (Write-Ahead Log)** and eventually the disk. During WAL flush, the backend process is **blocked on I/O**, allowing other connections to use the CPU. This I/O wait means more connections can be productive, pushing the optimal pool size higher.

The formula `(cores × 2) + spindles` accounts for this: the `spindles` term represents I/O wait. An all-flash database with a fully cached dataset behaves more like the SELECT case (fewer connections). A spinning-disk database behaves more like the INSERT case (more connections).

### Why does performance degrade with too many connections?

PostgreSQL spawns one backend process per connection. When active connections exceed CPU cores, the OS context-switches between them. This overhead reduces time for actual database work, dropping throughput and raising latency.

## Pool-Locking Formula

When a single thread may hold multiple connections simultaneously (e.g., nested transactions), use this formula to avoid deadlock:

```
pool_size = Tn × (Cm - 1) + 1
```

Where `Tn` = max threads, `Cm` = max connections per thread.

## Code Structure

| File | Purpose |
|------|---------|
| `main.go` | Setup, warmup, benchmark runner, all 4 CRUD benchmark functions |
| `go.mod` | Dependencies: `pgx/v5` (PostgreSQL driver), `x/sync/errgroup` (goroutine management) |

Key functions:
- `runBench()` — iterates pool sizes, runs a benchmark function, prints the table with peak marker
- `benchmarkSelect/Insert/Update/Delete()` — each creates goroutines via `errgroup.Group.Go()` and measures throughput + latency
- `randSeq()` — generates random strings for INSERT/UPDATE payloads

## Caveats

- Pool sizing is deployment-specific. The formula is a starting point, not a rule.
- The demo uses fast PK operations. Sequential scans or complex joins shift the bottleneck differently.
- Writes show higher variance because WAL flushes and checkpointing add non-deterministic latency.
- Your mileage will vary based on CPU cores, disk type (SSD vs HDD), dataset size, and PostgreSQL configuration (especially `max_connections`, `shared_buffers`, and `wal_buffers`).

## References

- [HikariCP Wiki: About Pool Sizing](https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing)
- [HikariCP GitHub](https://github.com/brettwooldridge/HikariCP)
- [PostgreSQL Connection Pooling](https://wiki.postgresql.org/wiki/Connection_Pooling)
