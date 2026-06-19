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

1. **Setup** — creates a `bench` table with 50,000 rows (PK, random integer, text padding)
2. **Warmup** — runs sequential scans to load data into PostgreSQL's buffer cache
3. **Benchmark** — for each pool size (2–90), fires 2000 concurrent goroutines doing PK lookups against PostgreSQL:
   - `SetMaxOpenConns` caps the number of concurrent database connections
   - Excess goroutines queue up inside Go's `database/sql` pool (simulating real-world pool saturation)
   - Measures wall-clock duration and average per-query latency (including queue wait time)

Each benchmark creates a fresh connection pool so results are independent.

## Requirements

- Go 1.21+
- PostgreSQL (any recent version)
- A database named `pool-size`

## Quick Start

```bash
# Set up the database
createdb pool-size

# Run the demo
go run .
```

Or configure a custom DSN by editing the `dsn` constant in `main.go`:

```go
const dsn = "host=localhost port=5432 user=postgres password=postgres dbname=pool-size sslmode=disable"
```

## Interpreting the Results

On an **8-core** machine, the benchmark produces a curve like this:

```
Pool Size  Duration       Throughput/s   Avg Latency
    2       111ms          18,041         57ms
    4        74ms          26,988         43ms
    8        68ms          29,368         39ms     ← peak throughput
   12        75ms          26,650         45ms
   16        71ms          27,988         45ms
   20        82ms          24,417         55ms
   24        93ms          21,552         64ms
   32        97ms          20,711         68ms
   40       112ms          17,875         66ms
   60       140ms          14,335        100ms
   80       171ms          11,679        126ms
   90       202ms           9,893        127ms
```

| Zone | Pool Size | Behavior |
|------|-----------|----------|
| Too few | 2 | DB is underutilized; throughput is low |
| Sweet spot | 4–16 | Peak throughput, lowest latency |
| Too many | 32+ | Throughput degrades, latency climbs |

The recommended formula value `(8 × 2 + 1) = 17` lands at the upper end of the sweet spot.

### Why does performance degrade with more connections?

PostgreSQL spawns one backend process per connection. When the number of active connections exceeds the number of CPU cores, the operating system must context-switch between these processes. This overhead reduces the time available for actual database work, causing throughput to drop and latency to rise.

## Tuning the Demo

You can adjust:

- **Query count** — change the `n` argument in `benchmark(size, n)` calls
- **Table size** — change the `generate_series(1, 50000)` range in `setup()`
- **Query type** — the slow query variant is in `benchmarkSlow()` (commented out)

## Pool-Locking Formula

When a single thread may hold multiple connections simultaneously (e.g., nested transactions), use this formula to avoid deadlock:

```
pool_size = Tn × (Cm - 1) + 1
```

Where `Tn` = max threads, `Cm` = max connections per thread.

## Caveats

- Pool sizing is deployment-specific. The formula is a starting point, not a rule.
- Mix of long and short transactions may benefit from separate pool instances.
- The demo uses fast PK lookups. Slower queries shift the bottleneck to the DB CPU, where the sweet spot tends to be even smaller.
- Your mileage will vary based on hardware, dataset size, query complexity, and PostgreSQL configuration.

## References

- [HikariCP Wiki: About Pool Sizing](https://github.com/brettwooldridge/HikariCP/wiki/About-Pool-Sizing)
- [HikariCP GitHub](https://github.com/brettwooldridge/HikariCP)
- [PostgreSQL Connection Pooling](https://wiki.postgresql.org/wiki/Connection_Pooling)
