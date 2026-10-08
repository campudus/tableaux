# Row loading benchmark

- `bench_rows.py`: HTTP benchmark for the rows of one table: a request matrix, a full table load
  as the frontend does it and as grud-aggregator does it, and error accounting. Every
  parameter and the output format are documented in the docstring at the top of the file.
- `pgcount.py`: a PostgreSQL proxy that counts the statements the backend sends. **Local use only.**

Both need only Python 3.8+ and nothing else installed. Response bodies are never written
anywhere; the output holds timings, sizes and SHA-256 digests of the bodies.

## Run locally

1. Start the backend. For numbers comparable to the reference below, use
   `isRowPermissionCheckEnabled: false` (as in bikeparts production), `-Xmx512M`, and
   PostgreSQL with `jit=off`.
2. Restart it right before the run if you want a cold first request (see "First vs cold").
3. Run it from a scratch directory, because the results file goes to the current directory:

   ```sh
   python3 scripts/benchmark/bench_rows.py --base-url http://127.0.0.1:8080 --table 122 --tag before
   ```

4. Apply the change, restart the backend, and run again with `--tag after`.
   - Compare `median_s` (matrix) and `median_wall_s` (full loads).
   - Equal `digest` values mean byte-identical responses.

A full run on table 122 (6,856 rows) takes about 3 minutes. Use `--scenarios` and `--cases`
for smaller runs, and `--repeat` to set the number of repetitions (default 3):

```sh
python3 scripts/benchmark/bench_rows.py --base-url http://127.0.0.1:8080 --table 122 \
  --scenarios frontend --repeat 0                      # one frontend full table load only
python3 scripts/benchmark/bench_rows.py --base-url http://127.0.0.1:8080 --table 122 \
  --cases rows_limit500,rows_limit500_lastpage        # two matrix cases
```

## Run against a remote environment

```sh
python3 scripts/benchmark/bench_rows.py --base-url https://grud.example.com/api --table 122 \
  -H "Authorization: Bearer $TOKEN" --scenarios frontend,aggregator --tag prod-before
```

- `--base-url` includes the path prefix of the gateway. `-H` can be repeated. Header values are
  never written to the output.
- The token must stay valid for the whole run. If it expires, the remaining requests fail with
  401, and the error summary and the exit status show it.
- The benchmark puts real load on the environment. The matrix includes unpaged requests (about
  10 MB and 3 s of database time each on table 122). In production, prefer
  `--scenarios frontend,aggregator` and a small `--repeat`.

## First vs cold

"Cold" means the first request after a backend restart, when the metadata and cell caches
are empty. The script cannot restart a backend. It reports the first request of each matrix
case and the first run of each full table load separately from the repetitions.

- Only the very first request after a restart is really cold.
- For a cold number of a single scenario or case, restart the backend and run only that one.

## Statement counts (local only)

`pgcount.py` sits between the backend and PostgreSQL. It needs a plaintext database
connection, and it slows the backend down, so never use it for timing runs.

```sh
python3 scripts/benchmark/pgcount.py serve --listen-port 15432 --control-port 15433 --upstream-port 5432 &
# start the backend with "database": {"host": "127.0.0.1", "port": 15432, ...}
python3 scripts/benchmark/pgcount.py reset          # drop the start-up statements
python3 scripts/benchmark/bench_rows.py --base-url http://127.0.0.1:8080 --table 122 --cases rows_limit500 --repeat 0
sleep 1; python3 scripts/benchmark/pgcount.py reset # prints the cold counts ("statements")
python3 scripts/benchmark/bench_rows.py --base-url http://127.0.0.1:8080 --table 122 --cases rows_limit500 --repeat 0
sleep 1; python3 scripts/benchmark/pgcount.py reset # prints the warm counts
```

The output lists the most frequent statement texts, with digits replaced by `#`. Bind
values are never read. Simple queries can still carry literals, so review the output before
you share it.

## Reference numbers

Table 122 of the `bikeparts_20261008` dump, local backend, PostgreSQL 16 with `jit=off`, before any
row-loading optimisation:

| | first (cold) | warm median |
|---|---|---|
| Frontend full table load (30 + 14 × 500, 4 parallel) | 8.8 s | 5.8 s |
| Aggregator full table load (14 × 500, sequential) | 17.4 s | 15.3 s |
| `rows_limit500_lastpage` (offset 6,500) | 2.8 s | 1.9 s |
| `rows_limit10_lastpage` (offset 6,850) | 2.2 s | 1.9 s |
| Statements of a 500-row page (`pgcount.py`) | 7,920 | 8 |

After selecting the page's row ids first and indexing link tables on `id_2` (schema v44), measured on the
same data right after a run of the previous build, warm medians:

| | before | after |
|---|---|---|
| Frontend full table load | 5.84 s | 0.92 s |
| Aggregator full table load | 15.60 s | 1.49 s |
| `rows_limit500_lastpage` | 2.02 s | 0.08 s |
| `rows_limit10_lastpage` | 1.85 s | 0.01 s |
| `rows_unpaged` | 2.69 s | 1.46 s |

All 234 response digests were identical before and after.
