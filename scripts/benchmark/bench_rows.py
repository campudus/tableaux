#!/usr/bin/env python3
"""Benchmark how fast tableaux serves the rows of one table over HTTP.

Before/after benchmark for changes to row loading (GET /tables/:id/rows and the column
rows endpoints). Python 3.8+ standard library only, nothing to install. Works against a
local backend and against a remote environment (behind a gateway, with a token).
See README.md next to this file for the workflow and reference numbers.

USAGE
  python3 scripts/benchmark/bench_rows.py --base-url URL --table ID
          [-H 'Name: value']... [--scenarios LIST] [--cases LIST] [--repeat N]
          [--out FILE] [--tag TEXT] [--timeout SEC] [--insecure]

  Local:  python3 scripts/benchmark/bench_rows.py --base-url http://127.0.0.1:8080 --table 122 --tag before
  Remote: python3 scripts/benchmark/bench_rows.py --base-url https://grud.example.com/api --table 122 \\
              -H "Authorization: Bearer $TOKEN" --scenarios frontend,aggregator --tag prod-before

PARAMETERS
  --base-url URL     Backend base URL, including any path prefix of a gateway, e.g.
                     http://127.0.0.1:8080 or https://host/api. Required.
  --table ID         Id of the table to load. Required.
  -H, --header 'Name: value'
                     Extra request header, repeatable, e.g. 'Authorization: Bearer ...'.
                     Only header names are written to the output, never their values.
  --scenarios LIST   Comma-separated scenarios, run in the given order: matrix, frontend,
                     aggregator, or all. Default: all (= matrix,frontend,aggregator); with
                     --cases the default is matrix. Error accounting always runs.
  --cases LIST       Run only these matrix cases (labels below). Default: all cases.
  --repeat N         Repetitions after the first request (matrix) or first run (full table
                     loads). Medians are taken over these. Default 3; 0 = first only.
  --out FILE         JSON-lines output file, appended to. Default ./bench_rows_results.jsonl.
  --tag TEXT         Free label stored in every record, e.g. "before" or "issue-02".
  --timeout SEC      Per-request timeout in seconds. Default 300.
  --insecure         Do not verify TLS certificates.

FIRST REQUEST VS COLD
  "Cold" means the first request after a backend restart: the column metadata and the cell
  cache are empty. The script cannot restart a backend, least of all a remote one. It
  therefore reports the first request of every matrix case and the first run of every full
  table load separately ("first") from the repetitions that follow (median, "warm").
  Only the very first request after a restart is really cold; later "first" requests find
  caches that earlier cases already filled. To measure one case or scenario cold, restart
  the backend and run only that, e.g. --scenarios frontend --repeat 0.

SCENARIOS
  1 matrix      Every case below once ("first"), then N repetitions (median). Cases run in
                this order. "First column" is the first entry of the column list other
                than the concat column (virtual, id 0; /columns/first/rows covers that
                one). The total size and a row id come from the first page, the last-page
                offset is ((totalSize - 1) // limit) * limit, the middle offset totalSize // 2.
                  columns                     GET /tables/:id/columns
                  rows_limit10 .. 1000        GET /tables/:id/rows?limit=L, L = 10 30 50 100 200 500 1000
                  rows_limit10_off50/200/1000 ?limit=10&offset=O
                  rows_limit10_lastpage       ?limit=10&offset=<last page>
                  rows_limit500_lastpage      ?limit=500&offset=<last page>
                  rows_unpaged                GET /tables/:id/rows
                  rows_unpaged_colname        ?columnNames=<first column name>
                  column_rows                 GET /tables/:id/columns/<first column id>/rows
                  column_first_rows           GET /tables/:id/columns/first/rows
                  row_single                  GET /tables/:id/rows/<first row id>
                  rows_limit500_mid           ?limit=500&offset=<middle>
                  rows_limit500_mid_archived  ... &archived=false
                  rows_limit500_mid_final     ... &final=true
                  rows_limit500_mid_colname   ... &columnNames=<first column name>
                The last four exist for the identical-response checks of later issues
                (paging combined with the row and column filters).
  2 frontend    Full table load as tableaux-frontend does it (redux/actions/rowActions.js,
                loadAllRows): offset=0&limit=30&archived=false, then limit=500&archived=false
                at offsets 30, 530, 1030, ... up to page.totalSize, in batches of 4 parallel
                requests; each batch waits for its slowest request. First run + N runs;
                reports wall time, batch wall times and per-page times.
  3 aggregator  Full table load as grud-aggregator does it: limit=500 at offsets 0, 500, ...
                up to page.totalSize, strictly sequential. First run + N runs.
  4 errors      Always on. Every response other than 200 (and every transport failure,
                reported as code 0) is counted and listed in its scenario record and in a
                final "errors" record; the stdout summary shows the count.

OUTPUT
  One JSON line per scenario run is appended to --out, plus a final "errors" line. Common
  fields: scenario, run_id (start time of the invocation, groups its records), ts, tag,
  base_url, table, header_names, repeat. A request result is
    {"code", "t" (s, until the last body byte), "ttfb" (s, until the response headers),
     "size" (body bytes), "digest" (first 16 hex chars of the body's SHA-256)}
  plus "reason" (status line, e.g. a backend error id) when code != 200 and "error" for
  transport failures. Equal digests mean byte-identical bodies, which is how before/after
  runs are checked for identical responses.
    matrix:     cases[] = {case, path, first, repeats[], median_s, size, digest, stable}
    frontend,
    aggregator: runs[] = {run ("first" or 1..N), wall_s, total_size, digest, pages[]
                (request result + offset, limit, batch)}, first_wall_s, median_wall_s,
                page_median_s[]; frontend runs also have batch_walls_s.
    errors:     n_requests, n_errors, by_code, errors[] = {scenario, case, path, code, ...}
  A compact summary table goes to stdout, progress to stderr.

DATA PROTECTION
  Response bodies contain production data. They are streamed and discarded; only their size
  and digest are kept. The column list and the first page of a run are parsed in memory to
  read the first column, page.totalSize and one row id, then dropped. Error responses are
  reported with status code and status line only. Nothing else of a response is written.

EXIT STATUS
  0 every response was 200, 1 at least one non-200 response or transport failure,
  2 usage error.
"""

import argparse
import datetime
import hashlib
import http.client
import json
import ssl
import statistics
import sys
import threading
import time
import urllib.parse

DEFAULT_OUT = "bench_rows_results.jsonl"
ALL_SCENARIOS = ("matrix", "frontend", "aggregator")
FRONTEND_FIRST_LIMIT = 30
FRONTEND_PAGE_LIMIT = 500
FRONTEND_PARALLEL = 4
AGGREGATOR_PAGE_LIMIT = 500
READ_CHUNK = 256 * 1024


def progress(msg):
    print(msg, file=sys.stderr, flush=True)


def short_digest(*parts):
    h = hashlib.sha256()
    for p in parts:
        h.update(p.encode() if isinstance(p, str) else p)
    return h.hexdigest()[:16]


# --------------------------------------------------------------------------- HTTP


class Client:
    """Minimal HTTP/1.1 GET client with one keep-alive connection per slot.

    Bodies are streamed into a digest and discarded, unless keep_body=True (then they are
    returned in memory to the caller and never written anywhere).
    """

    def __init__(self, base_url, headers, timeout, insecure):
        u = urllib.parse.urlsplit(base_url)
        if u.scheme not in ("http", "https") or not u.hostname:
            raise ValueError("--base-url must be an http(s) URL, got %r" % base_url)
        if u.username or u.password:
            raise ValueError("put credentials into a header (-H), not into --base-url")
        if u.query or u.fragment:
            raise ValueError("--base-url must not contain a query or fragment")
        self.scheme = u.scheme
        self.host = u.hostname
        self.port = u.port
        self.prefix = u.path.rstrip("/")
        self.headers = {"Accept": "application/json", "User-Agent": "tableaux-bench-rows"}
        self.headers.update(headers)
        self.timeout = timeout
        self.ssl_context = None
        if self.scheme == "https":
            self.ssl_context = ssl._create_unverified_context() if insecure else ssl.create_default_context()
        self.conns = {}

    def _connect(self):
        if self.scheme == "https":
            return http.client.HTTPSConnection(self.host, self.port, timeout=self.timeout, context=self.ssl_context)
        return http.client.HTTPConnection(self.host, self.port, timeout=self.timeout)

    def _drop(self, slot):
        conn = self.conns.pop(slot, None)
        if conn is not None:
            conn.close()

    def get(self, path, slot=0, keep_body=False):
        """GET path. Returns (result, body); body is None unless keep_body and code == 200.

        A slot must only be used by one thread at a time.
        """
        for attempt in (1, 2):
            conn = self.conns.get(slot)
            reused = conn is not None and conn.sock is not None
            if conn is None:
                conn = self.conns[slot] = self._connect()
            got_response = False
            t0 = time.perf_counter()
            try:
                conn.request("GET", self.prefix + path, headers=self.headers)
                resp = conn.getresponse()
                got_response = True
                ttfb = time.perf_counter() - t0
                digest = hashlib.sha256()
                size = 0
                kept = bytearray() if keep_body else None
                view = memoryview(bytearray(READ_CHUNK))
                while True:
                    n = resp.readinto(view)
                    if not n:
                        break
                    size += n
                    digest.update(view[:n])
                    if kept is not None:
                        kept += view[:n]
                t = time.perf_counter() - t0
                if resp.will_close:
                    self._drop(slot)
                result = {"code": resp.status, "t": round(t, 4), "ttfb": round(ttfb, 4), "size": size,
                          "digest": digest.hexdigest()[:16]}
                if resp.status != 200:
                    result["reason"] = resp.reason
                    kept = None
                return result, (bytes(kept) if kept is not None else None)
            except (http.client.HTTPException, OSError) as e:
                self._drop(slot)
                if reused and not got_response and attempt == 1:
                    continue  # the server closed an idle keep-alive connection: retry once on a fresh one
                return {"code": 0, "t": round(time.perf_counter() - t0, 4), "size": 0,
                        "reason": type(e).__name__, "error": str(e)[:200]}, None


# --------------------------------------------------------------------------- bookkeeping


class Tally:
    """Counts the requests of one scenario and collects its non-200 responses."""

    def __init__(self, scenario):
        self.scenario = scenario
        self.n_requests = 0
        self.errors = []

    def add(self, result, case, path):
        self.n_requests += 1
        if result["code"] != 200:
            err = {"scenario": self.scenario, "case": case, "path": path}
            err.update(result)
            self.errors.append(err)
            progress("  ! %s %s -> %s %s" % (case, path, result["code"], result.get("reason", "")))

    def fields(self):
        return {"n_requests": self.n_requests, "n_errors": len(self.errors), "errors": self.errors}


def median_or_none(values):
    values = list(values)
    return round(statistics.median(values), 4) if values else None


def parse_json(body):
    try:
        return json.loads(body) if body else None
    except ValueError:
        return None


def page_info(body):
    """(totalSize, first row id) from a rows response held in memory."""
    doc = parse_json(body)
    if not isinstance(doc, dict):
        return None, None
    total = (doc.get("page") or {}).get("totalSize")
    rows = doc.get("rows") or []
    row_id = rows[0].get("id") if rows and isinstance(rows[0], dict) else None
    return total, row_id


class Skip(Exception):
    pass


class TableFacts:
    """Facts the matrix needs, learnt from the first responses of its own cases.

    Only when a case needs a fact that no earlier case provided (e.g. with --cases) is an
    extra, unmeasured discovery request sent; it is listed in the record under "discovery".
    """

    def __init__(self, client, table, tally):
        self.client = client
        self.table = table
        self.tally = tally
        self.columns = None
        self.total_size = None
        self.row_id = None
        self.discovery = []

    def learn_columns(self, body):
        doc = parse_json(body)
        cols = doc.get("columns") if isinstance(doc, dict) else None
        if isinstance(cols, list):
            # skip the concat column (virtual, id 0); /columns/first/rows already covers it
            self.columns = [{"id": c.get("id"), "name": c.get("name")} for c in cols
                            if isinstance(c, dict) and c.get("id") != 0]

    def learn_page(self, body):
        total, row_id = page_info(body)
        if total is not None:
            self.total_size = total
            self.row_id = row_id

    def _discover(self, path, learn):
        result, body = self.client.get(path, keep_body=True)
        self.tally.add(result, "discovery", path)
        self.discovery.append(dict(path=path, **result))
        if body is not None:
            learn(body)

    def first_column(self):
        if self.columns is None:
            self._discover("/tables/%s/columns" % self.table, self.learn_columns)
        if not self.columns:
            raise Skip("column list unavailable or without a non-concat column")
        return self.columns[0]

    def total(self):
        if self.total_size is None:
            self._discover("/tables/%s/rows?limit=1" % self.table, self.learn_page)
        if self.total_size is None:
            raise Skip("page.totalSize unavailable")
        return self.total_size

    def first_row_id(self):
        self.total()
        if self.row_id is None:
            self.total_size = None
            self.total()  # rows_limit10 may have been skipped by --cases: ask once more
        if self.row_id is None:
            raise Skip("table has no rows")
        return self.row_id

    def summary(self):
        return {"total_size": self.total_size, "first_column": self.columns[0] if self.columns else None,
                "row_id": self.row_id}


# --------------------------------------------------------------------------- scenario 1: request matrix


def last_page_offset(total, limit):
    return max(total - 1, 0) // limit * limit


def matrix_cases(table):
    """(label, path builder, learner) in execution order."""
    t = "/tables/%s" % table
    rows = t + "/rows"

    def colname(f):
        return urllib.parse.quote(str(f.first_column()["name"]), safe="")

    def mid(f):
        return f.total() // 2

    cases = [
        ("columns", lambda f: t + "/columns", TableFacts.learn_columns),
        ("rows_limit10", lambda f: rows + "?limit=10", TableFacts.learn_page),
    ]
    for limit in (30, 50, 100, 200, 500, 1000):
        cases.append(("rows_limit%d" % limit, lambda f, limit=limit: rows + "?limit=%d" % limit, None))
    for offset in (50, 200, 1000):
        cases.append(("rows_limit10_off%d" % offset,
                      lambda f, offset=offset: rows + "?limit=10&offset=%d" % offset, None))
    cases += [
        ("rows_limit10_lastpage", lambda f: rows + "?limit=10&offset=%d" % last_page_offset(f.total(), 10), None),
        ("rows_limit500_lastpage", lambda f: rows + "?limit=500&offset=%d" % last_page_offset(f.total(), 500), None),
        ("rows_unpaged", lambda f: rows, None),
        ("rows_unpaged_colname", lambda f: rows + "?columnNames=" + colname(f), None),
        ("column_rows", lambda f: "%s/columns/%s/rows" % (t, f.first_column()["id"]), None),
        ("column_first_rows", lambda f: t + "/columns/first/rows", None),
        ("row_single", lambda f: "%s/%s" % (rows, f.first_row_id()), None),
        ("rows_limit500_mid", lambda f: rows + "?limit=500&offset=%d" % mid(f), None),
        ("rows_limit500_mid_archived", lambda f: rows + "?limit=500&offset=%d&archived=false" % mid(f), None),
        ("rows_limit500_mid_final", lambda f: rows + "?limit=500&offset=%d&final=true" % mid(f), None),
        ("rows_limit500_mid_colname", lambda f: rows + "?limit=500&offset=%d&columnNames=%s" % (mid(f), colname(f)),
         None),
    ]
    return cases


def run_matrix(client, table, repeat, only_cases):
    tally = Tally("matrix")
    facts = TableFacts(client, table, tally)
    out = []
    for label, build_path, learn in matrix_cases(table):
        if only_cases and label not in only_cases:
            continue
        try:
            path = build_path(facts)
        except Skip as e:
            out.append({"case": label, "skipped": str(e)})
            progress("[matrix] %-28s skipped: %s" % (label, e))
            continue
        first, body = client.get(path, keep_body=learn is not None)
        tally.add(first, label, path)
        if learn is not None and body is not None:
            learn(facts, body)
        del body
        repeats = []
        for _ in range(repeat):
            r, _ = client.get(path)
            tally.add(r, label, path)
            repeats.append(r)
        ok = [r for r in [first] + repeats if r["code"] == 200]
        case = {"case": label, "path": path, "first": first, "repeats": repeats,
                "median_s": median_or_none(r["t"] for r in repeats if r["code"] == 200),
                "size": first["size"], "digest": first.get("digest"),
                "stable": len({r["digest"] for r in ok}) <= 1}
        out.append(case)
        progress("[matrix] %-28s first %8.3fs  median %s  %10d B%s" % (
            label, first["t"], fmt_s(case["median_s"]), first["size"],
            "" if case["stable"] else "  (bodies differ between repetitions)"))
    record = {"scenario": "matrix", "facts": facts.summary(), "discovery": facts.discovery, "cases": out}
    record.update(tally.fields())
    return record


# --------------------------------------------------------------------------- scenarios 2 and 3: full table load


def frontend_plan(total):
    """(offset, limit) after the first 30 rows, exactly as rowActions.js loadAllRows builds them."""
    if total <= FRONTEND_FIRST_LIMIT:
        return []
    if total <= FRONTEND_PAGE_LIMIT:
        return [(FRONTEND_FIRST_LIMIT, total)]
    end = total + 1 if total % FRONTEND_PAGE_LIMIT else total
    return [(off, FRONTEND_PAGE_LIMIT) for off in range(FRONTEND_FIRST_LIMIT, end, FRONTEND_PAGE_LIMIT)]


def frontend_run(client, table, tally, run):
    rows = "/tables/%s/rows" % table
    pages, batch_walls = [], []
    t0 = time.perf_counter()
    path = "%s?offset=0&limit=%d&archived=false" % (rows, FRONTEND_FIRST_LIMIT)
    first, body = client.get(path, keep_body=True)
    total, _ = page_info(body)
    del body
    tally.add(first, "run %s" % run, path)
    pages.append(dict(first, offset=0, limit=FRONTEND_FIRST_LIMIT, batch=0))
    plan = frontend_plan(total) if total is not None else []
    for b, i in enumerate(range(0, len(plan), FRONTEND_PARALLEL), start=1):
        batch = plan[i:i + FRONTEND_PARALLEL]
        paths = ["%s?offset=%d&limit=%d&archived=false" % (rows, off, lim) for off, lim in batch]
        results = [None] * len(batch)

        def fetch(j):
            results[j] = client.get(paths[j], slot=j)[0]

        bt0 = time.perf_counter()
        threads = [threading.Thread(target=fetch, args=(j,)) for j in range(len(batch))]
        for th in threads:
            th.start()
        for th in threads:
            th.join()
        batch_walls.append(round(time.perf_counter() - bt0, 4))
        for (off, lim), p, r in zip(batch, paths, results):
            tally.add(r, "run %s" % run, p)
            pages.append(dict(r, offset=off, limit=lim, batch=b))
    wall = time.perf_counter() - t0
    return {"run": run, "wall_s": round(wall, 4), "total_size": total, "batch_walls_s": batch_walls,
            "digest": short_digest(*[p.get("digest", "-") for p in pages]), "pages": pages}


def aggregator_run(client, table, tally, run):
    rows = "/tables/%s/rows" % table
    pages = []
    t0 = time.perf_counter()
    path = "%s?offset=0&limit=%d" % (rows, AGGREGATOR_PAGE_LIMIT)
    first, body = client.get(path, keep_body=True)
    total, _ = page_info(body)
    del body
    tally.add(first, "run %s" % run, path)
    pages.append(dict(first, offset=0, limit=AGGREGATOR_PAGE_LIMIT))
    for off in range(AGGREGATOR_PAGE_LIMIT, total or 0, AGGREGATOR_PAGE_LIMIT):
        path = "%s?offset=%d&limit=%d" % (rows, off, AGGREGATOR_PAGE_LIMIT)
        r, _ = client.get(path)
        tally.add(r, "run %s" % run, path)
        pages.append(dict(r, offset=off, limit=AGGREGATOR_PAGE_LIMIT))
    wall = time.perf_counter() - t0
    return {"run": run, "wall_s": round(wall, 4), "total_size": total,
            "digest": short_digest(*[p.get("digest", "-") for p in pages]), "pages": pages}


def run_full_load(scenario, run_once, client, table, repeat):
    tally = Tally(scenario)
    runs = []
    for run in ["first"] + list(range(1, repeat + 1)):
        r = run_once(client, table, tally, run)
        runs.append(r)
        n_err = sum(1 for p in r["pages"] if p["code"] != 200)
        progress("[%s] run %-5s wall %8.3fs  %2d pages  digest %s%s" % (
            scenario, run, r["wall_s"], len(r["pages"]), r["digest"], "  %d errors" % n_err if n_err else ""))
    warm = runs[1:] or runs
    per_page = {}
    for r in warm:
        for p in r["pages"]:
            if p["code"] == 200:
                per_page.setdefault((p["offset"], p["limit"]), []).append(p["t"])
    record = {"scenario": scenario, "first_wall_s": runs[0]["wall_s"],
              "median_wall_s": median_or_none(r["wall_s"] for r in runs[1:]),
              "page_median_s": [{"offset": o, "limit": lim, "t": median_or_none(ts)}
                                for (o, lim), ts in sorted(per_page.items())],
              "stable": len({r["digest"] for r in runs}) == 1,
              "runs": runs}
    record.update(tally.fields())
    return record


# --------------------------------------------------------------------------- output


def fmt_s(v):
    return "%8.3fs" % v if v is not None else "       -"


def num(v):
    return "%.3f" % v if v is not None else "-"


def print_summary(meta, records, errors_record, out_path):
    p = print
    p("")
    p("run %s  table %s  %s  repeat %d%s" % (meta["run_id"], meta["table"], meta["base_url"], meta["repeat"],
                                             "  tag %s" % meta["tag"] if meta["tag"] else ""))
    p("%-11s %-28s %9s %9s %11s %-16s %s" % ("scenario", "case", "first_s", "median_s", "size_B", "digest", "err"))
    for rec in records:
        sc = rec["scenario"]
        if sc == "matrix":
            for c in rec["cases"]:
                if "skipped" in c:
                    p("%-11s %-28s skipped: %s" % (sc, c["case"], c["skipped"]))
                    continue
                n_err = sum(1 for r in [c["first"]] + c["repeats"] if r["code"] != 200)
                p("%-11s %-28s %9.3f %9s %11d %-16s %d%s" % (
                    sc, c["case"], c["first"]["t"], num(c["median_s"]), c["size"],
                    c["digest"] or "-", n_err, "" if c["stable"] else "  bodies differ"))
        else:
            first = rec["runs"][0]
            pages = first["pages"]
            label = "full load, %d pages" % len(pages)
            size = sum(pg["size"] for pg in pages)
            p("%-11s %-28s %9.3f %9s %11d %-16s %d%s" % (
                sc, label, rec["first_wall_s"], num(rec["median_wall_s"]), size, first["digest"],
                rec["n_errors"], "" if rec["stable"] else "  bodies differ"))
            p("%-11s   page median s (offset:t) %s" % (
                "", " ".join("%d:%.3f" % (x["offset"], x["t"]) for x in rec["page_median_s"] if x["t"] is not None)))
            if sc == "frontend" and len(rec["runs"]) > 1:
                walls = [r["batch_walls_s"] for r in rec["runs"][1:]]
                med = [median_or_none(w[i] for w in walls if i < len(w)) for i in range(max(map(len, walls)))]
                p("%-11s   batch median wall s       %s" % ("", " ".join("%.3f" % m for m in med)))
    p("non-200 responses: %d of %d requests%s" % (
        errors_record["n_errors"], errors_record["n_requests"],
        "  " + json.dumps(errors_record["by_code"]) if errors_record["n_errors"] else ""))
    for e in errors_record["errors"][:20]:
        p("  %s %s %s -> %s %s %s" % (e["scenario"], e["case"], e["path"], e["code"], e.get("reason", ""),
                                     e.get("error", "")))
    if errors_record["n_errors"] > 20:
        p("  ... %d more, see %s" % (errors_record["n_errors"] - 20, out_path))
    p("results appended to %s" % out_path)


def parse_header(text):
    name, sep, value = text.partition(":")
    if not sep or not name.strip():
        raise argparse.ArgumentTypeError("header must look like 'Name: value', got %r" % text)
    return name.strip(), value.strip()


def parse_args(argv):
    ap = argparse.ArgumentParser(description="Benchmark tableaux row loading. See the module docstring for details.")
    ap.add_argument("--base-url", required=True)
    ap.add_argument("--table", required=True, type=int)
    ap.add_argument("-H", "--header", action="append", default=[], type=parse_header, metavar="'Name: value'")
    ap.add_argument("--scenarios", default=None, help="comma-separated: matrix,frontend,aggregator or all")
    ap.add_argument("--cases", default=None, help="comma-separated matrix case labels")
    ap.add_argument("--repeat", type=int, default=3)
    ap.add_argument("--out", default=DEFAULT_OUT)
    ap.add_argument("--tag", default="")
    ap.add_argument("--timeout", type=float, default=300.0)
    ap.add_argument("--insecure", action="store_true")
    args = ap.parse_args(argv)

    if args.repeat < 0:
        ap.error("--repeat must be >= 0")
    args.cases = [c.strip() for c in args.cases.split(",") if c.strip()] if args.cases else None
    known_cases = [label for label, _, _ in matrix_cases(args.table)]
    for c in args.cases or []:
        if c not in known_cases:
            ap.error("unknown matrix case %r; known: %s" % (c, ", ".join(known_cases)))
    if args.scenarios is None:
        args.scenarios = ["matrix"] if args.cases else list(ALL_SCENARIOS)
    else:
        sc = [s.strip() for s in args.scenarios.split(",") if s.strip()]
        args.scenarios = list(ALL_SCENARIOS) if sc == ["all"] else sc
        for s in args.scenarios:
            if s not in ALL_SCENARIOS:
                ap.error("unknown scenario %r; known: %s, all" % (s, ", ".join(ALL_SCENARIOS)))
    return args


def main(argv=None):
    args = parse_args(sys.argv[1:] if argv is None else argv)
    try:
        client = Client(args.base_url, dict(args.header), args.timeout, args.insecure)
    except ValueError as e:
        print("error: %s" % e, file=sys.stderr)
        return 2
    u = urllib.parse.urlsplit(args.base_url)
    meta = {"run_id": datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "tag": args.tag, "base_url": urllib.parse.urlunsplit((u.scheme, u.netloc, u.path, "", "")),
            "table": args.table, "header_names": [n for n, _ in args.header], "repeat": args.repeat}

    out = open(args.out, "a", encoding="utf-8")

    def emit(record):
        line = {"scenario": record["scenario"]}
        line.update(meta)
        line["ts"] = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        line.update(record)
        out.write(json.dumps(line, separators=(",", ":")) + "\n")
        out.flush()

    records = []
    progress("benchmark %s table %s, scenarios %s, repeat %d -> %s" % (
        meta["base_url"], args.table, ",".join(args.scenarios), args.repeat, args.out))
    for scenario in args.scenarios:
        if scenario == "matrix":
            rec = run_matrix(client, args.table, args.repeat, args.cases)
        elif scenario == "frontend":
            rec = run_full_load("frontend", frontend_run, client, args.table, args.repeat)
        else:
            rec = run_full_load("aggregator", aggregator_run, client, args.table, args.repeat)
        emit(rec)
        records.append(rec)

    errors = [e for rec in records for e in rec["errors"]]
    by_code = {}
    for e in errors:
        by_code[str(e["code"])] = by_code.get(str(e["code"]), 0) + 1
    errors_record = {"scenario": "errors", "n_requests": sum(r["n_requests"] for r in records),
                     "n_errors": len(errors), "by_code": by_code, "errors": errors}
    emit(errors_record)
    out.close()
    print_summary(meta, records, errors_record, args.out)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
