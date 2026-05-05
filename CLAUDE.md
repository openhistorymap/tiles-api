# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

The parent `/srv/ohm/CLAUDE.md` describes the wider OHM ecosystem, the shared MongoDB/PostGIS/Redis credentials, and the cross-service time-encoding conventions. Read it first; this file only covers what is specific to `ohm_tileserver/`.

## What this service is

A FastAPI service that publishes Mapbox Vector Tiles for the OHM persistent (non-ephemeral) timeline from PostGIS `ohm_prod`, and a sibling FastAPI ingest API that pushes incoming GeoJSON onto a Redis queue for an out-of-process worker to store.

The same `app/` tree is built into two Docker images by switching the `uvicorn` entrypoint:

| Image | Built by | Entrypoint | Purpose |
| --- | --- | --- | --- |
| api | `Dockerfile.api` | `uvicorn app.api:app` | `POST /items` ingest, `GET /status` |
| tileserver | `Dockerfile.tileserver` | `uvicorn app.tileserver:app` | `GET /{timeline}/{year}/{z}/{y}/{x}/vector.pbf`, `GET /relation/{ids}` |

Both Dockerfiles share `requirements.txt` (Python 3.9, FastAPI 0.68, uvicorn 0.15, GeoAlchemy2, psycopg2-binary, Shapely 1.7, pyproj 3.2). Don't bump major versions of pyproj/Shapely without checking — the code uses the legacy `pyproj.Proj(init='epsg:4326')` form.

## Components in `app/`

- **`api.py`** — the ingest API (`app.api:app`). Reprojects each incoming GeoJSON feature 4326→3857, splits multi-geometries, and pushes one Redis list entry per (layer, geom) onto queue `store` (Redis DB `REDIS_DB`, default 3). Polygons additionally produce a synthetic `<layer>_label` point at the representative point.
- **`tileserver.py`** — the tile API (`app.tileserver:app`). For each `vector.pbf` request it builds a `(layer, admin_level_field, admin_level_limit)` spec list, then runs **one** SQL call (`TILE_SQL`) that calls the Postgres function `ohm_tile(z, x, y, yr, lyr, fld, lmt)` per layer via `unnest` and `string_agg`s the resulting MVT bytes. The blocking query is dispatched with `asyncio.to_thread` so a single worker can serve concurrent tiles. Responses carry an MD5 ETag and honour `If-None-Match` (304).
- **`db.py`** — schema, partitioning, and worker. Defines the SQLAlchemy models (`OhmItems`, `OhmRels`, `OhmGeometries`, `OhmRelMembers`), the partition layout, and the two PL/pgSQL functions `ohm_storegeometryhash` and `ohm_tile` as Python string constants. Provides a Click CLI: `initdb` (creates extensions, tables, and procedures) and `run` (Redis-driven worker pool).
- **`bots.py`** — an alternative Flask-shaped ingest module. **Not wired into any Dockerfile.** It also has a leftover `jsonify(...)` from a Flask port and will fail at runtime. Don't treat it as live code; if you need to revive it, port the response to FastAPI first.
- **`bot/movement.py`, `bot/parent_boundary.py`** — bot scripts loaded dynamically by the worker (see below). `movement.py` still does `from db import ohm_rel_members`, which doesn't resolve under the current package layout (`app.db`) — it has not been run since the package move.
- **`api copy.py`** is an editor backup of `api.py`. Don't edit it.

## Tile pipeline (read this before editing `tileserver.py`)

1. Client requests `/{timeline}/{year}/{z}/{y}/{x}/vector.pbf?layers=a,b,c`.
2. `year` (a float) is snapped to a month with `get_month(year)` (step `1/12/31`, the same `EPHEMERAL_STEP` the rest of OHM uses).
3. `_build_specs(z, layers)` decides which layers are eligible at this zoom. Below `limit_zoom = 4`, only the layers listed in `filters` (boundary/culture and their `_label` siblings) survive, and an `admin_level` cap of `max(2, z/2)` is applied via the PL/pgSQL function's `field`/`lim` arguments. At z ≥ 4, all requested layers pass through with `field/lim = NULL`.
4. `TILE_SQL` issues a single query that loops over `(layer, field, limit)` triples with `unnest`, calls `ohm_tile(...)` for each, and `string_agg`s the bytes — concatenated MVT layers form a valid tile body.
5. The handler hashes the result for an ETag, returns `application/x-protobuf`, and sets `Cache-Control: public, max-age=3600`.

There are two Redis configs in `tileserver.py` (`REDIS_DB`, `REDIS_DB_2`), but the current implementation does not actually read or write the cache — `cache` is constructed and unused. Keep the env vars; clients elsewhere expect them. The legacy per-layer streaming generator (`StreamingResponse` over an async `gen(...)`) was replaced by the single-query path; if you re-introduce streaming, also re-introduce the per-layer cache lookups it used to do.

## Storage & ingest pipeline

The `run` Click command in `db.py` is the worker that pairs with `api.py`:

```
python -m app.db run --workers 20 store,bot
```

It blocks on `BLPOP` against the Redis lists named on the command line. Two queue types are recognised:

- `store` — payload is the JSON dict produced by `api.py`'s `map_single`. Worker calls `ohm_storegeometryhash(...)` (PL/pgSQL) to dedupe & simplify the geometry across `zoom_levels` precisions, then inserts an `OhmItems` row referring to the resulting hash. As a quirk, layer `places` is silently rewritten to `place` before insert.
- `bot` — payload is a bot module name (e.g. `parent_boundary`). The worker does `importlib.import_module('bot.%s' % v)` and calls its `run(db)`. There is **no module name validation**; only invoke this from trusted code paths.

`initdb` creates the partitioning hierarchy (`items` and `relations` partitioned by `LIST(layer)` then `RANGE(ohm_from)` using the year buckets in `timepartitions`; `geometries` partitioned by geometry type and zoom range). Re-running `initdb` on an existing database will fail on the `CREATE EXTENSION` and table `CREATE`s — it expects an empty database.

## Building & running

```bash
# build the two images from one tree
docker build -f Dockerfile.api        -t ohm-tileserver-api .
docker build -f Dockerfile.tileserver -t ohm-tileserver .

# local dev (Python 3.9, embedded venv at bin/lib/include/share is gitignored)
pip install -r requirements.txt
uvicorn app.api:app        --host 0.0.0.0 --port 9039  # ingest
uvicorn app.tileserver:app --host 0.0.0.0 --port 9034  # tiles

# one-time DB setup (against an empty Postgres database)
python -m app.db initdb --dbname ohm_prod

# storage/bot worker
python -m app.db run --workers 20 store,bot
```

There are no tests, no linter config, and no CI in this directory. Don't claim a change "passes tests" — there are none to pass.

## Vercel deployment (tileserver only)

The Vercel scaffold deploys **only** `app.tileserver` — the read-only tile API. The ingest API (`app.api`), the storage/bot worker (`app.db run`), and the bot scripts (`app/bot/`) stay on the Docker path; they don't fit Vercel (worker is long-running, ingest is paired with the worker through Redis). This split is also the right conceptual line for working on the codebase: `app/tileserver.py` is genuinely standalone — it imports nothing from `app/api.py`, `app/bots.py`, `app/db.py`, or `app/bot/`.

Layout:

- `api/index.py` — `from app.tileserver import app`. All paths route here.
- `api/requirements.txt` — Vercel-only dep pins (Python 3.12 wheels: numpy 1.26+, pyproj 3.6+, Shapely 2.x, SQLAlchemy 1.4.50+ pinned `<2.0` so the codebase's `engine.execute(text(...))` style still works). The project-root `requirements.txt` is left at its 3.9-era pins for the Docker images.
- `vercel.json` — rewrites every path to `/api/index`; Vercel preserves the original URL in the ASGI scope so FastAPI's path matching still works.
- `.vercelignore` — excludes the embedded venv (`bin/`, `lib/`, `include/`, `share/`), the Dockerfiles, the project-root `requirements.txt`, **and the ingest/worker/bot modules** (`app/api.py`, `app/bots.py`, `app/db.py`, `app/bot/`). The Vercel bundle therefore contains only `app/__init__.py` + `app/tileserver.py` from `app/`.

What stays on Docker (do not try to push these to Vercel):

- `Dockerfile.api` → `app.api:app` ingest API. Runs on a VM/container alongside the worker. The pair share Redis (`store` queue) with the worker.
- `app.db run` storage/bot worker. Blocks on Redis `BLPOP`. Same VM as the ingest is fine.
- `app.db initdb`. One-shot CLI; run from a VM/laptop with Postgres reachable.

Required environment variables in the Vercel project (the defaults in source point at production PostGIS):

- `POSTGRES`, `POSTGRES_USER`, `POSTGRES_PASS`, `POSTGRES_DBNAME` — must be reachable from Vercel egress IPs. Use a pooler (PgBouncer / Supabase / Neon) — `tileserver.py` constructs `create_engine(..., pool_size=20)` per cold lambda, which will exhaust `max_connections` against direct Postgres under any real load.
- `REDIS_HOST`, `REDIS_PORT`, `REDIS_DB`, `REDIS_DB_2` — currently constructed but unused by `tileserver.py`'s active code paths. Either set them to a network-reachable Redis (Upstash etc.) or delete the unused `cache = Redis(...)` line; the default `localhost:6379` won't connect to anything from a Vercel function.
- `TILES_LAYERS`, `EXPIRY`, `EXPANDER` — optional, defaults are fine.

Cold-start notes: `pyproj` + `Shapely` + `numpy` push the bundle close to the 250 MB unzipped limit. None of those libraries are actually called in `tileserver.py`'s active code paths — they're only imported — so if size becomes a problem, the cheapest win is to delete the unused imports rather than to swap libraries.

## Things to watch out for

- The three Python entrypoints (`api.py`, `tileserver.py`, `bots.py`) each carry their own copies of `EPHEMERAL_STEP`, `PERSIST_STEP`, `get_month`, and `float_to_date`. The parent `CLAUDE.md` notes the same helpers are also duplicated across sibling services (`ohms`, `ohm-events-api`). If you change one, change them all.
- `tileserver.py` and `db.py` both default `POSTGRES` to `51.15.160.236:25432` and database `ohm_prod` — running locally without overriding the env vars will hit production PostGIS.
- The PL/pgSQL functions `ohm_storegeometryhash` and `ohm_tile` are defined as string constants in `db.py` and are **only** installed by `initdb`. Editing them in the Python source has no effect until `initdb` is re-run against the target DB; in practice you usually want to apply the changed `CREATE OR REPLACE FUNCTION` directly to the live DB instead.
- `ohm_tile` queries `items.layer = lyr` and joins on the geometry hash — the partition pruning relies on the layer being a literal at SQL-parse time; the current parameterised approach passes layer through `unnest`, which Postgres can still partition-prune on a `LIST` partitioning key but only when statistics support it. If you see slow plans, check `EXPLAIN` for the `Append`/`Seq Scan` shape before assuming the SQL is wrong.
- `api.py`'s pydantic models are extremely loose (`properties: Any`, `itms: List[GeoJ]`). Validation effectively happens inside `map_single`. Don't tighten the schema without checking what the upstream callers (`ohmp`, importers) actually post.
