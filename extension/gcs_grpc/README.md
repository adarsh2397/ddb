# gcs_grpc — GCS reads over gRPC BidiReadObject / DirectPath

Read-only prototype that serves `gs://` / `gcs://` paths through the
`google.storage.v2` gRPC API instead of httpfs (HTTP/XML). Ported from the
Sirius `gcs_grpc_reactor`. On a GCE VM co-located with a Rapid (zonal) bucket
it uses **BidiReadObject** streams over **DirectPath** (`google-c2p:///`,
ALTS), bypassing the GFE.

When loaded, this filesystem takes over `gs://`; `SET gcs_grpc_enabled=false`
falls back to httpfs (which also handles globbing and writes — both
unimplemented here).

## Build (GCE VM)

Dependencies (conda/pixi/mamba, same toolchain for runtime + codegen):

```bash
micromamba install -c conda-forge 'libgrpc>=1.62' libprotobuf grpc-cpp-plugin cmake ninja
# protoc + grpc_cpp_plugin must come from the SAME toolchain as the libs
```

```bash
CORE_EXTENSIONS="gcs_grpc" GEN=ninja make reldebug
./build/reldebug/duckdb
```

Notes:
- gRPC >= 1.62 is required (RLS-based Rapid Storage redirect routing; older
  versions loop on redirects).
- The first configure FetchContent-clones googleapis and protoc-generates the
  storage.v2 stubs (see `gcs_grpc_proto.cmake`, including the `[ctype = CORD]`
  strip that keeps `ChecksummedData::content()` public).
- If protoc reports an unresolved import, add the named .proto to
  `GCS_GRPC_PROTO_FILES` in `gcs_grpc_proto.cmake`.

## Settings

| Setting | Default | Meaning |
|---|---|---|
| `gcs_grpc_enabled` | `true` | Handle gs:// here; `false` → httpfs |
| `gcs_grpc_transport` | `directpath` | `directpath` (c2p + ALTS, GCE only) or `cloudpath` (TLS + bearer) |
| `gcs_grpc_endpoint` | `storage.googleapis.com` | Target host |
| `gcs_grpc_num_channels` | `4` | Channel lanes (own TCP conn + CQ worker each) |
| `gcs_grpc_max_streams` | `16` | Total concurrent streams (split across lanes) |
| `gcs_grpc_bidi_reads` | `auto` | BidiReadObject policy: `off` / `on` / `auto` |
| `gcs_grpc_target_read_bytes` | 16 MiB | Sub-read split size for large ranges |
| `gcs_grpc_bearer_token` | `""` | Static token override (else GCE metadata server) |
| `gcs_grpc_verbose` | `false` | Transport INFO logs + 10s throughput stats to stderr |

Settings are snapshotted when the reactor starts (first gs:// access) — set
them before the first read; changing them afterwards needs a fresh process.

## Smoke test

```sql
SELECT count(*) FROM duckdb_settings() WHERE name LIKE 'gcs_grpc%';  -- 9
SET gcs_grpc_verbose = true;
SELECT count(*) FROM 'gs://<rapid-bucket>/<file>.parquet';
```

Expected stderr with a Rapid bucket + DirectPath:
- `reactor up | channels=... bidi_reads=auto directpath=1 target=google-c2p:///storage.googleapis.com ...`
- `Rapid Storage redirect handshake active for bucket=...` (expected for zonal)
- `BidiReadObject ACTIVE for bucket=...`
- shutdown totals with `bidi_sessions > 0`, `bidi_fallbacks = 0`

Sanity variants:
- `SET gcs_grpc_bidi_reads='off';` → totals show `unary_streams > 0`
- `SET gcs_grpc_transport='cloudpath';` (fresh process) → unary via GFE, OAuth2
  token from the metadata server
- `SET gcs_grpc_enabled=false;` → next gs:// read autoloads httpfs and works

Debugging DirectPath routing:
```bash
GRPC_TRACE=google_c2p_resolver,rls_lb,xds_client GRPC_VERBOSITY=DEBUG ./build/reldebug/duckdb
```

## Benchmark vs httpfs

Same VM + zonal bucket, cold cache per run (fresh process, or
`SET enable_external_file_cache=false`), `.timer on`:

- A: defaults (directpath + bidi auto)
- B: `SET gcs_grpc_bidi_reads='off'` (unary over DirectPath)
- C: `SET gcs_grpc_transport='cloudpath'` (unary via GFE)
- D: `SET gcs_grpc_enabled=false` (httpfs baseline)

Sweep `gcs_grpc_num_channels` in {2,4,8} and `gcs_grpc_max_streams` in
{8,16,32}; the 10s stats lines (`gcs_grpc_verbose=true`) give MiB/s per run.

## Known limitations (prototype)

- Read-only; write/append flags throw.
- No glob patterns (exact paths only). Follow-up: `Storage.ListObjects` with
  `match_glob` for server-side globbing.
- No CREATE SECRET integration; auth = metadata server or static token.
- Statically linked only (no loadable build).
