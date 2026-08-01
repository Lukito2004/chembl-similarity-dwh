# chembl-similarity-dwh

Finds the top-10 most similar ChEMBL molecules for each compound in a personal input set,
using Morgan fingerprints and Tanimoto similarity.

The pipeline ingests ChEMBL into a medallion data warehouse on PostgreSQL, computes
fingerprints and similarity scores, publishes the results to S3, and serves a star-schema
data mart. ChEMBL is loaded either from the official release dump or from the public REST
API, and both paths land identical data. Everything is orchestrated with Apache Airflow and
runs from a single `docker compose up`.

## Running locally

Requires Docker with Compose v2. The stack is five services: Airflow
(webserver, scheduler, triggerer) on a LocalExecutor, its own metadata Postgres,
and a separate Postgres holding the data warehouse.

```bash
cp .env.example .env
```

Generate the two Airflow secrets and paste them into `.env`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"
```

Then:

```bash
docker compose build
docker compose up -d
docker compose ps
```

| Service | Address | Credentials |
| --- | --- | --- |
| Airflow UI | `http://localhost:$AIRFLOW_WEB_PORT` (default 8080) | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Warehouse Postgres | `localhost:$CHEMBL_DWH_HOST_PORT` (default 5433) | `CHEMBL_DWH_USER` / `CHEMBL_DWH_PASSWORD` |

The warehouse port is published so you can attach a SQL client and query the
data mart directly. The Airflow metadata database is not published.

Tear down with `docker compose down`, or `docker compose down -v` to also drop
both database volumes.

## Pipeline

Three DAGs, linked by an Airflow dataset rather than by schedule, so the downstream
work starts when its input actually changes.

| DAG | Trigger | Does |
| --- | --- | --- |
| `chembl_ingest` | `@monthly` | Loads ChEMBL into `bronze`, then conforms it into `silver.molecule` |
| `fingerprint_build` | `silver.molecule` dataset | Morgan fingerprints for every molecule, published to S3 |
| `input_compounds` | `@daily` | Lands the personal input files and selects the source molecule set |

`chembl_ingest` publishes the `chembl://warehouse/silver.molecule` dataset when
`build_silver` succeeds; `fingerprint_build` is scheduled on that dataset and needs no
schedule of its own. `input_compounds` is independent because the input files change on
their own cadence.

Every DAG is safe to re-run. `chembl_ingest` skips entirely when the release it finds is
already loaded, the fingerprint shards are keyed deterministically so a retry overwrites
rather than duplicates, and the source set is rebuilt wholesale each run.

## Warehouse architecture

The warehouse follows the medallion pattern over three data layers, plus a fourth schema
that holds pipeline state so operational rows are never mistaken for data.

| Schema | Holds | Rebuilt from |
| --- | --- | --- |
| `bronze` | ChEMBL exactly as delivered, plus the raw input files | the source |
| `silver` | typed, deduplicated, conformed molecules and the source set | `bronze` |
| `gold` | the star schema data mart and its views | `silver` |
| `meta` | ingest watermarks | not data |

**Bronze is a faithful landing and stores nothing else.** The ChEMBL API returns decimal
properties as JSON *strings* (`"alogp": "1.31"`, `"max_phase": "4.0"`) while returning
integers as numbers, so bronze mirrors that split: `text` where the source sends text,
`integer` where it sends an integer. Only primary keys are `NOT NULL`. A landing layer
that rejects a row because of an unexpected value has failed at its one job.

**Silver is where the typing happens**, and it has to be total. Because bronze holds text,
a naive `::numeric` cast would simply move the failure one layer downstream, so the
conversion goes through `silver.safe_numeric`, which returns `NULL` for anything that is
not a plain number rather than raising. Silver also applies the one business rule the whole
pipeline depends on: a molecule without a compound structure cannot be fingerprinted or
scored, so `silver.molecule` is restricted to the 2,897,819 molecules that have one.

**Gold is the data mart** — `dim_molecule` and `fact_molecule_similarity` in a star schema,
restricted to molecules the fact table actually references, plus the delivery views.

Current volumes:

| Table | Rows |
| --- | --- |
| `bronze.chembl_id_lookup` | 5,478,952 |
| `bronze.molecule_dictionary` | 2,921,148 |
| `bronze.compound_properties` | 2,901,464 |
| `bronze.compound_structures` | 2,897,819 |
| `silver.molecule` | 2,897,819 |
| `silver.source_molecule` | 100 |

The warehouse occupies roughly 5.8 GB.

Schema changes are applied with `CREATE TABLE IF NOT EXISTS`, which creates missing objects
but never alters existing ones. Adding or changing a column therefore needs the warehouse
volume recreated and the data reloaded. That is a deliberate trade for a warehouse that is
fully reproducible from its sources, and it avoids pulling in a migration tool for a
pipeline that can rebuild itself.

## Ingesting ChEMBL

The pipeline supports two ingest paths, selected by the `ingest_path` DAG parameter. Both
land the identical bronze contract, so nothing downstream can tell them apart.

- **`dump`** (default) downloads the official release archive, restores the four required
  tables with `pg_restore`, and drains each into bronze.
- **`api`** pages the ChEMBL REST API with a rate limiter, retry with exponential backoff,
  and a resumable offset watermark.

### Why the dump is the default

The REST API was measured repeatedly over one working day. The results did not converge:

| Measurement | Result |
| --- | --- |
| Page latency, morning | 1.8 – 3.5 s |
| Page latency, afternoon | 21 – 52 s, median 32.9 s |
| 6 concurrent workers | 4.46 s/page, projecting **9.9 h** |
| 12 concurrent workers | **5 of 12 requests returned HTTP 500** |
| 24 concurrent workers | 0.55 s/page, projecting 1.3 h |
| Later, healthy | 1.26 s/page (molecules), 0.68 s/page (lookup), projecting **~2 h** |
| At one point | **complete outage — HTTP 500 on every endpoint for over 15 minutes** |

Two things follow. First, no single figure honestly describes the API path, so the design
has to tolerate the spread rather than assume a number: hence the retries, the resumable
watermark, and a configurable worker count. Second, the server genuinely fails under
concurrency — the 500s at twelve workers were not simulated.

The decisive difference is bytes, not latency. **The API does not compress** — verified by
comparing an explicit `Accept-Encoding: identity` request against a `gzip` one, which
returned byte-identical sizes with no `content-encoding` header. So the two paths move very
different amounts of data for the same result:

| Path | Transferred | Observed wall time |
| --- | --- | --- |
| REST API, both endpoints | ~12.6 GB of uncompressed JSON | ~2 h when healthy, unbounded when not |
| Release dump | **2.09 GB compressed** | ~12 min download, ~35 min restore and drain |

The course Q&A confirmed there is no requirement on the fetch mechanism, only that it must
not be a manual operation. The dump path is fully automated inside Airflow — discovery,
resumable download, SHA-256 verification, selective restore and drain are all tasks — so it
meets that condition while moving six times fewer bytes and remaining available when the
API is not.

The API path is kept, tested and runnable (`ingest_path=api`), and the rate limiter the
course Q&A suggested is implemented in `chembl_sim/chembl/client.py`.

### Restoring without exhausting disk

A naive restore would hold each ChEMBL table twice — once in the staging schema and once in
bronze — peaking near 20 GB. Instead each table is restored, drained into bronze and
dropped in turn, so at most one staging table exists at a time and the peak stays near
11 GB. `pg_restore` runs with `--section=pre-data --section=data`, skipping index and
constraint creation, because each table is read exactly once by a hash join and a unique
index over 2.9 million `standard_inchi` values would be pure waste.

The warehouse database runs PostgreSQL 17 to match the `pg_restore` client shipped in the
Airflow image. A 17 client emits `SET transaction_timeout`, which a 16 server rejects, and
the restore aborts on its first statement. Airflow's own metadata database stays on 16,
which is the version Airflow 2.10 supports.

## Fingerprints

Morgan fingerprints, radius 2 and 2048 bits as the task specifies, computed with RDKit's
`rdFingerprintGenerator` and stored as **packed bytes** — `np.packbits` turns the 2048-bit
vector into 256 bytes, rather than a 2048-character bit string.

Measured throughput is 10,641 molecules per second on a single thread, so the entire
library takes about 4.5 minutes. Work is still split into shards, but for
retriability and bounded memory rather than speed: each shard is an independent Airflow
mapped task that can be retried alone, and each writes exactly one parquet object.

Shard boundaries come from a single `row_number()` pass, and each shard then reads a
contiguous `chembl_id` range through the primary key index. Deep `OFFSET` paging was
rejected because the later shards would rescan millions of rows to reach their start.

Results of a full run:

| Measure | Value |
| --- | --- |
| Objects written | 12 parquet files |
| Total size | **165 MB** |
| Fingerprints | 2,897,802 |
| Duplicate identifiers | **0** |
| Molecules RDKit could not parse | 17 |

Zero duplicates across 2.9 million rows is the check that matters: because the shards are
contiguous key ranges, an off-by-one would show as either overlap or a gap, and neither
occurred.

The 165 MB is far below what the raw arithmetic suggests (2.9M × 256 B ≈ 740 MB). Morgan
fingerprints are sparse — typically around 43 of 2048 bits set — so the packed bytes are
mostly zeros and compress about fourfold under zstd.

## The source molecule set

The task asks for the top 10 most similar molecules for each of 100 chosen molecules. The
personal input files under `input/luka-javakhishvili/` supply 58 rows across five files,
and they identify compounds by **name**, not by ChEMBL identifier or structure.

The five files also disagree on their columns: `batch_004.csv` has no `logp`, and
`batch_005.csv` uses `IC50_nM` and `collection_date` where the others use `ic50_nm` and
`assay_date`. Headers are therefore normalised to lower case and mapped onto the union of
all columns, with absent columns landing as `NULL`.

Names are resolved against ChEMBL's preferred names. Of 57 named rows, **56 resolve to
exactly one molecule** and none resolve ambiguously. The remaining set is topped up to 100
from molecules ordered by `md5(chembl_id)` — deterministic, reproducible on any machine,
and spread across the whole library rather than clustered on the earliest ChEMBL entries as
ordering by identifier would be. Re-running the selection reproduces the identical 100.

| Origin | Count |
| --- | --- |
| `input_file` | 56 |
| `top_up` | 44 |
| **Total** | **100** |

Every source molecule records its `origin`, so the composition is auditable in one query.

Two input rows are rejected, and both are recorded in `silver.source_molecule_rejected`
with a reason rather than silently dropped:

| File | Compound | Reason |
| --- | --- | --- |
| `batch_004.csv` | *(blank)* | compound name is blank |
| `batch_001.csv` | Paracetamol | name matches no ChEMBL preferred name |

Paracetamol is a real name collision rather than a data error: ChEMBL records it under
`ACETAMINOPHEN`. Resolving it would need the `molecule_synonyms` table, which is outside
the four tables this project is asked to ingest, so it is left as a documented rejection.

## Similarity search

Tanimoto similarity is computed directly on the packed bytes: the intersection is
`popcount(a AND b)` and the union is `popcount(a) + popcount(b) - intersection`. Library
popcounts are computed once per run and reused for every source.

Airflow 2.10 pins numpy to 1.26, which predates `np.bitwise_count`, so a 256-entry lookup
table supplies the popcount. The code uses the native operation when the numpy version
offers one, so nothing needs changing if the pin moves.

Scoring is blocked at 250,000 molecules so the intermediate `AND` never materialises for
the whole library at once. Measured on the full set:

| Measure | Value |
| --- | --- |
| Library size | 2,897,802 fingerprints, 742 MB packed |
| Load from S3 | 192 s |
| Library popcounts | 1.7 s, once per run |
| Scoring, one source against all | **1.70 s** |
| 100 source molecules | **2.8 min** |
| Peak resident memory | 2.1 GB |

Because loading the library costs two orders of magnitude more than scoring a single
source, the whole search runs as one task rather than as mapped tasks — parallelising it
would pay the 192 second load again in every worker.

Ties matter more than they might appear. Tanimoto over 2048-bit fingerprints yields
rationals with small denominators, so identical scores are common rather than rare. The
top-N selection therefore takes every molecule at or above the Nth score, orders by score
descending then by identifier ascending so the result is deterministic, and sets
`has_duplicates_of_last_largest_score` on the included rows holding the boundary score
whenever more molecules share it than there is room for.

Across the 100 source molecules the search produces 1,000 top-10 rows, of which 40 carry
`has_duplicates_of_last_largest_score`, spread over 24 sources. Boundary ties are the
common case rather than an edge case, which is why the flag exists.

Each source molecule's full score table is written to S3 as a self-contained parquet
object carrying `source_chembl_id`, `target_chembl_id` and `tanimoto_score`. The constant
source column costs nothing — it dictionary-encodes to a single entry — so the file is the
same size as a minimal one while remaining readable on its own. With zstd each is about
15.5 MB, roughly 1.55 GB for all 100, against 27.4 MB each under snappy.

## Known data gaps

**`cx_logp` and `molecular_species` are always `NULL`.** Both columns are required in the
dimension table, and neither exists in ChEMBL 37. This is not an API limitation: the
official schema documentation for the release lists `COMPOUND_PROPERTIES` with 15 columns
— `molregno` plus `mw_freebase`, `alogp`, `hba`, `hbd`, `psa`, `rtb`, `ro3_pass`,
`num_ro5_violations`, `full_mwt`, `aromatic_rings`, `heavy_atoms`, `qed_weighted`,
`full_molformula` and `np_likeness_score` — and neither field is among them. The columns
are kept in `silver.molecule` and `gold.dim_molecule` so the schema matches the
specification and a backfill from an older release has somewhere to land.

**17 molecules have no fingerprint.** Their SMILES cannot be parsed by RDKit. They are
counted and logged per shard rather than failing the run.

Each source molecule's full score table is written to S3 as a self-contained parquet
object carrying `source_chembl_id`, `target_chembl_id` and `tanimoto_score`. The constant
source column costs nothing — it dictionary-encodes to a single entry — so the file is the
same size as a minimal one while remaining readable on its own. Under zstd each object is
about 7.1 MB, 0.71 GB for all 100. A benchmark against uniform random floats predicted
15.5 MB each; real scores cluster near zero and are heavily quantised, so they compress
more than twice as well.

Scoring is done in double precision and archived as float32. Float32 was measured against
float64 over twenty sources and 58 million comparisons: the top-10 ordering, the tie flags
and the count of distinct values were identical, so float32 loses nothing the search can
distinguish. It does however render exact ratios awkwardly — 7/10 becomes 0.69999999 — so
the values that reach the mart are computed as doubles, while the S3 archive keeps float32
and stays half the size. Rounding was rejected: at six decimal places distinct scores
would genuinely collide, since two Tanimoto values can differ by as little as 1/2048².

And add a row to the measurement table:

| Full task, including the S3 upload | **24.5 min** |
