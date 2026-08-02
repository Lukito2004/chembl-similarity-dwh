# chembl-similarity-dwh

For every compound in a personal input set, this project finds the ten most similar
molecules in ChEMBL using Morgan fingerprints and Tanimoto similarity.

ChEMBL is loaded into a medallion warehouse on PostgreSQL, fingerprints and similarity
scores are computed and published to S3 and a star schema data mart is built on top.
Loading works from either the official release dump or the public REST API and both
routes produce the same data. Apache Airflow orchestrates everything and the whole stack
starts with one `docker compose up`.

## Running locally

You need Docker with Compose v2. Five services come up: three Airflow processes
(webserver, scheduler, triggerer) running a LocalExecutor, a Postgres for Airflow's own
metadata and a second Postgres holding the warehouse.

```bash
cp .env.example .env
```

Two Airflow secrets have to be generated and pasted into `.env`:

```bash
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
python -c "import secrets; print(secrets.token_hex(32))"
```

### AWS access

Fingerprints and similarity scores live on S3, so three of the four DAGs need working
credentials. `AWS_PROFILE` and `AWS_DEFAULT_REGION` come from `.env`, `CHEMBL_S3_BUCKET`
together with `CHEMBL_S3_ROOT_PREFIX` decides where objects are written and
`CHEMBL_INPUT_PREFIX` points at the personal input files inside that bucket.

No credential is ever copied into the project. `docker-compose.yml` mounts `${HOME}/.aws`
into the containers, so the profile is resolved from the host:

```bash
aws sso login --profile De-School-students
```

That token expires after a few hours. Once it does, every task touching S3 fails with
`TokenRetrievalError` and the fix is to run the command again. The mount is deliberately
writable rather than read-only, because botocore refreshes its token cache in place.

After that:

```bash
docker compose build
docker compose up -d
docker compose ps
```

| Service | Address | Credentials |
| --- | --- | --- |
| Airflow UI | `http://localhost:$AIRFLOW_WEB_PORT` (default 8080) | `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` |
| Warehouse Postgres | `localhost:$CHEMBL_DWH_HOST_PORT` (default 5433) | `CHEMBL_DWH_USER` / `CHEMBL_DWH_PASSWORD` |

The warehouse port is published so a SQL client can reach the data mart directly. Airflow's
metadata database is deliberately not published.

### The first run

DAGs arrive paused. Unpause all four, then start `chembl_ingest` by hand. It is the only one
that needs starting, since everything after it is chained on datasets:

```bash
docker compose exec airflow-scheduler airflow dags unpause chembl_ingest
docker compose exec airflow-scheduler airflow dags trigger chembl_ingest
```

| Order | DAG | Starts on | Roughly |
| --- | --- | --- | --- |
| 1 | `chembl_ingest` | triggered by hand | 50 min on the dump route |
| 2 | `fingerprint_build` | the `silver.molecule` dataset | 5 min |
| 3 | `input_compounds` | its own daily schedule | under a minute |
| 4 | `similarity_mart` | both datasets above | 15 min |

`input_compounds` does not depend on the ChEMBL load, so trigger it once by hand rather than
waiting for its first daily run. `similarity_mart` then starts on its own as soon as the
fingerprints and the source set both exist. Nothing else needs triggering.

The mart is queryable on the published warehouse port as soon as that finishes:

```sql
SELECT * FROM gold.v_avg_similarity_per_source ORDER BY avg_tanimoto_score DESC LIMIT 5;
```

`docker compose down` stops everything. Add `-v` to drop both database volumes as well.

## Pipeline

Four DAGs. They are chained by Airflow datasets instead of by schedule, so downstream work
only starts once its input has actually been rebuilt.

| DAG | Trigger | Does |
| --- | --- | --- |
| `chembl_ingest` | `@monthly` | Loads ChEMBL into `bronze`, then conforms it into `silver.molecule` |
| `fingerprint_build` | `silver.molecule` dataset | Morgan fingerprints for every molecule, published to S3 |
| `input_compounds` | `@daily` | Lands the personal input files and selects the source molecule set |
| `similarity_mart` | both datasets above | Scores every source against ChEMBL and rebuilds the mart |

When `build_silver` finishes, `chembl_ingest` publishes the
`chembl://warehouse/silver.molecule` dataset. `fingerprint_build` listens for it and
carries no schedule of its own. `input_compounds` runs on its own timer because the input
files arrive independently of any ChEMBL release.

`similarity_mart` waits for both the fingerprints and the source set. Requiring both keeps
its inputs consistent with each other. The cost is that a change to the source set alone
does not start it, so that case is picked up either at the next fingerprint rebuild or by
triggering the DAG by hand.

Re-running any DAG is safe. `chembl_ingest` exits early if the release it finds is already
loaded. Fingerprint shards use deterministic object keys, so a retry replaces a shard
instead of adding one. The source set and the mart are both rebuilt from scratch on every
run.

## Repository layout

```
dags/                   the four DAG definitions plus the shared dataset declarations
src/chembl_sim/
    settings.py         typed readers for every environment variable
    logging_setup.py
    alerting.py         the Teams webhook, carrying no Airflow imports
    chembl/             REST client, record shapes, bronze loader, release dump
    chem/               fingerprints, Tanimoto, the similarity search
    inputs/             input CSV parsing and validation
    quality/            the data quality checks and the gate that runs them
    storage/            warehouse connections and S3 parquet transfer
    transform/          bronze to silver, the source set, the mart, the generated view
sql/                    schema DDL and the four static views, applied in filename order
tests/                  209 tests, none touching the network or a database
docker/Dockerfile       the Airflow image with RDKit and the project requirements
```

Airflow imports `dags/` alone. Everything under `src/chembl_sim` is an ordinary library on
`PYTHONPATH`, which is what lets the whole test suite run without Airflow present.

## Warehouse architecture

Three medallion layers hold data. A fourth schema keeps pipeline state separate, so
bookkeeping rows can never be confused with ChEMBL rows.

| Schema | Holds | Rebuilt from |
| --- | --- | --- |
| `bronze` | ChEMBL exactly as delivered, plus the raw input files | the source |
| `silver` | typed, deduplicated, conformed molecules and the source set | `bronze` |
| `gold` | the star schema data mart and its views | `silver` |
| `meta` | ingest watermarks plus the quality check history | not data |

**Bronze stores the source as it arrives and nothing more.** ChEMBL's API hands back
decimal properties as JSON *strings* (`"alogp": "1.31"`, `"max_phase": "4.0"`) while
sending integers as real numbers. Bronze copies that split exactly: `text` for whatever
arrives as text, `integer` for whatever arrives as an integer. Nothing but primary keys is
`NOT NULL`. A landing layer that refuses a row over an unexpected value defeats its own
purpose.

**Silver is responsible for typing. That conversion has to be total.** Since bronze
holds text, a plain `::numeric` cast would only push the failure one layer along. Casting
goes through `silver.safe_numeric` instead, which yields `NULL` for anything that is not a
plain number rather than raising. Silver also enforces the rule the rest of the pipeline
depends on: without a compound structure a molecule can be neither fingerprinted nor
scored, so `silver.molecule` covers only the 2,897,819 molecules that have one.

**Gold is the data mart.** It holds `dim_molecule` and `fact_molecule_similarity` in a star
schema, limited to molecules the fact table refers to, together with the delivery views.

Row counts for every table are listed under [Results](#results). Once loaded the warehouse
takes up roughly 6.4 GB on disk.

DDL is applied with `CREATE TABLE IF NOT EXISTS`. Missing objects get created, existing
ones are never altered. Changing a column therefore means recreating the warehouse volume
and reloading. That trade is deliberate: everything here can be rebuilt from its sources,
which is cheaper than carrying a migration tool for a pipeline that regenerates itself.

## Ingesting ChEMBL

Two ingest routes exist, chosen with the `ingest_path` DAG parameter. Both write the same
bronze tables with the same column types, so nothing further down the pipeline can tell
which one ran.

- **`dump`** (the default) fetches the official release archive, restores the four required
  tables with `pg_restore` and drains each of them into bronze.
- **`api`** pages the ChEMBL REST API behind a rate limiter, with exponential backoff on
  retries and a resumable offset watermark.

### Why the dump is the default

The REST API was timed several times across one working day. The numbers never settled:

| Measurement | Result |
| --- | --- |
| Page latency, morning | 1.8 to 3.5 s |
| Page latency, afternoon | 21 to 52 s, median 32.9 s |
| 6 concurrent workers | 4.46 s/page, projecting **9.9 h** |
| 12 concurrent workers | **5 of 12 requests returned HTTP 500** |
| 24 concurrent workers | 0.55 s/page, projecting 1.3 h |
| Later, healthy | 1.26 s/page (molecules), 0.68 s/page (lookup), projecting **~2 h** |
| At one point | **total outage, HTTP 500 on every endpoint for more than 15 minutes** |

Two conclusions come out of that. There is no honest single figure for the API route, so
the client is built to absorb the variance rather than assume a number: retries, a
resumable watermark and a worker count that can be tuned. And the server really does fail
under concurrency. The 500s at twelve workers were observed, not simulated.

Bytes turn out to matter more than latency. **The API sends no compression at all.** An
explicit `Accept-Encoding: identity` request and a `gzip` request returned byte-identical
sizes, with no `content-encoding` header on the response. The two routes therefore move
very different volumes for the same result:

| Path | Transferred | Observed wall time |
| --- | --- | --- |
| REST API, both endpoints | about 12.6 GB of uncompressed JSON | about 2h when healthy, unbounded when not |
| Release dump | **2.09 GB compressed** | about 12min download, about 35min restore and drain |

The course Q&A confirmed that no particular fetch mechanism is required, only that it must
not be a manual step. Every part of the dump route is an Airflow task: discovery, the
resumable download, SHA-256 verification, the selective restore and the drain. It satisfies
that condition, moves roughly six times fewer bytes and keeps working when the API does
not.

The API route stays in the codebase, covered by tests and runnable with `ingest_path=api`.
The rate limiter suggested in the course Q&A lives in `chembl_sim/chembl/client.py`.

### Restoring without running out of disk

Restoring all four tables at once would keep two copies of each, one in the staging schema
and one in bronze, with a peak close to 20 GB. Each table is instead restored, drained and
dropped in turn, so only one staging table exists at any moment and the peak stays near
11 GB. `pg_restore` is given `--section=pre-data --section=data`, which skips indexes and
constraints. Every table is read exactly once by a hash join, so building a unique index
across 2.9 million `standard_inchi` values would be wasted effort.

The warehouse runs PostgreSQL 17 so that it matches the `pg_restore` client bundled in the
Airflow image. A version 17 client emits `SET transaction_timeout`, a parameter a version
16 server rejects, which aborts the restore on its very first statement. Airflow's metadata
database stays on 16, the newest release Airflow 2.10 supports.

## Fingerprints

Morgan fingerprints at radius 2 and 2048 bits, as the task requires, generated by RDKit's
`rdFingerprintGenerator`. They are kept as **packed bytes**: `np.packbits` compresses the
2048-bit vector into 256 bytes instead of a 2048-character bit string.

Throughput measures at 10,641 molecules per second on one thread, putting the whole library
at roughly 4.5 minutes. The work is still divided into shards, though not for speed. Each
shard is a separate Airflow mapped task, so it can be retried on its own, its memory use is
bounded and it produces exactly one parquet object.

Shard boundaries come from one `row_number()` pass over the table. Each shard then reads a
contiguous `chembl_id` range straight off the primary key index. Deep `OFFSET` paging was
ruled out because later shards would have to scan past millions of rows just to reach their
first one.

A full run produces:

| Measure | Value |
| --- | --- |
| Objects written | 12 parquet files |
| Total size | **165 MB** |
| Fingerprints | 2,897,802 |
| Duplicate identifiers | **0** |
| Molecules RDKit could not parse | 17 |

The zero matters most. Shards are contiguous key ranges, so any off-by-one would surface
either as duplicated identifiers or as a gap in coverage. Neither appeared.

165 MB is well under the 740 MB the arithmetic suggests (2.9M multiplied by 256 B). Morgan
fingerprints are sparse, with roughly 43 of the 2048 bits set, so most of each packed
vector is zeros and zstd compresses it around fourfold.

## The source molecule set

The task calls for the ten nearest neighbours of each of 100 chosen molecules. The personal
input files sit at whatever S3 prefix `CHEMBL_INPUT_PREFIX` names, here
`input/luka-javakhishvili/`. They hold 58 rows spread over five files and they name
compounds by **name**, with no ChEMBL identifier and no structure.

Those five files do not agree on their columns either. `batch_004.csv` omits `logp` and
`batch_005.csv` writes `IC50_nM` and `collection_date` where the rest use `ic50_nm` and
`assay_date`. Headers are lowercased and mapped onto the union of every column seen and
anything a file omits lands as `NULL`.

Compound names are matched against ChEMBL preferred names. Out of 57 named rows, **56 match
exactly one molecule** and not one matches more than one. The set is then filled up to 100
using molecules sorted by `md5(chembl_id)`. That ordering is deterministic, gives the same
answer on any machine and scatters the choice across the whole library instead of bunching
it on the lowest ChEMBL identifiers the way sorting by identifier would. Running the
selection again reproduces exactly the same 100.

| Origin | Count |
| --- | --- |
| `input_file` | 56 |
| `top_up` | 44 |
| **Total** | **100** |

Each row carries its `origin`, so the makeup of the set can be checked with one query.

Two input rows do not make it in. Both are written to `silver.source_molecule_rejected`
with a reason instead of being quietly discarded:

| File | Compound | Reason |
| --- | --- | --- |
| `batch_004.csv` | *(blank)* | compound name is blank |
| `batch_001.csv` | Paracetamol | name matches no ChEMBL preferred name |

Paracetamol is a naming difference rather than bad data. ChEMBL files it under
`ACETAMINOPHEN`. Matching it would need the `molecule_synonyms` table, which falls outside
the four tables this project ingests, so it stays a recorded rejection.

## Similarity search

Tanimoto is calculated straight on the packed bytes. The intersection is
`popcount(a AND b)`. The union is `popcount(a) + popcount(b) - intersection`. Library
popcounts are worked out once at the start of a run and reused for every source afterwards.

Airflow 2.10 pins numpy at 1.26, which is older than `np.bitwise_count`, so a 256-entry
lookup table stands in for it. The code picks the native operation whenever the installed
numpy provides one, so lifting that pin needs no change here.

Scoring runs in blocks of 250,000 molecules so the intermediate `AND` never has to exist
for the entire library at once. Measured over the full set:

| Measure | Value |
| --- | --- |
| Library size | 2,897,802 fingerprints, 742 MB packed |
| Load from S3 | 192 s |
| Library popcounts | 1.7 s, once per run |
| Scoring, one source against all | **1.70 s** |
| 100 source molecules | **2.8 min** |
| Full task, including the S3 upload | **14.7 min to 24.5 min** |
| Peak resident memory | 2.1 GB |

The full task is quoted as a range because writing the 100 score objects to S3 dominates it.
Loading the library plus scoring the 100 sources accounts for only about six minutes, so the
remainder is 711 MB leaving the machine at whatever throughput the link gives that day. The
two figures are the fastest and slowest runs measured. Nothing else in the table varies
meaningfully between runs.

Loading the library costs two orders of magnitude more than scoring a single source
against it. The entire search therefore runs inside one task rather than being spread over
mapped tasks, since splitting it would make every worker pay that 192 second load again.

Ties deserve more attention than they first appear to. Tanimoto over 2048-bit fingerprints
produces ratios with small denominators, which makes equal scores ordinary rather than
unusual. Selection takes every molecule scoring at or above the tenth value, sorts by score
descending and then by identifier ascending so the outcome is repeatable and marks
`has_duplicates_of_last_largest_score` on whichever selected rows sit at that boundary
score, but only when more molecules share it than there is room for.

Over the 100 sources the search returns 1,000 rows, 40 of which carry that flag, spread
across 24 different sources. Boundary ties are the normal case here, which is the reason
the flag exists at all.

Every source molecule gets its complete score table written to S3 as a standalone parquet
object holding `source_chembl_id`, `target_chembl_id` and `tanimoto_score`. Repeating the
source on every row is free, because it dictionary-encodes down to a single entry, so the
object is no larger than a stripped-down one would be and can still be read on its own.
Under zstd each comes to about 7.1 MB, or 0.71 GB for all 100. A benchmark on uniformly
random floats predicted 15.5 MB apiece, but real scores cluster near zero and are heavily
quantised, so they compress more than twice as well.

Scores are calculated in double precision and archived as float32. The two were compared
directly over twenty sources and 58 million comparisons: identical top-10 ordering,
identical tie flags, identical counts of distinct values. Float32 loses nothing the search
can act on. What it does do is print exact ratios badly, turning 7/10 into 0.69999999, so
the values that reach the mart are computed as doubles while the S3 archive stays float32
and half the size. Rounding was considered and rejected. At six decimal places genuinely
different scores would collide, because two Tanimoto values can sit as little as 1/2048
squared apart.

Some pairs score exactly 1.0 and those results are correct. Morgan fingerprints capture
connectivity but not stereochemistry, so at radius 2 both enantiomers of pregabalin,
`CC(C)C[C@H](CN)CC(=O)O` and `CC(C)C[C@@H](CN)CC(=O)O`, along with the version that leaves
stereochemistry unspecified, all reduce to the same bit vector. Salt and parent forms
collide in the same way. A source molecule is always removed from its own results, so a
perfect score always points at a genuinely different ChEMBL entry the fingerprint cannot
tell apart.

## Data mart views

Five views sit on the star schema. Four are ordinary SQL files applied alongside the rest
of the DDL. The fifth has to be generated at run time, because its columns are data rather
than schema.

| View | Task | Rows | Answers |
| --- | --- | --- | --- |
| `gold.v_avg_similarity_per_source` | 7a | 100 | mean similarity of each source molecule |
| `gold.v_avg_alogp_deviation` | 7b | 100 | mean absolute alogp gap between a source and its matches |
| `gold.v_similarity_pivot` | 8a | 100 | ten source molecules as columns, targets as rows |
| `gold.v_similarity_neighbours` | 8b | 1000 | each match with the next one down plus the source's second best |
| `gold.v_avg_similarity_rollup` | 8c | 214 | averages at four levels of grouping |

### Averages per source molecule

```
 source_chembl_id | avg_tanimoto_score
------------------+--------------------
 CHEMBL1168       |         0.97110389
 CHEMBL1200699    |         0.96475421
 CHEMBL809        |         0.96203007
```

The alogp view reports a mean *absolute* deviation, which is the option the course Q&A
recommended. Pairs where either molecule has no alogp contribute nothing, since `avg`
skips nulls. A deviation of zero is real rather than a bug: several sources have ten
matches that all share their alogp, which fits the stereoisomer collisions described above.

### The pivot

Ten source molecules become ten real columns. The lowest ten identifiers are used so the
same columns come back on every rebuild. Rendering happens in `chembl_sim/transform/views.py`
with `psycopg2.sql.Identifier` doing the quoting, since the column names come from data.
The view is dropped before it is recreated, because `CREATE OR REPLACE VIEW` cannot rename
a view's columns.

```
 target_chembl_id | CHEMBL1059 | CHEMBL1064 | CHEMBL1082
------------------+------------+------------+------------
 CHEMBL1314217    |            | 1.00000000 |
 CHEMBL1414674    |            | 1.00000000 |
 CHEMBL167003     | 1.00000000 |            |
 CHEMBL190074     | 0.66666667 |            |
```

Only three of the eleven columns are shown here. Most cells are empty by construction: the
fact table holds ten matches per source, so a target usually belongs to one source column
only. A JSON map would have been denser but harder for a downstream tool to consume, so
real columns were kept.

### Neighbours

```
 source_chembl_id | target_chembl_id | tanimoto_score | next_most_similar_target | second_most_similar_target
------------------+------------------+----------------+--------------------------+----------------------------
 CHEMBL25         | CHEMBL3833404    |     0.88888889 | CHEMBL350343             | CHEMBL350343
 CHEMBL25         | CHEMBL350343     |     0.85714286 | CHEMBL5282669            | CHEMBL350343
 CHEMBL25         | CHEMBL5282669    |     0.74074074 | CHEMBL4515737            | CHEMBL350343
```

`next_most_similar_target` is a `lead` over the source's matches ordered by score. The
`second_most_similar_target` column stays constant down the whole partition, because it
names the source's own second best match rather than anything relative to the current row.
That reading was the one confirmed in the course Q&A. It needs an explicit
`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` frame: the default frame stops
at the current row, which would leave the first row null. Ordering breaks ties on the
identifier so the view returns the same answer every time.

### The rollup

Four grouping sets in one pass with no `UNION` anywhere, which the task requires:

```
 source_molecule | aromatic_rings | heavy_atoms |   avg
-----------------+----------------+-------------+----------
 CHEMBL25        | TOTAL          | TOTAL       | 0.720156
 TOTAL           | 1              | 13          | 0.720156
 TOTAL           | TOTAL          | 13          | 0.760855
 TOTAL           | TOTAL          | TOTAL       | 0.760359
```

The 214 rows break down as 100 by source molecule, 72 by aromatic rings together with
heavy atoms, 41 by heavy atoms alone, plus the single grand total.

`grouping()` is what makes `TOTAL` correct. It reports whether a null came from the
rollup collapsing a column or was already in the data, so only the first kind is relabelled.
One source molecule, `CHEMBL4860540`, is typed `Unknown` in ChEMBL with no aromatic rings,
heavy atoms or alogp recorded. Its rows therefore show a genuinely empty cell rather than
`TOTAL`:

```
 source_molecule | aromatic_rings | heavy_atoms |   avg
-----------------+----------------+-------------+----------
 TOTAL           |                |             | 0.717386
 TOTAL           | TOTAL          |             | 0.717386
```

Using `coalesce` instead would have relabelled those as `TOTAL` and folded a molecule with
unknown properties into the grand total.

## Results

Every figure below comes from one complete run on ChEMBL 37, ingest through to the mart.

| Layer | Output |
| --- | --- |
| `bronze.chembl_id_lookup` | 5,478,952 rows |
| `bronze.molecule_dictionary` | 2,921,148 rows |
| `bronze.compound_properties` | 2,901,464 rows |
| `bronze.compound_structures` | 2,897,819 rows |
| `silver.molecule` | 2,897,819 rows |
| `silver.source_molecule` | 100 rows |
| `gold.dim_molecule` | 1,099 rows |
| `gold.fact_molecule_similarity` | 1,000 rows |
| S3 fingerprints | 12 objects, 165 MB |
| S3 similarity scores | 100 objects, 711 MB |

Each source molecule is scored against all 2,897,802 fingerprinted molecules, so every
similarity object on S3 holds a complete table of 2,897,802 rows. Only the ten best per
source reach the mart, which is where the 1,000 fact rows come from.

### Top ten for one source molecule

`CHEMBL1431` is metformin, one of the 56 compounds named in the input files.

```
 target_chembl_id | tanimoto_score |         pref_name
------------------+----------------+---------------------------
 CHEMBL3094198    |     1.00000000 |
 CHEMBL1703       |     0.94444444 | METFORMIN HYDROCHLORIDE
 CHEMBL1972482    |     0.63636364 |
 CHEMBL2348412    |     0.56666667 |
 CHEMBL1998326    |     0.52173913 |
 CHEMBL4297654    |     0.50000000 | BIGUANIDE
 CHEMBL2348413    |     0.47222222 | METFORMIN PREGABALIN SALT
 CHEMBL4791868    |     0.46153846 |
 CHEMBL2348410    |     0.45945946 | METFORMIN GABAPENTIN SALT
 CHEMBL1162547    |     0.43478261 | ACETYLGUANIDINIUM
```

The ordering is chemically sensible, which is the check worth making. A structural duplicate
scores 1.0, the hydrochloride follows it, two of metformin's co-crystal salts sit in the
middle of the list, then `BIGUANIDE`, the scaffold metformin is built on, lands at exactly
0.5. `ACETYLGUANIDINIUM` closes the list at 0.43 carrying only the guanidine fragment. Blank
names are ChEMBL entries with no preferred name recorded rather than missing data.

### The tie flag in practice

`CHEMBL41` is fluoxetine. Its list ends on a tie, which is what
`has_duplicates_of_last_largest_score` exists to report.

```
 target_chembl_id | tanimoto_score | flagged |        pref_name
------------------+----------------+---------+--------------------------
 CHEMBL1169388    |     1.00000000 | f       |
 CHEMBL153036     |     1.00000000 | f       | R-FLUOXETINE
 CHEMBL1201082    |     0.97297297 | f       | FLUOXETINE HYDROCHLORIDE
 CHEMBL1256757    |     0.97297297 | f       |
 CHEMBL1257031    |     0.97297297 | f       |
 CHEMBL4865485    |     0.72972973 | f       |
 CHEMBL4878226    |     0.72500000 | f       |
 CHEMBL1191185    |     0.68292683 | t       |
 CHEMBL1196148    |     0.68292683 | t       |
 CHEMBL1494       |     0.68292683 | t       | NORFLUOXETINE
```

Reading the full score table back from S3 accounts for the flag exactly. Seven molecules
beat 0.68292683, leaving three places. Eight molecules hold that score. Three fit, five are
cut, so every surviving row at that score is flagged to record that the boundary was
arbitrary.

R-fluoxetine scoring 1.0 is the stereochemistry case described earlier: Morgan fingerprints
at radius 2 carry connectivity but not chirality, so an enantiomer is indistinguishable from
its partner. `NORFLUOXETINE`, the active metabolite, arriving right at the cut is a
believable neighbour rather than an artefact.

### Score distribution

| Measure | Value |
| --- | --- |
| Lowest score kept | 0.4348 |
| Mean over the 1,000 rows | 0.7604 |
| Highest score | 1.0000 |
| Rows scoring exactly 1.0 | 75 |
| Flagged rows | 40, across 24 of the 100 sources |

Boundary ties touch roughly a quarter of the source molecules, so the flag reports a routine
outcome rather than a rare edge case.

### Where the results live

```
final_task/javakhishvili_luka/
    fingerprints/part-00000.parquet  ..  part-00011.parquet    12 objects, 165 MB
    similarity_scores/CHEMBL41.parquet  ..  one per source     100 objects, 711 MB
```

### Observed run times

Measured on the dataset triggered run that produced the numbers above.

| Stage | Wall clock |
| --- | --- |
| `fingerprint_build` | 4.9 min, 12 mapped shards, 8 running at once |
| `similarity_mart` | 14.9 min |
| `compute_similarity` alone | 14.7 min |
| `run_quality_checks` | 0.3 s |

## Data quality gate

Every layer that writes data is checked before the run continues. The checks live in
`src/chembl_sim/quality/checks.py` as one SQL query each. The gate runs as a task inside the
DAG that produced the data.

Checks that restate a table constraint are deliberately absent. The score range, the primary
keys and the fact's two foreign keys are already enforced by the database, so a check on them
can never fail. What is checked is everything the schema cannot express.

| Check | Layer | Severity |
|---|---|---|
| `silver_molecule_is_populated` | silver | block |
| `silver_conversion_keeps_every_structure` | silver | block |
| `silver_smiles_is_never_blank` | silver | block |
| `silver_numeric_cast_drops_nothing` | silver | block |
| `every_input_row_is_accounted_for` | source | block |
| `source_set_reaches_the_configured_size` | source | warn |
| `fact_excludes_self_matches` | gold | block |
| `fact_holds_the_top_n_for_every_source` | gold | block |
| `fact_sources_come_from_the_source_set` | gold | block |
| `dim_holds_no_unreferenced_molecules` | gold | block |
| `tie_flag_sits_only_on_the_boundary_score` | gold | block |
| `tie_flag_agrees_across_the_boundary_score` | gold | block |
| `dim_alogp_coverage_stays_high` | gold | warn |
| `release_gap_columns_are_still_empty` | gold | warn |

A blocking failure raises, which fails the task, stops the DAG before the next layer is built
and sends the same Teams notification every other failure sends. A warning is recorded then
logged, leaving the run alone.

The exception raised is `AirflowFailException`, so the task fails on its first attempt rather
than retrying. Re-running a check against unchanged data cannot produce a different answer,
so the configured retry would only delay the alert. Any other error, a dropped connection for
instance, still retries normally.

Three of these are worth singling out. `silver_conversion_keeps_every_structure` is the
executable form of the claim that the bronze to silver conversion has to be total.
`silver_numeric_cast_drops_nothing` makes `safe_numeric` accountable, since turning an
unparseable value into null is silent by design. `release_gap_columns_are_still_empty` is
inverted on purpose: it fires when `cx_logp` or `molecular_species` start arriving, so a
later release that fills the documented gap announces itself instead of going unnoticed.

The gate also owns the outlet datasets. `SILVER_MOLECULE` and `SOURCE_MOLECULE` are published
by the check task rather than by the task that built the layer, so a layer that failed its
checks cannot trigger the DAG downstream of it.

Every result is kept, one row per check per run:

    SELECT * FROM meta.v_quality_latest ORDER BY layer, check_name;

Keeping the history rather than only the current state means a regression can be traced back
to the run that introduced it.

This is an addition beyond the required scope. Nothing in the specification asks for it and
the pipeline meets every requirement without it.

## Failure notifications

Every task in all four DAGs carries an `on_failure_callback` that posts to a Microsoft
Teams webhook. The final task of `similarity_mart` also posts on success, so a completed
rebuild announces itself without every task adding noise.

The webhook URL comes from `CHEMBL_TEAMS_WEBHOOK_URL`. Leaving it empty disables alerting
completely: the callback logs a warning then returns. A clone of this repository has to run
without a webhook, so a missing URL can never be an error.

More importantly, **alerting never raises**. Network errors, timeouts and non-success
status codes are all caught, logged then swallowed. A callback that threw would replace the
real pipeline failure with a notification failure, which is precisely the information you
lose at the worst moment. The callback returns a boolean instead, so the outcome is still
visible in the task log.

`chembl_sim/alerting.py` imports nothing from Airflow. A failure callback receives a plain
dictionary, so the module reads it with `.get` and `getattr` rather than depending on the
scheduler. That keeps it unit testable with a stand-in object.

The payload carries two shapes at once: an adaptive card in `attachments` plus the same
content as plain text in `text`. Power Automate flows differ in which field they read, so
supplying both avoids guessing at someone else's flow definition. The cohort webhook
consumes the card.

One environment note. The Power Automate host resolves through a six hop CNAME chain that
some local resolvers reject, `systemd-resolved` on this machine among them, which fails
with `Received invalid reply` while a public resolver answers normally. The Airflow
services therefore pin `1.1.1.1` alongside `8.8.8.8` in `docker-compose.yml` so
notifications do not depend on whichever resolver the host happens to run.

## Tests and CI

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt \
  --constraint https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.11.txt

ruff check .
ruff format --check .
pytest --cov
```

209 tests, 92 percent statement coverage, with a floor of 88 configured in
`pyproject.toml`. Nothing in the suite touches the network, S3 or a database. The API
client is driven with `requests_mock`, warehouse code runs against a recording cursor
fixture in `conftest.py`, so the whole suite finishes in about fifteen seconds.

Coverage sits where the branching is. Every module carrying real decisions is at 93
percent or above: watermark resets on a release change, the skip when a release is already
loaded, the force override, tie flagging at the top-N boundary, the total numeric cast.
What is left uncovered is mostly thin wrappers over boto3 plus the orchestration inside
`ingest_from_dump`, which a full pipeline run exercises end to end.

Two checks run on every push, defined in `.github/workflows/ci.yml`.

The first installs against the same Airflow constraint file the container uses, on the same
Python 3.11, then runs ruff and pytest. Resolving identical versions to the image is the
point: a test suite that passes against different dependencies than production is not
evidence of much.

The second renders `docker-compose.yml` using `.env.example` alone, which catches the
example environment drifting behind the compose file. That check needs care, because
`docker compose config` exits zero when a variable is undefined and only prints a warning.
The job therefore greps its own stderr and fails on `variable is not set`. Without that,
the step would pass while quietly substituting blank strings.

## Known data gaps

**`cx_logp` and `molecular_species` are always `NULL`.** The dimension table is required to
carry both and neither exists in ChEMBL 37. This is not a limitation of the API. The
official schema documentation for the release gives `COMPOUND_PROPERTIES` 15 columns:
`molregno`, plus `mw_freebase`, `alogp`, `hba`, `hbd`, `psa`, `rtb`, `ro3_pass`,
`num_ro5_violations`, `full_mwt`, `aromatic_rings`, `heavy_atoms`, `qed_weighted`,
`full_molformula` and `np_likeness_score`. Neither field appears anywhere in that list.
Both columns are kept in `silver.molecule` and `gold.dim_molecule` so the schema still
matches the specification and so a backfill from an earlier release would have somewhere
to go.

**17 molecules end up with no fingerprint.** RDKit cannot parse their SMILES. They are
counted and reported per shard rather than being allowed to fail the run.

**Two columns depend on which ingest route ran.** The API exposes `resource_url` on
`chembl_id_lookup` and has no `entity_id`. The release dump is the other way round. Bronze
carries both columns and each route fills only the one its source actually provides.
Neither route invents the other's value. `helm_notation` behaves the same way and is filled
only by the API route, since the ChEMBL schema keeps it on `biotherapeutics` rather than on
`molecule_dictionary`.
