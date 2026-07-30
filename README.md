# chembl-similarity-dwh

Finds the top-10 most similar ChEMBL molecules for each compound in a personal input set,
using Morgan fingerprints and Tanimoto similarity.

The pipeline ingests ChEMBL from its public REST API into a medallion data warehouse on
PostgreSQL, computes fingerprints and similarity scores, publishes the results to S3, and
serves a star-schema data mart. Everything is orchestrated with Apache Airflow and runs
from a single `docker compose up`.

Setup, usage and results are documented below as the project takes shape.

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
