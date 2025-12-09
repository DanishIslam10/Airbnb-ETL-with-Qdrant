**Airbnb ETL with Qdrant**

This repository contains a small ETL pipeline that extracts clean and transform Airbnb listing dataset and stores it in a Postgres database, then builds textual representations, creates embeddings using Google's Gemini embedding model, and uploads vectors + payloads into a Qdrant collection for semantic search.

**Repository Structure**
- `save_embeddings.py`: Main ETL script — reads cleaned listings from Postgres, constructs `embedding_text`, creates embeddings, and upserts points to Qdrant.
- `semantic_search.py`: Example search client that embeds a query and queries Qdrant for top-k similar listings.
- `cleaning.py`: (project-specific) data cleaning utilities used to prepare the `airbnb_listings_clean` table.
- `.env`: environment variables used locally (DO NOT commit secrets).
- `.progress.json`: local state file used by `save_embeddings.py` to track `last_seen_id`.

**Requirements**
Install dependencies into a virtual environment. Example minimal packages (the exact package names may differ depending on provider SDK versions):

```
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Suggested `requirements.txt` (create this file if you want):

```
pandas
sqlalchemy
psycopg2-binary
python-dotenv
google-genai
qdrant-client

# optional / dev
# dotenv, typing, etc
```

**Environment Variables**
Create a `.env` file at the project root (do not commit it). `save_embeddings.py` expects the following variables:

- `USER` : Postgres username (e.g., `postgres`)
- `PASSWORD` : Postgres password
- `HOST` : Postgres host (e.g., `localhost`)
- `PORT` : Postgres port (e.g., `5432`)
- `DBNAME` : Postgres database name (e.g., `airbnb_db`)
- `GEMINI_API_KEY` : API key for Google Gemini / embedding provider
- `QDRANT_API_KEY` : Qdrant API key
- `QDRANT_CLUSTER_ENDPOINT` : Qdrant endpoint URL (e.g., `https://...qdrant.io`)

IMPORTANT: Never commit actual secret values to source control. Replace any keys in `.env` with your own before running.

**How it works (brief)**
- `save_embeddings.py` reads rows from the `airbnb_listings_clean` table in Postgres.
- It builds a human-readable `embedding_text` per row and batches calls to Gemini to create embeddings.
- Embeddings + the full row payload are upserted into a Qdrant collection (`COLLECTION_NAME = "airbnb_embeddings"`). If the collection does not exist, it is created automatically.
- The script keeps track of the last processed `listing_id` in `.progress.json` so repeated runs continue from where they left off.

**Running the ETL (examples)**
1. Make sure Postgres is accessible and the `airbnb_listings_clean` table exists and contains cleaned data.
2. Populate `.env` with the environment variables listed above.
3. Run the embedding upload:

```
python save_embeddings.py
```

Notes:
- `save_embeddings.py` will read a batch (default `DEFAULT_LIMIT = 100`) starting after the `listing_id` saved in `.progress.json` (if present).
- On success it writes a new `last_seen_id` into `.progress.json`.

**Semantic search (examples)**
Run the search example to perform a query:

```
python semantic_search.py
```

Or import the `semantic_search.semantic_search` function into an interactive shell and call it with your own queries.

**Qdrant collection behavior**
- The first time you run the pipeline, the collection `airbnb_embeddings` will be created with vector size equal to the embedding dimension returned by the embedding provider.
- The `payload` for each point is the full row dictionary (all selected columns) so you can display listing metadata with search results.

**Security & Best Practices**
- Add `.env` and `.progress.json` to `.gitignore` to avoid committing secrets and state:

```
.env
.progress.json
__pycache__/
*.pyc
```

- Rotate API keys and avoid storing long-lived secrets in repo.

**Troubleshooting**
- Postgres connection errors: verify `DATABASE_URL` in `save_embeddings.py` and that Postgres is running and reachable.
- `psycopg2` installation errors on Windows: install `psycopg2-binary` or use a prebuilt wheel.
- Gemini / embedding errors: confirm `GEMINI_API_KEY` is valid and the embedding model (`text-embedding-004`) is available for your account.
- Qdrant connection errors: verify `QDRANT_CLUSTER_ENDPOINT` and `QDRANT_API_KEY`. Ensure network connectivity.

**Contributing**
- Improvements welcome: better batching, retry logic, logging, unit tests, or support for other embedding providers.

**License**
This repository does not include a license file. Add a `LICENSE` if you plan to make it public (MIT recommended for permissive use).

---
If you'd like, I can also:
- create a `requirements.txt` or `environment.yml` for you
- add a `.gitignore` file entry
- add small unit tests for the embedding text builder

Feel free to tell me which you'd like next.
