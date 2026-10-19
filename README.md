# Sports Data Engine (SDE)

Cloud-first, sport-agnostic data layer for Kreopoly Lens, IMG-RAE, Project LEDGER and Poly-robin.

## v0.3
- FastAPI service and live progress dashboard
- Football and tennis provider adapters
- Sofascore, StatsBomb, TheSportsDB, Football-Data CSV and Polymarket adapters
- Background ingestion jobs and SSE progress stream
- Canonical event resolution and analysis packages
- Supabase-ready cloud archive

Runtime secrets are never committed. Configure them in the deployment environment.

## Run
```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Open `/dashboard` for live collection progress and `/health` for health status.
