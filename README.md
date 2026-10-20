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


## Rolling pre-match evidence (IMG-RAE / LEDGER)
- `/context?sport=football` and `/context?sport=tennis`: inspect recent input packages.
- `/context/match/{event_uuid}`: profiles, workload coverage, and missing inputs.
- `/data/context/{event_uuid}?as_of=ISO_TIMESTAMP`: engine-readable 7/14-day package.
- `/context/providers`: credential presence and source limitations; never returns keys.
- `POST /context/evidence`: authenticated public-source receipts; requires `X-SDE-Admin-Token` matching `SDE_ADMIN_TOKEN`.

Apply `sql/sde_context.sql` before deploying. The evidence table is append-only to the service role; anonymous and authenticated clients have no direct access. Existing history is preserved.

Public collection extracts recent ESPN score observations, athlete profiles and football rosters where accessible; TheSportsDB supplies an exact-name tennis profile fallback. Rolling evidence refreshes every five minutes while the instance is awake. Profile attempts are limited to eight entities per sport per pass, with a six-hour retry interval. Selected competitions do not establish complete player schedules. Measurements are converted only from explicit source units.

Optional credentialed adapters run every 15 minutes while awake:
- `API_TENNIS_KEY`: recent singles fixtures, results, and supplied match statistics.
- `SPORTMONKS_API_TOKEN`: football fixtures, rosters/profiles, supplied player minutes, and reported absences within subscribed coverage.

These adapters do not purchase access. Tracking speed/distance has no configured source. Missing minutes, missing injury information, and incomplete schedules remain unknown. No source response is treated as medical clearance. `engine_ready` remains false when required evidence is absent. Historical packages exclude evidence first retrieved after their decision cutoff, even when the underlying match was older. The default cutoff is the earlier of now and match start; performance windows never exceed 14 days. Stable profiles may be older, with observation and unknown source-update times visible. An ongoing old injury may be used only with a recent confirmation. Fatigue coefficients in the PRDs are not validated by this feed.
