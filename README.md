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

Full IMG-RAE v5 coverage: `/rae?sport=football` and `/rae?sport=tennis`; machine-readable requirements at `/data/rae/requirements`. Source observations, model computations, policy and private account inputs are distinguished. Context-only completeness never establishes overall engine readiness. Eredivisie (`ned.1`) is included in the football scoreboard collector. Challenger/ITF coverage remains a declared gap.

Current-match evidence: `/live/{event_id}` and `/data/live/{event_id}?as_of=...`. Separate 60-second polling lanes for both sports retain minute-level ESPN retrieval receipts for mapped fixtures. Explicit venue, weather, surface, indoor, situation and competitor statistics are retained when supplied. Missing values are unknown. Retrieval freshness is separate from unknown provider latency. Live snapshots never backdate into pre-match context. Render free-service sleep limits continuity.


## Unified delivery and remaining dependencies
- `/engine/{event_id}` and `/data/engine/{event_id}?as_of=...`: combined pre-match context, separate current state, source receipts, weather receipts, linked markets, book depth/slippage, complete bookmaker outcome sets and fail-closed blockers. Canonical bookmaker labels are home/draw/away or player1/player2. Multiplicative de-vig is computed only for complete same-book markets; sharpness and settlement remain separately unverified.
- `/operations` and `/data/operations`: latest collector runs, stalled-run flags and access requirements. A zero-record pass is not full competition coverage.
- ESPN summaries: bounded to eight uncached fixtures per sport/pass, six-hour receipt reuse, 15-minute cadence. Roster/injury/boxscore payloads are retained as source evidence; their availability does not imply complete or medically verified coverage.
- Weather: only explicit venue coordinates from a retained source receipt; no guessed city. Open-Meteo current model estimates/forecasts are labelled as modelled, not weather-station observations. Attribution CC BY 4.0. Indoor conditions remain unknown without evidence.
- Executable-book capture: both sports have independent lanes; all tokens of a market are requested together. Capture phases reference the **scheduled** start; actual-kickoff, closing-line and settlement verification remain pending. Historical price points do not reconstruct historical depth.
- Optional bookmaker access: configure `ODDS_API_KEY`, comma-separated `ODDS_API_SPORT_KEYS` using the provider sports catalogue, optional `ODDS_API_REGIONS` (default eu). Default daily request cap is six per sport, persisted before requests; configure `ODDS_API_DAILY_REQUESTS_PER_SPORT` only within the subscribed quota. A 15-minute polling loop does not imply 15-minute quote coverage after the cap is reached.
- Existing optional sports feeds require `API_TENNIS_KEY` / `SPORTMONKS_API_TOKEN`. No subscriptions or paid infrastructure upgrades are purchased automatically.
- Private account integration, independent fixture-source lineage, benchmark sharpness, complete lower-tier tennis schedules, actual-start/closing capture and settlement/fill reconciliation are **not complete**. They must not be reported as ready based on newly available receipts.
- Current database usage exceeds the free allowance. Unique records are retained; capacity needs an approved infrastructure choice. Continuous collection also requires hosting that stays awake.
