# Sports Data Engine (SDE)

## Database recovery hold (2026-10-20)
Postgres logs reported `No space left on device` while writing WAL. The last readable database was 1586 MB, above the free allowance. `SDE_COLLECTION_PAUSED` defaults to true and centrally blocks application writes, including repair RPCs. Reads remain available when Postgres responds. The dashboard shows an explicit unavailable state when reads fail. No retained history is deleted.

Restore database capacity and verify read/write health before setting `SDE_COLLECTION_PAUSED=0`. Identity repair is separately disabled by default; set `SDE_ENABLE_IDENTITY_REPAIR=1` only after checking disk/WAL headroom. It archives exact duplicate payloads and performs bounded transactions. The ten-minute GitHub schedule has exhibited multi-hour delays and does not guarantee awake hosting. A database scheduler ping cannot be installed while PostgreSQL refuses connections.

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

Provider failures are isolated per match/competition; a partial failed scan does not erase successful receipts. Football draws are retained as draws. Latest eligible result corrections replace earlier observations in recent-form aggregation. Tennis enrichment uses the source tournament and competition IDs with the core competition resource; coverage is empirically monitored and not guaranteed.


## LEDGER capture and resolution audit
`/ledger/{event_id}` and `/data/ledger/{event_id}?as_of=...` expose public capture/settlement-source receipts. Both sports poll up to 12 linked markets per pass with a persistent round-robin cursor. Gamma market identity and the V2 resolution state must agree on the condition and a one-hot payout before an outcome is scorable. Closed flags, disputed/proposed states, missing data and fractional/refund payouts never imply a winning outcome. This is venue-reported resolution, not independent blockchain finality or account payment confirmation. Identical resolution states are not duplicated.

Captures are reconstructed from retained capture intervals and book receipts for **all** tokens, with a five-second synchronization limit. Early and near-scheduled-start samples are distinct from actual-start/closing-line evidence. Current schedules are not historical schedule receipts. Actual kickoff, actual finish, a 24-hour outcome SLA and CLV remain unverified when their required evidence is absent.

`POST /ledger/forecasts` and `GET /data/ledger/private/{event_id}` require `X-SDE-Admin-Token` matching `SDE_ADMIN_TOKEN`; absent configuration denies access. Forecasts contain a UUID forecast_id, event_id, market_id, model, model_version, inputs_as_of and a complete token-ID probability vector. They are archived immutably using server receipt time, require a linked open market and a future scheduled start, and reject a source already showing in-progress/completed status. Do not backfill historical forecasts as prospective records. Model inputs are attested, not independently validated. Public audit pages exclude forecasts and scores. No account balances, API keys or personal trading fills belong in this endpoint.

Private audit scores use the multiclass sum Brier convention and separately report binary scalar Brier. Scheduled-start-based scores are labelled provisional until actual-start eligibility is verified. Actual realized PnL is never inferred from a winning outcome or hypothetical book fill.

## Growth control and non-destructive maintenance
Closed Gamma price snapshots are recorded only when token/outcome mapping, prices or resolution flag change. Open-market quote observations retain their existing cadence. Catalogue metadata still refreshes. Price writes happen before the closed-state fingerprint is committed, so a failed write remains retryable. Historical observations are not deleted or rewritten.

`sql/sde_index_maintenance.sql` records the bounded maintenance applied on 2026-10-20: index rebuilds with a one-second lock-acquisition limit and fifteen-second statement limit. The largest price index shrank from 53 MB to 28 MB; two other price indexes were rebuilt. This reclaims index bloat, not the storage of retained trade/price history, and does not make a database of roughly 1.5 GB fit a 500 MB allowance. Paid capacity or a separately designed, verified archival/restore strategy is still required. The duplicate archive remains intact.

## Free-only runtime (default)
`SDE_FREE_MODE=1` is the default for this deployment. Both sports retain equal collection lanes. The collectors prioritize the recent 14-day window and the next three days; old fixture expansion and automatic StatsBomb/TennisVisuals archive expansion are paused. Existing records are preserved, and adapters/on-demand routes remain present. `SDE_ENABLE_ARCHIVE_BACKFILL`, `SDE_ENABLE_OLD_FIXTURES` and `SDE_ENABLE_DEEP_TRADES` are explicit opt-ins; none purchases a service or upgrades infrastructure.

Automatic recent price-history work is limited to six linked markets per sport/pass with a persisted round-robin cursor, a 14-day maximum query window and hourly samples. Minute-level historical data already stored is not resampled or deleted. Current match-state and executable-book collectors keep their existing cadence. Deep trade backfills are paused by default. Timeline recomputation is restricted to recent fixtures in free mode. The operations page exposes the effective runtime policy.

This reduces resource use; it does **not** shrink the retained 1.5 GB history below the free database allowance, grant licensed source access, or guarantee continuous uptime on sleeping free hosting. A free archival design must preserve and verify recoverability before any historical records can be moved out of the query database. No historical deletion or paid upgrade is part of free mode.



## Independent fresh evidence during database recovery
`/fresh?sport=football` and `/fresh?sport=tennis` provide current source evidence without reading or writing the failed database. `/data/fresh`, `/fresh/match/{event_id}` and `/data/fresh/match/{event_id}` expose the corresponding source receipts and strictly bounded 7/14-day observations. This is partial evidence collection, not a declaration that IMG-RAE, historical replay or LEDGER are complete.

While `SDE_COLLECTION_PAUSED=1`, legacy database collectors and their shutdown writes stay paused. Independent lanes poll the selected ESPN leagues every 120 seconds while the service is awake. Each pass refreshes today/tomorrow and one prior date; 14-day history builds gradually and is not treated as a verified complete schedule. Unknown profiles, availability, minutes, server state or market benchmarks are not fabricated. Source failures and retrieval age are visible.

The existing private archive connection is reused under `sde/fresh/v1/` without creating credentials, changing bucket visibility or upgrading plans. Gzip JSON snapshots preserve normalized source facts, receipts and explicit unknowns. Uploads require object read-back equality, SHA-256 checks and a verified immutable manifest. The bucket byte budget is checked before writes; snapshots have row and decompression limits. Startup can restore verified manifests from the last three days; orphan objects are ignored. Retrieval timestamps are not refreshed by loading an old snapshot. A failed private write is labelled as unverified/memory-only collection. No historical database rows are removed or declared migrated.

`/health` now separates process liveness, credential configuration, the paused database and independent fresh-feed/archive state. It never equates a configured database key with verified database availability. Database-dependent HTTP failures return a controlled 503 rather than pretending zero coverage.

The fresh collector caches a dedicated-bucket size scan for up to an hour and conservatively adds its own upload bytes between scans, under a single in-process writer lock. This avoids scanning every historical object on every two-minute write. The archive application budget is not an account-wide provider billing cap; concurrent external writers or other buckets are outside its control.
