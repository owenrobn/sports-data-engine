# Private historical archive preparation

The live database and historical archive have different jobs. Supabase retains canonical identities, links, small health/coverage summaries and a recent working set. A dedicated private Cloudflare R2 Standard bucket holds lossless compressed historical records. The archive is finite: stop collection before either store reaches its budget. Do not enable collection while the current database full-disk incident remains unresolved.

R2 Standard currently includes 10 GB-month storage, one million Class A operations and ten million Class B operations monthly; direct egress is free. Usage above these allowances can be billed. This preparation uses an 8 GB application budget to leave headroom. It is not a provider-enforced billing cap and cannot protect against other applications, concurrent writers, account-wide usage or manual uploads. Use a dedicated bucket and a single exporter. Do not enable Infrequent Access, public access, automatic expiration or paid upgrades.

## Account setup needed

Create/sign in to a Cloudflare account and review the R2 activation/billing terms yourself. Create a private Standard bucket named `sde-history`. Keep public development access and custom domains disabled. Create a bucket-scoped read/write S3 credential. Store credentials only in the operator's secure environment; never send them in chat or commit them. Credential creation grants storage access and must be performed by the account owner.

The exporter needs `SDE_ARCHIVE_ENDPOINT` (`https://<account-id>.r2.cloudflarestorage.com`), `SDE_ARCHIVE_BUCKET`, `SDE_ARCHIVE_ACCESS_KEY_ID`, `SDE_ARCHIVE_SECRET_ACCESS_KEY`, and optional `SDE_ARCHIVE_REGION` (default `auto`). Existing backend Supabase credentials are needed for read-only export. Default `SDE_ARCHIVE_MAX_BYTES` is 8000000000; it cannot exceed that limit in this preparation.

## Export and verify

Install `requirements-archive.txt` on a separate operator/maintenance runner. The Render web service keeps its smaller existing dependency set. Start with a dry run:

```
python archive_store.py --table prediction_market_snapshots --before 2026-10-07T00:00:00Z
```

Use an explicit past, timezone-aware cutoff selected for the retention window; the example is illustrative. Add `--upload` for a verified batch. Each invocation defaults to one batch of at most 1000 rows. Resume using the reported `next_start_after` value. Reduce `--page-size` for large payloads. A date filter uses observation time for state, book, price and timeline records; trades use execution time. Null timestamps are deliberately excluded and need separate audited handling. Exports do not imply complete historical coverage.

Every Parquet/Zstandard batch contains the complete original JSON rows, including nulls, provenance and unknown timestamps. Content-derived keys and conditional creation prevent overwrites. After uploading, the exporter downloads the object, compares its SHA-256 and decoded rows with the source batch, and only then publishes and reads back a manifest. The manifest holds row count, ID bounds, event IDs, file and logical checksums, cutoff and verification time. Failed uploads never publish a verified receipt. `read_batch` checks file checksum, logical checksum, count and ID bounds for a restore.

Exporting creates a second copy; it does not free database disk. The tool contains no delete operation. Before any removal, require a second independent restore, a durable archive index supporting match replay, re-export/compare the current source rows to detect updates, and approval of the exact verified rows to remove. Deletion must never use only an ID range or cutoff because gaps, late arrivals and updates can exist. Database compaction itself requires working space/WAL headroom and a maintenance window; exporting/deleting alone does not guarantee physical space recovery.

## Current limits

Historical database export and migration remain unverified. The private archive connection is configured, and the separate fresh-evidence lane verifies its own uploads and manifests; inspect /fresh for runtime status. PostgreSQL startup is blocked by exhausted disk space even when management metadata reports ACTIVE_HEALTHY. Automatic database retention, historical replay loading, source deletion and database compaction remain disabled. Keep database-backed collection paused until recovery and write capacity are verified. Private archive files must be served through authenticated backend reads, not public bucket URLs.

References: https://developers.cloudflare.com/r2/pricing/ and https://developers.cloudflare.com/r2/api/s3/api/ .


## Recovery update: independent fresh lane
On 2026-10-26, PostgreSQL startup logs still reported inability to write a WAL temporary file because the disk was full, despite the management status reporting ACTIVE_HEALTHY. No destructive repair is attempted by the application. The private bucket connection can be verified independently at `/archive/status`; that check does not verify historical export.

A separate `fresh_feed.py` collector now writes bounded, read-back-verified gzip JSON source snapshots to `sde/fresh/v1/` during database recovery. Its live verification status appears on `/fresh` and `/health`. This new prefix does not replace the existing Parquet archive format or its checks. Historical database export, removal, compaction and replay migration remain unverified and disabled. Database collection stays paused until recovery and write capacity are verified.

## Database-independent context and profiles
The fresh lane now collects up to four detailed match summaries and four identity-checked player/team profile lookups per sport per poll. It reuses the existing public ESPN adapters without database writes. Summary receipts retain supplied rosters, injuries, box score, venue, format and situation fields; absent fields and referenced but unfetched resources do not count as supplied facts. Tennis profiles require a matching source ID and name; football rosters require the expected team ID. Profiles retain normalized measurements, units, source URLs and retrieval times, with unknown provider update time and no fitness assessment. View `/fresh/players?sport=football` or `tennis`.

Detail receipts are limited to 128 KiB each and 1 MiB total per sport; profile receipts are limited to 64 KiB each and 512 KiB total per sport. Older cached receipts may be evicted from the current snapshot to enforce these bounds; immutable previously verified snapshots remain. Live summaries refresh at most every 15 minutes, other summaries and profiles every six hours. Failed refreshes preserve prior observations and timestamps and expose the failure class. These summary intervals are unsuitable as a complete live trading-state feed. Profiles and summaries observed after a target match's pre-match cutoff are excluded from its pre-match package. No complete injury coverage, workload, historical database recovery, full schedule, model calibration or engine readiness is claimed.
