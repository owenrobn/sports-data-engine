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

This is tested export/restore preparation, not a connected archive service. No Cloudflare account, bucket or credential has been created. The database remains stuck in PAUSING, so no live export, source count comparison or migration has been verified. Automatic retention, archive indexing, replay loading, source deletion and database storage-pressure monitoring are not enabled. Keep collection paused until these are verified. Private archive files must be served through authenticated backend reads, not public bucket URLs.

References: https://developers.cloudflare.com/r2/pricing/ and https://developers.cloudflare.com/r2/api/s3/api/ .

