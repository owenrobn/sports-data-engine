"""One bounded, read-only archive connection check per process startup."""
import asyncio
import os
from datetime import datetime, timezone

STATE = {'status': 'pending', 'checked_at': None, 'historical_export_verified': False}


def probe():
    required = ('ENDPOINT', 'BUCKET', 'ACCESS_KEY_ID', 'SECRET_ACCESS_KEY')
    result = {'status': 'not_configured', 'checked_at': datetime.now(timezone.utc).isoformat(),
              'historical_export_verified': False}
    if not all(os.getenv('SDE_ARCHIVE_' + name) for name in required):
        return result
    try:
        from archive_store import S3Store
        store = S3Store()
        # Listing one object verifies authentication and bucket access without
        # downloading history, modifying objects or scanning the bucket.
        store.client.list_objects_v2(Bucket=store.bucket, MaxKeys=1)
        result['status'] = 'connected'
    except Exception:
        # SDK errors may include signed URLs, credential IDs or response bodies.
        result['status'] = 'connection_failed'
    return result


def install(app):
    @app.on_event('startup')
    async def start_archive_check():
        async def check():
            STATE.update(await asyncio.to_thread(probe))
        app.state.archive_check_task = asyncio.create_task(check())

    @app.get('/archive/status')
    async def archive_status():
        # Never contact the provider from a public request.
        return dict(STATE)


def status_html():
    import html
    labels = {'pending': 'Checking connection', 'not_configured': 'Credentials missing',
              'connected': 'Connected (bucket read access verified)',
              'connection_failed': 'Connection failed; check restricted credentials and endpoint'}
    return ('<section><h2>Historical archive</h2><p>' + labels[STATE['status']] +
            '</p><p>Last check: ' + html.escape(STATE['checked_at'] or 'pending') +
            '</p><p>Historical export has not yet been verified. Database recovery is required before export can run.</p></section>')
