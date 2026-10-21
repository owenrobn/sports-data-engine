"""Bounded, lossless, verify-before-publication archive. Never deletes database rows."""
import argparse
import hashlib
import io
import json
import os
from datetime import datetime, timezone
from urllib.parse import urlencode

TABLES={'prediction_market_snapshots':'observed_at','prediction_market_trades':'executed_at',
        'prediction_market_books':'observed_at','event_market_timeline':'observed_at',
        'football_match_events':'observed_at','tennis_match_events':'observed_at'}
MAX_ROWS=1000
MAX_BYTES=32*1024*1024

def canonical(rows):
    return json.dumps(rows,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()

def encode(rows):
    import pyarrow as pa
    import pyarrow.parquet as pq
    table=pa.table({'row_json':[canonical(r).decode() for r in rows]})
    target=io.BytesIO();pq.write_table(table,target,compression='zstd')
    return target.getvalue()

def decode(data):
    import pyarrow.parquet as pq
    return [json.loads(v) for v in pq.read_table(io.BytesIO(data),columns=['row_json']).column('row_json').to_pylist()]

def validate(rows):
    if not rows or len(rows)>MAX_ROWS: raise ValueError('Batch must contain 1–1000 rows')
    ids=[r.get('id') for r in rows]
    if any(type(i) is not int or i<0 for i in ids) or ids!=sorted(set(ids)):
        raise ValueError('Require unique ascending numeric source IDs')
    if len(canonical(rows))>MAX_BYTES: raise ValueError('Batch exceeds uncompressed byte budget; reduce page size')

def read_batch(store,manifest):
    data=store.read(manifest['object_key'])
    if hashlib.sha256(data).hexdigest()!=manifest['file_sha256']: raise ValueError('Archive file checksum mismatch')
    rows=decode(data)
    if len(rows)!=manifest['row_count'] or hashlib.sha256(canonical(rows)).hexdigest()!=manifest['rows_sha256']:
        raise ValueError('Archive logical checksum/count mismatch')
    validate(rows)
    if rows[0]['id']!=manifest['first_id'] or rows[-1]['id']!=manifest['last_id']:
        raise ValueError('Archive source ID mismatch')
    return rows

def archive_batch(store,table,rows,before,encoder=encode,decoder=decode):
    if table not in TABLES: raise ValueError('Unsupported source table')
    validate(rows)
    logical=hashlib.sha256(canonical(rows)).hexdigest()
    data=encoder(rows)
    if len(data)>MAX_BYTES: raise ValueError('Compressed batch exceeds byte budget')
    key=f'sde/v1/{table}/{before[:10]}/{rows[0]["id"]}-{rows[-1]["id"]}-{logical}.parquet'
    manifest={'version':1,'table_name':table,'object_key':key,'row_count':len(rows),
              'first_id':rows[0]['id'],'last_id':rows[-1]['id'],'file_sha256':hashlib.sha256(data).hexdigest(),
              'rows_sha256':logical,'compressed_bytes':len(data),'before':before,
              'event_ids':sorted({str(r['event_id']) for r in rows if r.get('event_id')}),
              'verified_at':datetime.now(timezone.utc).isoformat(),'format':'parquet/zstd; lossless row_json'}
    # Check the complete dedicated bucket, not merely this export's prefix.
    # Single-writer operation is required; this is an application budget, not a provider billing cap.
    receipt=canonical(manifest)
    if store.size()+len(data)+len(receipt)>store.limit: raise RuntimeError('Archive storage budget reached')
    store.write_once(key,data)
    downloaded=store.read(key)
    if hashlib.sha256(downloaded).hexdigest()!=manifest['file_sha256'] or canonical(decoder(downloaded))!=canonical(rows):
        raise ValueError('Read-back restore verification failed; no manifest published')
    manifest_key=key+'.manifest.json'
    # An existing manifest is reusable only if its immutable verification fields agree.
    existing=store.optional_read(manifest_key)
    if existing is not None:
        saved=json.loads(existing)
        for field in ('object_key','rows_sha256','file_sha256','row_count','first_id','last_id','table_name'):
            if saved.get(field)!=manifest[field]: raise ValueError('Conflicting existing manifest')
        return saved
    store.write_once(manifest_key,receipt)
    if store.read(manifest_key)!=receipt: raise ValueError('Manifest verification failed')
    return manifest

class S3Store:
    def __init__(self):
        import boto3
        from botocore.config import Config
        endpoint=os.environ['SDE_ARCHIVE_ENDPOINT']
        if not endpoint.startswith('https://'): raise ValueError('Archive endpoint must use HTTPS')
        self.bucket=os.environ['SDE_ARCHIVE_BUCKET']
        self.limit=int(os.getenv('SDE_ARCHIVE_MAX_BYTES','8000000000'))
        if not 0<self.limit<=8000000000: raise ValueError('Free preparation limits archive to at most 8 GB')
        self.client=boto3.client('s3',endpoint_url=endpoint,region_name=os.getenv('SDE_ARCHIVE_REGION','auto'),
             aws_access_key_id=os.environ['SDE_ARCHIVE_ACCESS_KEY_ID'],aws_secret_access_key=os.environ['SDE_ARCHIVE_SECRET_ACCESS_KEY'],
             config=Config(retries={'max_attempts':3,'mode':'standard'},connect_timeout=10,read_timeout=60))
    def size(self):
        total=0;pages=0
        for page in self.client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket):
            total+=sum(x['Size'] for x in page.get('Contents',[]));pages+=1
            if pages>1000: raise RuntimeError('Bucket listing budget exceeded')
        return total
    def optional_read(self,key):
        from botocore.exceptions import ClientError
        try: return self.read(key)
        except ClientError as ex:
            if ex.response['Error']['Code'] in ('NoSuchKey','404'): return None
            raise
    def read(self,key):
        response=self.client.get_object(Bucket=self.bucket,Key=key)
        body=response['Body']
        try:
            if response['ContentLength']>MAX_BYTES: raise ValueError('Object exceeds read budget')
            value=body.read(MAX_BYTES+1)
            if len(value)>MAX_BYTES: raise ValueError('Object exceeds read budget')
            return value
        finally: body.close()
    def write_once(self,key,data):
        from botocore.exceptions import ClientError
        try:
            self.client.put_object(Bucket=self.bucket,Key=key,Body=data,IfNoneMatch='*',StorageClass='STANDARD')
        except ClientError as ex:
            if ex.response['Error']['Code'] not in ('PreconditionFailed','412'): raise
            if self.read(key)!=data: raise ValueError('Refusing to overwrite an archive object') from None

def export(args):
    import httpx
    before=datetime.fromisoformat(args.before.replace('Z','+00:00'))
    if before.tzinfo is None or before>datetime.now(timezone.utc): raise ValueError('Use a past timezone-aware cutoff')
    if not 1<=args.max_batches<=20 or not 1<=args.page_size<=MAX_ROWS: raise ValueError('Export budget exceeded')
    if args.start_after<0: raise ValueError('Invalid source cursor')
    store=S3Store() if args.upload else None
    url=os.environ['SUPABASE_URL'].rstrip('/')
    key=os.getenv('SUPABASE_SECRET_KEY') or os.environ['SUPABASE_SERVICE_ROLE_KEY']
    cursor=args.start_after
    with httpx.Client(timeout=30,headers={'apikey':key,'Authorization':'Bearer '+key}) as client:
        for _ in range(args.max_batches):
            query=urlencode({'select':'*','id':'gt.'+str(cursor),TABLES[args.table]:'lt.'+before.isoformat(),'order':'id.asc','limit':args.page_size})
            response=client.get(url+'/rest/v1/'+args.table+'?'+query);response.raise_for_status();rows=response.json()
            if not rows: break
            validate(rows)
            manifest=archive_batch(store,args.table,rows,before.isoformat()) if store else {'dry_run':True,'row_count':len(rows),'first_id':rows[0]['id'],'last_id':rows[-1]['id']}
            print(json.dumps(manifest));cursor=rows[-1]['id']
    # Cursor is reported only after verified upload (or explicitly labelled dry run).
    print(json.dumps({'next_start_after':cursor,'uploaded':bool(store),'source_rows_deleted':0}))

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--table',choices=TABLES,required=True);parser.add_argument('--before',required=True)
    parser.add_argument('--start-after',type=int,default=0);parser.add_argument('--max-batches',type=int,default=1)
    parser.add_argument('--page-size',type=int,default=1000);parser.add_argument('--upload',action='store_true')
    try: export(parser.parse_args())
    except Exception as exc:
        # SDK/HTTP exceptions may contain URLs/credentials; keep operator output safe.
        print(json.dumps({'status':'failed','error_type':type(exc).__name__,'source_rows_deleted':0}));raise SystemExit(1)

