import json
import unittest
from archive_store import archive_batch,canonical,encode,decode,read_batch,validate

class MemoryStore:
    def __init__(self): self.objects={};self.limit=8_000_000_000;self.corrupt=False
    def size(self): return sum(map(len,self.objects.values()))
    def write_once(self,key,data):
        if key in self.objects and self.objects[key]!=data: raise ValueError('overwrite')
        self.objects[key]=data
    def read(self,key): return b'corrupt' if self.corrupt else self.objects[key]
    def optional_read(self,key): return self.objects.get(key)

class ArchiveSafety(unittest.TestCase):
    def setUp(self):
        self.rows=[{'id':1,'event_id':'match','effective_at':None,'payload':{'unknown':None,'score':'40–A'}},
                   {'id':5,'event_id':'match','price':0.42,'observed_at':'2026-10-20T00:00:00Z'}]
    def archive(self,store):
        return archive_batch(store,'prediction_market_snapshots',self.rows,'2026-10-20T00:00:00Z',encoder=canonical,decoder=json.loads)
    def test_retry_is_idempotent_and_rows_remain_lossless(self):
        store=MemoryStore();first=self.archive(store);second=self.archive(store)
        self.assertEqual(first,second);self.assertEqual(len(store.objects),2)
        self.assertEqual(json.loads(store.read(first['object_key'])),self.rows)
    def test_corruption_never_publishes_verified_manifest(self):
        store=MemoryStore();store.corrupt=True
        with self.assertRaises(ValueError): self.archive(store)
        self.assertFalse(any(k.endswith('.manifest.json') for k in store.objects))
    def test_full_bucket_stops_before_upload(self):
        store=MemoryStore();store.limit=10
        with self.assertRaises(RuntimeError): self.archive(store)
        self.assertEqual(store.objects,{})
    def test_source_ids_must_be_unique_and_ordered(self):
        with self.assertRaises(ValueError): validate([{'id':1},{'id':1}])
        with self.assertRaises(ValueError): validate([{'id':2},{'id':1}])
    def test_actual_parquet_restore_preserves_every_field(self):
        try: import pyarrow
        except ImportError: self.skipTest('Archive dependency installed in CI')
        store=MemoryStore();manifest=archive_batch(store,'prediction_market_snapshots',self.rows,'2026-10-20T00:00:00Z')
        self.assertEqual(read_batch(store,manifest),self.rows)
        bad={**manifest,'row_count':99}
        with self.assertRaises(ValueError): read_batch(store,bad)

