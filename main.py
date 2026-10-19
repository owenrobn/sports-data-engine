import asyncio, json, os, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
import httpx

app=FastAPI(title="Sports Data Engine",version="0.5.0")
SUPABASE_URL=os.getenv("SUPABASE_URL","").rstrip("/")
SUPABASE_KEY=os.getenv("SUPABASE_SECRET_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
mem_events=[]; mem_jobs={}

def now(): return datetime.now(timezone.utc).isoformat()
def headers():
    return {"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Content-Type":"application/json","Prefer":"return=representation"}
async def db(method,table,payload=None,query=""):
    if not (SUPABASE_URL and SUPABASE_KEY): return None
    async with httpx.AsyncClient(timeout=25) as c:
        r=await c.request(method,f"{SUPABASE_URL}/rest/v1/{table}{query}",headers=headers(),json=payload)
        r.raise_for_status()
        return r.json() if r.content else None
async def job_patch(jid,**fields):
    fields["updated_at"]=now(); mem_jobs.setdefault(jid,{}).update(fields)
    try: await db("PATCH","ingestion_jobs",fields,f"?id=eq.{jid}")
    except Exception as e: emit("db_warning","Job persistence failed",error=str(e))
def emit(kind,message,**data):
    item={"ts":now(),"kind":kind,"message":message,**data}; mem_events.insert(0,item); del mem_events[500:]

@app.on_event("startup")
async def startup():
    emit("system","SDE v0.5 online",persistent=bool(SUPABASE_KEY))
    if SUPABASE_KEY:
        asyncio.create_task(bootstrap_collectors())
        asyncio.create_task(collection_loop())

async def collection_loop():
    await asyncio.sleep(15)
    while True:
        try:
            date=datetime.now(timezone.utc).date().isoformat()
            for provider in ("polymarket","thesportsdb"):
                jid=await create_job(provider,"football",date)
                asyncio.create_task(run_discovery(jid,provider,"football",date))
            emit("scheduler","Scheduled football collection launched",date=date)
        except Exception as e: emit("error","Scheduled collection launch failed",error=str(e))
        await asyncio.sleep(3600)

async def sport_id(slug):
    rows=await db("GET","sports",query=f"?select=id&slug=eq.{slug}&limit=1")
    return rows[0]["id"] if rows else None

async def normalize_polymarket(items,sport):
    sid=await sport_id(sport); event_count=0; snap_count=0
    if not sid: return event_count,snap_count
    for item in items:
        pid=str(item.get("id") or item.get("slug") or "")
        if not pid: continue
        name=item.get("title") or item.get("question") or item.get("slug") or pid
        mapping=await db("GET","provider_mappings",query=f"?select=canonical_id&provider=eq.polymarket&entity_type=eq.event&provider_entity_id=eq.{pid}&limit=1")
        if mapping: eid=mapping[0]["canonical_id"]
        else:
            er=await db("POST","events",{"sport_id":sid,"name":name,"starts_at":item.get("startDate") or item.get("start_date"),"status":"active" if item.get("active") else "scheduled","metadata":{"source":"polymarket","slug":item.get("slug")}})
            eid=er[0]["id"]; event_count+=1
            await db("POST","provider_mappings",{"provider":"polymarket","entity_type":"event","provider_entity_id":pid,"canonical_id":eid})
        markets=item.get("markets") or []
        for m in markets:
            mid=str(m.get("id") or m.get("conditionId") or "")
            outcomes=m.get("outcomes") or []
            prices=m.get("outcomePrices") or []
            if isinstance(outcomes,str):
                try: outcomes=json.loads(outcomes)
                except: outcomes=[outcomes]
            if isinstance(prices,str):
                try: prices=json.loads(prices)
                except: prices=[]
            for i,outcome in enumerate(outcomes):
                try: price=float(prices[i]) if i<len(prices) else None
                except: price=None
                await db("POST","prediction_market_snapshots",{"event_id":eid,"venue":"polymarket","market_id":mid or pid,"outcome":str(outcome),"price":price,"volume":m.get("volumeNum") or m.get("volume"),"liquidity":m.get("liquidityNum") or m.get("liquidity"),"observed_at":now(),"payload":{"question":m.get("question"),"slug":m.get("slug")}})
                snap_count+=1
        hist,books=await backfill_polymarket_event(item,eid)
        snap_count+=hist
    return event_count,snap_count

async def polymarket_price_history(token_id,event_id,market_id,start_ts=None,end_ts=None):
    params={"market":token_id,"fidelity":1}
    if start_ts is not None: params["startTs"]=start_ts
    if end_ts is not None: params["endTs"]=end_ts
    if start_ts is None and end_ts is None: params["interval"]="max"
    async with httpx.AsyncClient(timeout=45) as client:
        response=await client.get("https://clob.polymarket.com/prices-history",params=params)
        response.raise_for_status(); history=response.json().get("history",[])
    rows=[]
    for point in history:
        if point.get("t") is None or point.get("p") is None: continue
        rows.append({"event_id":event_id,"venue":"polymarket","market_id":market_id,"outcome":token_id,"price":point["p"],"observed_at":datetime.fromtimestamp(int(point["t"]),timezone.utc).isoformat(),"payload":{"stream":"clob_price_history","token_id":token_id}})
    for i in range(0,len(rows),250):
        await db("POST","prediction_market_snapshots",rows[i:i+250])
    return len(rows)

async def polymarket_book_snapshot(token_id,event_id,market_id):
    async with httpx.AsyncClient(timeout=30) as client:
        response=await client.get("https://clob.polymarket.com/book",params={"token_id":token_id})
        response.raise_for_status(); book=response.json()
    bids=book.get("bids") or []; asks=book.get("asks") or []
    def px(x):
        try: return float(x.get("price"))
        except: return None
    bb=max([px(x) for x in bids if px(x) is not None],default=None)
    ba=min([px(x) for x in asks if px(x) is not None],default=None)
    await db("POST","prediction_market_books",{"event_id":event_id,"venue":"polymarket","market_id":market_id,"token_id":token_id,"best_bid":bb,"best_ask":ba,"spread":(ba-bb if ba is not None and bb is not None else None),"bids":bids,"asks":asks,"observed_at":now()})
    return 1

async def backfill_polymarket_event(item,event_id):
    total=0; books=0
    for market in item.get("markets") or []:
        market_id=str(market.get("id") or market.get("conditionId") or item.get("id"))
        tokens=market.get("clobTokenIds") or []
        if isinstance(tokens,str):
            try: tokens=json.loads(tokens)
            except: tokens=[]
        for token in tokens:
            try: total+=await polymarket_price_history(str(token),event_id,market_id)
            except Exception as e: emit("warning","CLOB history unavailable",market_id=market_id,error=str(e))
            try: books+=await polymarket_book_snapshot(str(token),event_id,market_id)
            except Exception as e: emit("warning","CLOB book unavailable",market_id=market_id,error=str(e))
    return total,books

async def bootstrap_collectors():
    await asyncio.sleep(3)
    try:
        existing=await db("GET","ingestion_jobs",query="?select=id&limit=1")
        if existing:
            emit("system","Bootstrap already completed; skipping duplicate seed")
            return
        date=datetime.now(timezone.utc).date().isoformat()
        for provider in ("sofascore","polymarket"):
            jid=await create_job(provider,"football",date)
            asyncio.create_task(run_discovery(jid,provider,"football",date))
        emit("system","Initial football collectors launched",date=date)
    except Exception as e:
        emit("error","Bootstrap launch failed",error=str(e))
@app.get("/")
async def root(): return RedirectResponse("/dashboard")
@app.get("/health")
async def health(): return {"ok":True,"service":"sports-data-engine","version":"0.5.0","database":bool(SUPABASE_KEY),"time":now()}
@app.get("/providers")
async def providers(): return {"providers":{"sofascore":"blocked-on-render","polymarket":"active","statsbomb":"backfill","football-data":"backfill","thesportsdb":"active-fallback"},"sports":["football","tennis"]}
@app.get("/status")
async def status():
    counts={}
    if SUPABASE_KEY:
        for table in ("events","event_snapshots","odds_snapshots","prediction_market_snapshots","prediction_market_trades","prediction_market_books","raw_ingestions"):
            try:
                async with httpx.AsyncClient(timeout=15) as c:
                    h=headers(); h["Prefer"]="count=exact"; r=await c.get(f"{SUPABASE_URL}/rest/v1/{table}?select=id&limit=1",headers=h); counts[table]=r.headers.get("content-range","0/0").split("/")[-1]
            except: counts[table]="?"
    return {"ok":True,"version":"0.4.0","persistent":bool(SUPABASE_KEY),"counts":counts,"events_buffered":len(mem_events)}
@app.get("/progress")
async def progress(): return mem_events[:100]
@app.get("/progress/stream")
async def stream():
    async def gen():
        seen=None
        while True:
            if mem_events and mem_events[0]["ts"]!=seen:
                seen=mem_events[0]["ts"]; yield "data: "+json.dumps(mem_events[0])+"\n\n"
            else: yield ": keepalive\n\n"
            await asyncio.sleep(2)
    return StreamingResponse(gen(),media_type="text/event-stream")
@app.get("/jobs")
async def jobs():
    if SUPABASE_KEY:
        try: return await db("GET","ingestion_jobs",query="?select=*&order=created_at.desc&limit=100")
        except: pass
    return sorted(mem_jobs.values(),key=lambda x:x.get("created_at",""),reverse=True)

async def create_job(provider,sport,date):
    jid=str(uuid.uuid4()); row={"id":jid,"job_type":"discover","provider":provider,"sport":sport,"status":"queued","progress":0,"params":{"date":date},"created_at":now(),"updated_at":now()}
    mem_jobs[jid]=row
    if SUPABASE_KEY: await db("POST","ingestion_jobs",row)
    return jid

async def raw_ingest(provider,resource_id,payload):
    row={"provider":provider,"resource_type":"discovery","provider_resource_id":resource_id,"observed_at":now(),"retrieved_at":now(),"metadata":{"payload":payload}}
    if SUPABASE_KEY: await db("POST","raw_ingestions",row)

@app.post("/jobs/discover/{provider}/{sport}/{date}")
async def discover(provider:str,sport:str,date:str):
    if provider not in ("sofascore","polymarket","thesportsdb"): raise HTTPException(400,"cloud discovery currently supports sofascore or polymarket")
    jid=await create_job(provider,sport,date); asyncio.create_task(run_discovery(jid,provider,sport,date)); return {"job_id":jid}

async def run_discovery(jid,provider,sport,date):
    await job_patch(jid,status="running",progress=10,started_at=now()); emit("job","Discovery started",job_id=jid,provider=provider,sport=sport)
    try:
        if provider=="thesportsdb":
            key=os.getenv("THESPORTSDB_KEY","123")
            async with httpx.AsyncClient(timeout=30) as client:
                response=await client.get(f"https://www.thesportsdb.com/api/v1/json/{key}/eventsday.php",params={"d":date,"s":"Soccer"})
                response.raise_for_status(); payload=response.json()
            items=payload.get("events") or []
        elif provider=="sofascore":
            url=f"https://www.sofascore.com/api/v1/sport/{sport}/scheduled-events/{date}"
            async with httpx.AsyncClient(timeout=30,headers={"User-Agent":"SDE/0.4"}) as c:
                r=await c.get(url); r.raise_for_status(); payload=r.json()
            items=payload.get("events",[])
        else:
            async with httpx.AsyncClient(timeout=30) as c:
                r=await c.get("https://gamma-api.polymarket.com/events",params={"active":"true","closed":"false","limit":100}); r.raise_for_status(); all_items=r.json()
            needle=sport.lower(); items=[x for x in all_items if needle in json.dumps(x).lower()]
            payload={"events":items,"source_count":len(all_items)}
        await job_patch(jid,progress=55,message=f"Fetched {len(items)} events")
        await raw_ingest(provider,f"{sport}:{date}",payload)
        normalized_events=0; market_snapshots=0
        if provider=="polymarket":
            normalized_events,market_snapshots=await normalize_polymarket(items,sport)
        await job_patch(jid,status="completed",progress=100,result={"items":len(items),"normalized_events":normalized_events,"market_snapshots":market_snapshots},finished_at=now())
        emit("job","Discovery completed",job_id=jid,provider=provider,sport=sport,items=len(items))
    except Exception as e:
        await job_patch(jid,status="failed",error=str(e),finished_at=now()); emit("error","Discovery failed",job_id=jid,error=str(e))

DASH="""<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width"><title>SDE Live</title><style>body{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:auto;max-width:1100px;padding:24px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:12px}.card{background:#131b31;border:1px solid #263250;border-radius:14px;padding:18px;margin:12px 0}.ok{color:#62e6a7}pre{white-space:pre-wrap;word-break:break-word;color:#b9c7e8}button{padding:9px 13px;border:0;border-radius:8px;margin:4px}</style></head><body><h1>Sports Data Engine <small>v0.5</small></h1><p class=ok>● LIVE</p><div class=grid><div class=card><h3>System</h3><pre id=s>loading…</pre></div><div class=card><h3>Providers</h3><pre id=p>loading…</pre></div></div><div class=card><h3>Jobs</h3><pre id=j>loading…</pre></div><div class=card><h3>Live feed</h3><pre id=f></pre></div><script>async function r(){s.textContent=JSON.stringify(await(await fetch('/status')).json(),null,2);p.textContent=JSON.stringify(await(await fetch('/providers')).json(),null,2);j.textContent=JSON.stringify(await(await fetch('/jobs')).json(),null,2)}r();setInterval(r,5000);let es=new EventSource('/progress/stream');es.onmessage=e=>{f.textContent=e.data+'\n'+f.textContent.slice(0,14000)}</script></body></html>"""
@app.get("/dashboard",response_class=HTMLResponse)
async def dashboard(): return DASH
