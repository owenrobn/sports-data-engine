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
        asyncio.create_task(historical_polymarket_loop())
        asyncio.create_task(ingest_statsbomb_open())

SPORT_TERMS={
 "football":["soccer","football","premier league","champions league","la liga","serie a","bundesliga","ligue 1","mls","uefa","fifa"],
 "tennis":["tennis","atp","wta","wimbledon","us open tennis","australian open tennis","french open tennis"],
 "mma":["ufc","mma"],
 "basketball":["nba","basketball","wnba","ncaa basketball"],
 "baseball":["mlb","baseball"],
 "hockey":["nhl","hockey"],
 "cricket":["cricket"],
 "rugby":["rugby"]
}
def classify_sport(item):
    text=" ".join(str(item.get(k) or "") for k in ("title","question","slug","description")).lower()
    tags=" ".join(str(t.get("label") or t.get("name") or t.get("slug") or "") for t in (item.get("tags") or []) if isinstance(t,dict)).lower()
    hay=text+" "+tags
    for sport,terms in SPORT_TERMS.items():
        if any(term in hay for term in terms): return sport
    return None

async def historical_polymarket_loop():
    await asyncio.sleep(45)
    offset=0
    while True:
        try:
            async with httpx.AsyncClient(timeout=45) as client:
                response=await client.get("https://gamma-api.polymarket.com/events",params={"closed":"true","limit":50,"offset":offset,"order":"endDate","ascending":"false"})
                response.raise_for_status(); batch=response.json()
            grouped={}
            for item in batch:
                sport=classify_sport(item)
                if sport: grouped.setdefault(sport,[]).append(item)
            for sport,items in grouped.items():
                await raw_ingest("polymarket",f"historical:{sport}:{offset}",{"events":items,"source_count":len(batch)})
                await normalize_polymarket(items,sport)
            emit("backfill","Historical Polymarket page processed",offset=offset,sports={k:len(v) for k,v in grouped.items()})
            offset += 50
            if not batch: offset=0
        except Exception as e: emit("error","Historical Polymarket backfill page failed",offset=offset,error=str(e))
        await asyncio.sleep(20)

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
        hist,books,trades=await backfill_polymarket_event(item,eid)
        snap_count+=hist
        emit("backfill","Polymarket history archived",event_id=eid,price_points=hist,books=books,trades=trades)
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
        rows.append({"event_id":event_id,"venue":"polymarket","market_id":market_id,"outcome":token_id,"token_id":token_id,"price":point["p"],"observed_at":datetime.fromtimestamp(int(point["t"]),timezone.utc).isoformat(),"effective_at":datetime.fromtimestamp(int(point["t"]),timezone.utc).isoformat(),"payload":{"stream":"clob_price_history","token_id":token_id}})
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

async def polymarket_trades(condition_id,event_id):
    total=0; offset=0
    while offset<=10000:
        async with httpx.AsyncClient(timeout=45) as client:
            response=await client.get("https://data-api.polymarket.com/trades",params={"market":condition_id,"limit":1000,"offset":offset,"takerOnly":"false"})
            response.raise_for_status(); trades=response.json()
        if not trades: break
        rows=[]
        for t in trades:
            ts=t.get("timestamp")
            if ts is None: continue
            price=t.get("price"); size=t.get("size")
            rows.append({"event_id":event_id,"venue":"polymarket","market_id":condition_id,"token_id":str(t.get("asset") or ""),"trade_id":t.get("transactionHash"),"trader":t.get("proxyWallet"),"side":t.get("side"),"outcome":t.get("outcome"),"price":price,"size":size,"amount":(float(price)*float(size) if price is not None and size is not None else None),"executed_at":datetime.fromtimestamp(int(ts),timezone.utc).isoformat(),"observed_at":now(),"payload":{"title":t.get("title"),"slug":t.get("slug"),"eventSlug":t.get("eventSlug"),"name":t.get("name"),"pseudonym":t.get("pseudonym")}})
        for i in range(0,len(rows),250): await db("POST","prediction_market_trades",rows[i:i+250])
        total+=len(rows)
        if len(trades)<1000: break
        offset+=1000
    return total

async def backfill_polymarket_event(item,event_id):
    total=0; books=0; trades=0
    for market in item.get("markets") or []:
        market_id=str(market.get("id") or market.get("conditionId") or item.get("id"))
        condition_id=str(market.get("conditionId") or "")
        if condition_id:
            try: trades+=await polymarket_trades(condition_id,event_id)
            except Exception as e: emit("warning","Trade history unavailable",market_id=market_id,error=str(e))
        tokens=market.get("clobTokenIds") or []
        if isinstance(tokens,str):
            try: tokens=json.loads(tokens)
            except: tokens=[]
        for token in tokens:
            try: total+=await polymarket_price_history(str(token),event_id,market_id)
            except Exception as e: emit("warning","CLOB history unavailable",market_id=market_id,error=str(e))
            try: books+=await polymarket_book_snapshot(str(token),event_id,market_id)
            except Exception as e: emit("warning","CLOB book unavailable",market_id=market_id,error=str(e))
    return total,books,trades

async def ingest_statsbomb_open():
    base="https://raw.githubusercontent.com/statsbomb/open-data/master/data"
    async with httpx.AsyncClient(timeout=60) as client:
        comps=(await client.get(base+"/competitions.json")).json()
        picked=[x for x in comps if x.get("competition_gender")=="male"][:8]
        for comp in picked:
            try:
                matches=(await client.get(base+f'/matches/{comp["competition_id"]}/{comp["season_id"]}.json')).json()
            except: continue
            for match in matches[:50]:
                name=f'{match.get("home_team",{}).get("home_team_name")} vs {match.get("away_team",{}).get("away_team_name")}'
                start=f'{match.get("match_date")}T{match.get("kick_off") or "00:00:00"}'
                sid=await sport_id("football"); pid=str(match.get("match_id"))
                mapping=await db("GET","provider_mappings",query=f"?provider=eq.statsbomb&entity_type=eq.event&provider_entity_id=eq.{pid}&select=canonical_id&limit=1")
                if mapping: eid=mapping[0]["canonical_id"]
                else:
                    created=await db("POST","events",{"sport_id":sid,"name":name,"starts_at":start,"status":match.get("match_status") or "historical","metadata":{"source":"statsbomb","competition":comp.get("competition_name"),"season":comp.get("season_name"),"home_score":match.get("home_score"),"away_score":match.get("away_score")}})
                    if not created: continue
                    eid=created[0]["id"]; await db("POST","provider_mappings",{"provider":"statsbomb","entity_type":"event","provider_entity_id":pid,"canonical_id":eid})
                try:
                    evs=(await client.get(base+f'/events/{pid}.json')).json()
                    rows=[]
                    for x in evs:
                        typ=(x.get("type") or {}).get("name")
                        if typ not in ("Shot","Own Goal Against","Own Goal For","Yellow Card","Red Card","Substitution"): continue
                        minute=x.get("minute"); second=x.get("second"); period=x.get("period")
                        rows.append({"event_id":eid,"provider":"statsbomb","provider_event_id":str(x.get("id")),"event_type":typ,"period":period,"minute":minute,"second":second,"team_name":(x.get("team") or {}).get("name"),"player_name":(x.get("player") or {}).get("name"),"xg":(x.get("shot") or {}).get("statsbomb_xg"),"payload":x})
                    for i in range(0,len(rows),200): await db("POST","football_match_events",rows[i:i+200])
                except Exception as ex: emit("warning","StatsBomb event ingest failed",match=pid,error=str(ex))
            emit("backfill","StatsBomb competition imported",competition=comp.get("competition_name"),season=comp.get("season_name"))

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
@app.get("/data/overview")
async def data_overview():
    if not SUPABASE_KEY: return {"events":[],"markets":[],"books":[],"trades":[]}
    events=await db("GET","events",query="?select=id,name,starts_at,status,metadata&order=updated_at.desc&limit=20") or []
    markets=await db("GET","prediction_market_snapshots",query="?select=event_id,market_id,outcome,price,volume,liquidity,observed_at&order=id.desc&limit=100") or []
    books=await db("GET","prediction_market_books",query="?select=event_id,market_id,token_id,best_bid,best_ask,spread,observed_at&order=id.desc&limit=30") or []
    trades=await db("GET","prediction_market_trades",query="?select=event_id,market_id,trader,side,outcome,price,size,amount,executed_at&order=id.desc&limit=50") or []
    return {"events":events,"markets":markets,"books":books,"trades":trades}

@app.get("/data/market-summary")
async def market_summary():
    if not SUPABASE_KEY: return []
    return await db("GET","prediction_market_snapshots",query="?select=market_id,outcome,price,observed_at,event_id&order=observed_at.desc&limit=500") or []

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

DASH="""<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width"><title>SDE Data Console</title><style>
body{font-family:system-ui;background:#07101f;color:#e8eefc;margin:auto;max-width:1400px;padding:22px}h1{margin-bottom:4px}.muted{color:#8fa4c9}.ok{color:#55e6a5}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}.card{background:#101b31;border:1px solid #263858;border-radius:14px;padding:16px;margin:12px 0;overflow:auto}.metric{font-size:28px;font-weight:700}table{width:100%;border-collapse:collapse;font-size:13px}th,td{text-align:left;padding:8px;border-bottom:1px solid #253550;white-space:nowrap}th{position:sticky;top:0;background:#101b31;color:#9db6df}.scroll{max-height:420px;overflow:auto}.pill{padding:3px 7px;border-radius:12px;background:#1b2c4b}pre{white-space:pre-wrap;word-break:break-word;color:#b9c7e8}
</style></head><body><h1>Sports Data Engine <small>v0.5</small></h1><div class=muted>Live sports + prediction-market data console · <span class=ok>● connected</span></div>
<div class=grid id=metrics></div>
<div class=card><h2>Canonical sports events</h2><div class=scroll><table><thead><tr><th>Event</th><th>Status</th><th>Starts</th><th>Source</th></tr></thead><tbody id=events></tbody></table></div></div>
<div class=card><h2>Polymarket price observations</h2><div class=scroll><table><thead><tr><th>Observed</th><th>Market</th><th>Outcome / token</th><th>Price</th><th>Volume</th><th>Liquidity</th></tr></thead><tbody id=markets></tbody></table></div></div>
<div class=grid><div class=card><h2>Order books</h2><div class=scroll><table><thead><tr><th>Observed</th><th>Market</th><th>Bid</th><th>Ask</th><th>Spread</th></tr></thead><tbody id=books></tbody></table></div></div>
<div class=card><h2>Executed trades / wallets</h2><div class=scroll><table><thead><tr><th>Time</th><th>Wallet</th><th>Side</th><th>Outcome</th><th>Price</th><th>Size</th></tr></thead><tbody id=trades></tbody></table></div></div></div>
<div class=card><h2>Ingestion jobs</h2><div class=scroll><table><thead><tr><th>Provider</th><th>Sport</th><th>Status</th><th>Progress</th><th>Result</th></tr></thead><tbody id=jobs></tbody></table></div></div>
<div class=card><h2>Live collector feed</h2><pre id=feed>Waiting for collector events…</pre></div>
<script>
const el=id=>document.getElementById(id);\nconst esc=x=>String(x??'').replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
const short=x=>x?String(x).slice(0,16)+'…':'—'; const n=x=>x==null?'—':x;
async function refresh(){try{
 const [s,d,j]=await Promise.all([fetch('/status').then(x=>x.json()),fetch('/data/overview').then(x=>x.json()),fetch('/jobs').then(x=>x.json())]);
 el('metrics').innerHTML=Object.entries(s.counts||{}).map(([k,v])=>'<div class=card><div class=muted>'+esc(k.replaceAll('_',' '))+'</div><div class=metric>'+esc(v)+'</div></div>').join('');
 el('events').innerHTML=(d.events||[]).map(x=>'<tr><td>'+esc(x.name)+'</td><td><span class=pill>'+esc(x.status)+'</span></td><td>'+esc(x.starts_at||'—')+'</td><td>'+esc(x.metadata?.source||'—')+'</td></tr>').join('')||'<tr><td colspan=4>No normalized events yet</td></tr>';
 el('markets').innerHTML=(d.markets||[]).map(x=>'<tr><td>'+esc(x.observed_at)+'</td><td>'+esc(short(x.market_id))+'</td><td>'+esc(x.outcome)+'</td><td>'+esc(n(x.price))+'</td><td>'+esc(n(x.volume))+'</td><td>'+esc(n(x.liquidity))+'</td></tr>').join('')||'<tr><td colspan=6>No market observations yet</td></tr>';
 el('books').innerHTML=(d.books||[]).map(x=>'<tr><td>'+esc(x.observed_at)+'</td><td>'+esc(short(x.market_id))+'</td><td>'+esc(n(x.best_bid))+'</td><td>'+esc(n(x.best_ask))+'</td><td>'+esc(n(x.spread))+'</td></tr>').join('')||'<tr><td colspan=5>No book snapshots yet</td></tr>';
 el('trades').innerHTML=(d.trades||[]).map(x=>'<tr><td>'+esc(x.executed_at)+'</td><td>'+esc(short(x.trader))+'</td><td>'+esc(x.side)+'</td><td>'+esc(x.outcome)+'</td><td>'+esc(n(x.price))+'</td><td>'+esc(n(x.size))+'</td></tr>').join('')||'<tr><td colspan=6>No executed trades captured yet</td></tr>';
 el('jobs').innerHTML=(j||[]).slice(0,30).map(x=>'<tr><td>'+esc(x.provider)+'</td><td>'+esc(x.sport)+'</td><td>'+esc(x.status)+'</td><td>'+esc(x.progress)+'%</td><td>'+esc(JSON.stringify(x.result||{}))+'</td></tr>').join('');
}catch(e){el('feed').textContent='Dashboard refresh error: '+e}}
refresh();setInterval(refresh,5000);</script></body></html>"""
@app.get("/dashboard",response_class=HTMLResponse)
async def dashboard():
    counts=(await status()).get("counts",{})
    d=await data_overview()
    def e(v):
        import html
        return html.escape(str(v if v is not None else "—"))
    cards="".join('<div class="card"><div class="muted">'+e(k.replace("_"," "))+'</div><div class="metric">'+e(v)+'</div></div>' for k,v in counts.items())
    erows="".join("<tr><td>"+e(x.get("name"))+"</td><td>"+e(x.get("status"))+"</td><td>"+e(x.get("starts_at"))+"</td><td>"+e((x.get("metadata") or {}).get("source"))+"</td></tr>" for x in d.get("events",[]))
    mrows="".join("<tr><td>"+e(x.get("observed_at"))+"</td><td>"+e(str(x.get("market_id") or "")[:20])+"</td><td>"+e(x.get("outcome"))+"</td><td>"+e(x.get("price"))+"</td><td>"+e(x.get("volume"))+"</td><td>"+e(x.get("liquidity"))+"</td></tr>" for x in d.get("markets",[]))
    brows="".join("<tr><td>"+e(x.get("observed_at"))+"</td><td>"+e(str(x.get("market_id") or "")[:20])+"</td><td>"+e(x.get("best_bid"))+"</td><td>"+e(x.get("best_ask"))+"</td><td>"+e(x.get("spread"))+"</td></tr>" for x in d.get("books",[]))
    page=DASH[:DASH.find("<script>")]+"</body></html>"
    page=page.replace('<div class=grid id=metrics></div>','<div class=grid id=metrics>'+cards+'</div>')
    page=page.replace('<tbody id=events></tbody>','<tbody id=events>'+erows+'</tbody>')
    page=page.replace('<tbody id=markets></tbody>','<tbody id=markets>'+mrows+'</tbody>')
    page=page.replace('<tbody id=books></tbody>','<tbody id=books>'+brows+'</tbody>')
    page=page.replace('<div class=card><h2>Live collector feed</h2><pre id=feed>Waiting for collector events…</pre></div>','')
    return page
