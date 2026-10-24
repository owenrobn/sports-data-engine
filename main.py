import asyncio, json, os, uuid
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Header
from fastapi.responses import HTMLResponse, StreamingResponse, RedirectResponse
import httpx

app=FastAPI(title="Sports Data Engine",version="0.5.0")
from archive_health import install as install_archive_health
install_archive_health(app)
SUPABASE_URL=os.getenv("SUPABASE_URL","").rstrip("/")
SUPABASE_KEY=os.getenv("SUPABASE_SECRET_KEY") or os.getenv("SUPABASE_SERVICE_ROLE_KEY")
mem_events=[]; mem_jobs={}
DB_CLIENT=None

def now(): return datetime.now(timezone.utc).isoformat()
def headers():
    return {"apikey":SUPABASE_KEY,"Authorization":f"Bearer {SUPABASE_KEY}","Content-Type":"application/json","Prefer":"return=representation"}
async def db(method,table,payload=None,query="",prefer=None):
    if not (SUPABASE_URL and SUPABASE_KEY): return None
    from runtime_policy import collection_paused
    if method not in ('GET','HEAD') and collection_paused():
        raise RuntimeError('Collection paused for database disk recovery')
    identities={"football_match_events":"provider,event_id,provider_event_id","tennis_match_events":"provider,event_id,provider_event_id","prediction_market_snapshots":"ingest_key","prediction_market_trades":"ingest_key","prediction_market_books":"ingest_key","event_market_timeline":"ingest_key","backfill_checkpoints":"provider,stream,cursor_key"}
    if method=="POST" and table in identities and "on_conflict=" not in query:
        query+=("&" if query else "?")+"on_conflict="+identities[table]
    global DB_CLIENT
    if DB_CLIENT is None or DB_CLIENT.is_closed:
        DB_CLIENT=httpx.AsyncClient(timeout=25,limits=httpx.Limits(max_connections=20,max_keepalive_connections=10))
    c=DB_CLIENT
    h=headers()
    if method=="POST" and not table.startswith("rpc/"): h["Prefer"]=(prefer or "resolution=ignore-duplicates")
    if method=="POST" and not table.startswith("rpc/") and "return=" not in h["Prefer"]: h["Prefer"]="return=representation,"+h["Prefer"]
    r=await c.request(method,f"{SUPABASE_URL}/rest/v1/{table}{query}",headers=h,json=payload)
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
        from laboratory import run_engine
        app.state.engine=asyncio.create_task(run_engine(globals()))

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
            await db("POST","polymarket_market_catalog",{"market_id":mid or pid,"polymarket_event_id":pid,"condition_id":m.get("conditionId"),"question":m.get("question") or name,"slug":m.get("slug"),"sport":sport,"start_time":m.get("startDate") or item.get("startDate"),"end_time":m.get("endDate") or item.get("endDate"),"closed":bool(m.get("closed")),"resolved":bool(m.get("resolved")),"metadata":{"event_title":name,"event_slug":item.get("slug"),"outcomes":m.get("outcomes"),"clobTokenIds":m.get("clobTokenIds")}},prefer="resolution=merge-duplicates")
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
                start=None  # Provider local kickoff has no verified timezone.
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
                        if typ not in ("Shot","Own Goal Against","Own Goal For","Bad Behaviour","Foul Committed","Substitution"): continue
                        minute=x.get("minute"); second=x.get("second"); period=x.get("period")
                        rows.append({"event_id":eid,"provider":"statsbomb","provider_event_id":str(x.get("id")),"event_type":typ,"period":period,"minute":minute,"second":second,"team_name":(x.get("team") or {}).get("name"),"player_name":(x.get("player") or {}).get("name"),"xg":(x.get("shot") or {}).get("statsbomb_xg"),"payload":x})
                    for i in range(0,len(rows),200): await db("POST","football_match_events",rows[i:i+200])
                except Exception as ex: emit("warning","StatsBomb event ingest failed",match=pid,error=str(ex))
            emit("backfill","StatsBomb competition imported",competition=comp.get("competition_name"),season=comp.get("season_name"))

async def ingest_tennisvisuals_pbp():
    sid=await sport_id("tennis")
    api="https://api.github.com/repos/TennisVisuals/tennis-match-data/contents"
    async with httpx.AsyncClient(timeout=60,follow_redirects=True,headers={"User-Agent":"SDE/0.6"}) as client:
        try:
            listing=(await client.get(api)).json()
            files=[]
            for x in listing if isinstance(listing,list) else []:
                if x.get("type")=="file" and str(x.get("name","")).endswith((".csv",".csv.gz")): files.append(x)
                elif x.get("type")=="dir":
                    sub=(await client.get(x.get("url"))).json()
                    files.extend([y for y in sub if isinstance(y,dict) and y.get("type")=="file" and str(y.get("name","")).endswith((".csv",".csv.gz"))])
            files=files[:20]
            emit("backfill","TennisVisuals repository scanned",files=len(files))
            for info in files:
                raw=await client.get(info["download_url"])
                if raw.status_code!=200: continue
                import csv,io,gzip
                body=raw.content
                if info["name"].endswith(".gz"): body=gzip.decompress(body)
                rows=list(csv.DictReader(io.StringIO(body.decode("utf-8",errors="replace"))))
                await raw_ingest("tennisvisuals",info["name"],{"url":info["download_url"],"rows":len(rows)})
                for m in rows[:5000]:
                    p1=m.get("server1") or m.get("player1"); p2=m.get("server2") or m.get("player2")
                    if not p1 or not p2: continue
                    pid=f'tennisvisuals:{info["name"]}:{m.get("date")}:{p1}:{p2}'
                    mapping=await db("GET","provider_mappings",query=f"?provider=eq.tennisvisuals&entity_type=eq.event&provider_entity_id=eq.{pid}&select=canonical_id&limit=1")
                    if mapping: continue
                    starts=m.get("date")
                    if starts and len(starts)>=10: starts_at=starts[:10]+"T00:00:00+00:00"
                    else: starts_at=None
                    created=await db("POST","events",{"sport_id":sid,"name":f"{p1} vs {p2}","starts_at":starts_at,"status":"historical","metadata":{"source":"tennisvisuals","tournament":m.get("tournament"),"tour":m.get("tour"),"surface":m.get("surface"),"round":m.get("round"),"score":m.get("score"),"duration":m.get("duration")}})
                    if not created: continue
                    eid=created[0]["id"]; await db("POST","provider_mappings",{"provider":"tennisvisuals","entity_type":"event","provider_entity_id":pid,"canonical_id":eid})
                    pbp=m.get("pbp") or ""
                    point=0
                    for ch in pbp:
                        if ch not in "SRADsrad": continue
                        point+=1
                        server_won=ch.upper() in ("S","A")
                        await db("POST","tennis_match_events",{"event_id":eid,"provider":"tennisvisuals","provider_event_id":f"{pid}:{point}","event_type":"point","point_number":point,"server_name":p1,"receiver_name":p2,"surface":m.get("surface"),"payload":{"code":ch,"server_won":server_won,"ace":ch.upper()=="A","double_fault":ch.upper()=="D"}})
                emit("backfill","TennisVisuals point data imported",file=info["name"],matches=len(rows))
        except Exception as ex: emit("error","TennisVisuals import failed",error=str(ex))

async def ingest_tennis_archive():
    sid=await sport_id("tennis")
    years=[2026,2025,2024]
    tours=["atp","wta"]
    async with httpx.AsyncClient(timeout=60,follow_redirects=True) as client:
        for tour in tours:
            for year in years:
                urls=[
                    f"https://raw.githubusercontent.com/JeffSackmann/tennis_{tour}/master/{tour}_matches_{year}.csv",
                    f"https://raw.githubusercontent.com/JeffSackmann/tennis_{tour}/master/{tour}_matches_qual_chall_{year}.csv"
                ]
                for url in urls:
                    try:
                        response=await client.get(url)
                        if response.status_code!=200: continue
                        import csv,io
                        rows=list(csv.DictReader(io.StringIO(response.text)))
                        await raw_ingest("tennis_archive",f"{tour}:{year}:{url.split('/')[-1]}",{"url":url,"rows":len(rows),"license_note":"provider/archive terms apply"})
                        for m in rows[:3000]:
                            p1=m.get("winner_name"); p2=m.get("loser_name")
                            if not p1 or not p2: continue
                            date=m.get("tourney_date") or ""
                            starts_at=None
                            if len(date)==8: starts_at=f"{date[:4]}-{date[4:6]}-{date[6:8]}T00:00:00+00:00"
                            pid=f'{tour}:{m.get("tourney_id")}:{m.get("match_num")}:{p1}:{p2}'
                            mapping=await db("GET","provider_mappings",query=f"?provider=eq.tennis_archive&entity_type=eq.event&provider_entity_id=eq.{pid}&select=canonical_id&limit=1")
                            if mapping: continue
                            created=await db("POST","events",{"sport_id":sid,"name":f"{p1} vs {p2}","starts_at":starts_at,"status":"historical","metadata":{"source":"tennis_archive","tour":tour,"tournament":m.get("tourney_name"),"surface":m.get("surface"),"round":m.get("round"),"score":m.get("score"),"winner":p1,"loser":p2,"winner_rank":m.get("winner_rank"),"loser_rank":m.get("loser_rank"),"best_of":m.get("best_of")}})
                            if not created: continue
                            eid=created[0]["id"]
                            await db("POST","provider_mappings",{"provider":"tennis_archive","entity_type":"event","provider_entity_id":pid,"canonical_id":eid})
                            await db("POST","tennis_match_events",{"event_id":eid,"provider":"tennis_archive","provider_event_id":pid,"event_type":"match_result","surface":m.get("surface"),"effective_at":starts_at,"payload":{"score":m.get("score"),"winner":p1,"loser":p2,"w_ace":m.get("w_ace"),"l_ace":m.get("l_ace"),"w_df":m.get("w_df"),"l_df":m.get("l_df"),"w_bpSaved":m.get("w_bpSaved"),"w_bpFaced":m.get("w_bpFaced"),"l_bpSaved":m.get("l_bpSaved"),"l_bpFaced":m.get("l_bpFaced")}})
                        emit("backfill","Tennis archive imported",tour=tour,year=year,rows=len(rows))
                    except Exception as ex: emit("warning","Tennis archive import failed",tour=tour,year=year,error=str(ex))
                    await asyncio.sleep(2)

async def build_event_market_timeline():
    while True:
        try:
            links=await db("GET","market_match_links",query="?select=event_id,market_id,condition_id&limit=500") or []
            for link in links:
                eid=link["event_id"]; mid=link["market_id"]
                prices=await db("GET","prediction_market_snapshots",query=f"?event_id=eq.{eid}&market_id=eq.{mid}&select=market_id,condition_id,token_id,outcome,price,best_bid,best_ask,observed_at,effective_at&order=observed_at.asc&limit=5000") or []
                for p in prices:
                    ts=p.get("effective_at") or p.get("observed_at")
                    row={"event_id":eid,"sport":"unknown","effective_at":ts,"state":{},"market_id":mid,"condition_id":p.get("condition_id"),"token_id":p.get("token_id"),"outcome":p.get("outcome"),"market_price":p.get("price"),"best_bid":p.get("best_bid"),"best_ask":p.get("best_ask"),"source":"polymarket"}
                    await db("POST","event_market_timeline",row)
            emit("timeline","Event-market timeline pass complete",links=len(links))
        except Exception as ex: emit("error","Timeline join failed",error=str(ex))
        await asyncio.sleep(1800)

def name_tokens(text):
    import re
    stop={"vs","v","will","win","match","the","at","open","men","women"}
    return {x for x in re.findall(r"[a-z0-9]+",str(text).lower()) if len(x)>2 and x not in stop}

async def link_sports_markets():
    while True:
        try:
            sports=await db("GET","sports",query="?select=id,slug") or []
            smap={x["id"]:x["slug"] for x in sports}
            events=await db("GET","events",query="?select=id,sport_id,name,starts_at,metadata&order=updated_at.desc&limit=500") or []
            catalog=await db("GET","polymarket_market_catalog",query="?select=market_id,polymarket_event_id,condition_id,question,sport,start_time&order=observed_at.desc&limit=3000") or []
            markets={x["market_id"]:{"market_id":x["market_id"],"condition_id":x.get("condition_id"),"title":x.get("question") or "","pm_event_id":x.get("polymarket_event_id"),"sport":x.get("sport"),"start_time":x.get("start_time")} for x in catalog}
            for ev in events:
                sport=smap.get(ev.get("sport_id"))
                if sport not in ("football","tennis"): continue
                et=name_tokens(ev.get("name")); 
                if len(et)<2: continue
                best=None
                for m in markets.values():
                    if m.get("sport") and m.get("sport")!=sport: continue
                    mt=name_tokens(m["title"]); inter=len(et & mt); union=len(et | mt) or 1
                    sim=inter/union
                    if inter>=2 and (best is None or sim>best[0]): best=(sim,m)
                if not best: continue
                sim,m=best; conf=min(0.99,0.55+sim*0.44)
                payload={"event_id":ev["id"],"polymarket_event_id":str(m.get("pm_event_id") or ""),"market_id":m["market_id"],"candidate_title":m["title"],"sport":sport,"event_start":ev.get("starts_at"),"name_similarity":sim,"confidence":conf,"status":"accepted" if conf>=0.78 else "candidate","metadata":{"method":"token_overlap_v1"}}
                existing=await db("GET","market_link_candidates",query=f'?event_id=eq.{ev["id"]}&market_id=eq.{m["market_id"]}&select=id&limit=1')
                if not existing: await db("POST","market_link_candidates",payload)
                if conf>=0.78:
                    linked=await db("GET","market_match_links",query=f'?event_id=eq.{ev["id"]}&market_id=eq.{m["market_id"]}&select=id&limit=1')
                    if not linked: await db("POST","market_match_links",{"event_id":ev["id"],"market_id":m["market_id"],"condition_id":m.get("condition_id"),"market_question":m["title"],"market_type":"sports","confidence":conf,"method":"token_overlap_v1","metadata":{"sport":sport}})
            emit("linker","Sports market matching pass complete",events=len(events),markets=len(markets))
        except Exception as ex: emit("error","Sports market linker failed",error=str(ex))
        await asyncio.sleep(900)

async def backfill_polymarket_catalog():
    offset=0
    async with httpx.AsyncClient(timeout=60) as client:
        while True:
            try:
                r=await client.get("https://gamma-api.polymarket.com/events",params={"closed":"true","limit":100,"offset":offset})
                items=r.json() if r.status_code==200 else []
                if not isinstance(items,list) or not items: break
                kept=0
                for item in items:
                    sport=classify_sport(item)
                    if sport not in ("football","tennis"): continue
                    kept+=1; pid=str(item.get("id") or item.get("slug") or "")
                    name=item.get("title") or item.get("question") or item.get("slug") or pid
                    for m in item.get("markets") or []:
                        mid=str(m.get("id") or m.get("conditionId") or "")
                        if not mid: continue
                        await db("POST","polymarket_market_catalog",{"market_id":mid,"polymarket_event_id":pid,"condition_id":m.get("conditionId"),"question":m.get("question") or name,"slug":m.get("slug"),"sport":sport,"start_time":m.get("startDate") or item.get("startDate"),"end_time":m.get("endDate") or item.get("endDate"),"closed":bool(m.get("closed")),"resolved":bool(m.get("resolved")),"metadata":{"event_title":name,"event_slug":item.get("slug"),"outcomes":m.get("outcomes"),"clobTokenIds":m.get("clobTokenIds")}},prefer="resolution=merge-duplicates")
                emit("backfill","Polymarket football+tennis catalog page",offset=offset,kept=kept)
                offset+=100
                if len(items)<100: break
                await asyncio.sleep(1)
            except Exception as ex:
                emit("error","Polymarket catalog backfill failed",offset=offset,error=str(ex)); break

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
    if SUPABASE_KEY: await db("POST","raw_ingestions",row,prefer="return=minimal")

@app.post("/jobs/discover/{provider}/{sport}/{date}")
async def discover(provider:str,sport:str,date:str,x_sde_admin_token:str=Header(default="")):
    import secrets
    expected=os.getenv("SDE_ADMIN_TOKEN","")
    if not expected or not secrets.compare_digest(x_sde_admin_token,expected): raise HTTPException(403,"Collector control requires administrator authentication")
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

from laboratory import install
install(globals())




from context_feed import install as install_context
install_context(globals())

from rae_requirements import install as install_rae
install_rae(globals())

from live_state import install as install_live
install_live(globals())

from source_enrichment import install as install_sources
install_sources(globals())
from market_capture import install as install_captures
install_captures(globals())
from bookmaker_feed import install as install_bookmakers
install_bookmakers(globals())
from engine_delivery import install as install_delivery
install_delivery(globals())

from ledger_audit import install as install_ledger
install_ledger(globals())

@app.on_event('shutdown')
async def close_shared_clients():
    if DB_CLIENT is not None: await DB_CLIENT.aclose()
    from context_feed import close_source_client
    await close_source_client()

