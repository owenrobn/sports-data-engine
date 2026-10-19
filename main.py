import asyncio, json, os, time, uuid
from collections import deque
from datetime import datetime, timezone
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse
import httpx

app=FastAPI(title="Sports Data Engine",version="0.3.0")
events=deque(maxlen=1000)
jobs={}

def now(): return datetime.now(timezone.utc).isoformat()
def emit(kind,message,**data):
    events.appendleft({"ts":now(),"kind":kind,"message":message,**data})

@app.on_event("startup")
async def startup(): emit("system","SDE v0.3 online")

@app.get("/health")
async def health(): return {"ok":True,"service":"sports-data-engine","version":"0.3.0","time":now()}

@app.get("/status")
async def status():
    return {"ok":True,"version":"0.3.0","jobs":len(jobs),"events_buffered":len(events),"supabase_configured":bool(os.getenv("SUPABASE_URL"))}

@app.get("/providers")
async def providers():
    return {"providers":["sofascore","statsbomb","thesportsdb","football-data","polymarket"],"sports":["football","tennis"]}

@app.get("/progress")
async def progress(): return list(events)[:100]

@app.get("/jobs")
async def list_jobs(): return sorted(jobs.values(),key=lambda x:x["created_at"],reverse=True)

@app.get("/progress/stream")
async def stream():
    async def gen():
        seen=None
        while True:
            if events and events[0]["ts"]!=seen:
                seen=events[0]["ts"]
                yield "data: "+json.dumps(events[0])+"\n\n"
            else: yield ": keepalive\n\n"
            await asyncio.sleep(2)
    return StreamingResponse(gen(),media_type="text/event-stream")

@app.post("/jobs/discover/{provider}/{sport}/{date}")
async def discover(provider:str,sport:str,date:str):
    jid=str(uuid.uuid4()); job={"id":jid,"type":"discover","provider":provider,"sport":sport,"date":date,"status":"queued","progress":0,"created_at":now()}; jobs[jid]=job
    asyncio.create_task(run_discovery(jid)); return {"job_id":jid}

async def run_discovery(jid):
    j=jobs[jid]; j["status"]="running"; j["progress"]=10; emit("job","Discovery started",job_id=jid,provider=j["provider"],sport=j["sport"])
    try:
        if j["provider"]=="sofascore":
            url=f"https://www.sofascore.com/api/v1/sport/{j['sport']}/scheduled-events/{j['date']}"
        elif j["provider"]=="polymarket":
            url="https://gamma-api.polymarket.com/events?active=true&closed=false&limit=100"
        else: raise ValueError("provider discovery adapter not enabled on cloud yet")
        async with httpx.AsyncClient(timeout=25) as c:
            r=await c.get(url); r.raise_for_status(); data=r.json()
        j["status"]="completed"; j["progress"]=100; j["result_summary"]={"items":len(data.get("events",data)) if isinstance(data,(dict,list)) else None}; emit("job","Discovery completed",job_id=jid)
    except Exception as e:
        j["status"]="failed"; j["error"]=str(e); emit("error","Discovery failed",job_id=jid,error=str(e))

DASH="""<!doctype html><html><head><meta charset=utf-8><meta name=viewport content="width=device-width"><title>SDE Live</title><style>body{font-family:system-ui;background:#0b1020;color:#e8eefc;margin:0;padding:24px}.card{background:#131b31;border:1px solid #263250;border-radius:14px;padding:18px;margin:12px 0}.ok{color:#62e6a7}pre{white-space:pre-wrap;word-break:break-word;color:#b9c7e8}h1{margin-bottom:4px}</style></head><body><h1>Sports Data Engine</h1><p class=ok>● LIVE</p><div class=card><h3>System</h3><pre id=s>loading…</pre></div><div class=card><h3>Jobs</h3><pre id=j>loading…</pre></div><div class=card><h3>Live feed</h3><pre id=f></pre></div><script>async function r(){s.textContent=JSON.stringify(await(await fetch('/status')).json(),null,2);j.textContent=JSON.stringify(await(await fetch('/jobs')).json(),null,2)}r();setInterval(r,5000);let es=new EventSource('/progress/stream');es.onmessage=e=>{f.textContent=e.data+'\n'+f.textContent.slice(0,12000)}</script></body></html>"""

@app.get("/dashboard",response_class=HTMLResponse)
async def dashboard(): return DASH
