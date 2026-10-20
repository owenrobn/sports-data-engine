
-- Preserve duplicate rows for audit before enforcing stable provider identities.
create table if not exists public.sde_duplicate_archive (
 table_name text not null, row_id bigint not null, archived_at timestamptz not null default now(),
 row_data jsonb not null, primary key(table_name,row_id)
);
alter table public.sde_duplicate_archive enable row level security;
revoke all on public.sde_duplicate_archive from anon, authenticated;
grant all on public.sde_duplicate_archive to service_role;
alter table public.prediction_market_snapshots add column if not exists ingest_key text;
alter table public.prediction_market_trades add column if not exists ingest_key text;
alter table public.prediction_market_books add column if not exists ingest_key text;
alter table public.event_market_timeline add column if not exists ingest_key text;
alter table public.event_market_timeline add column if not exists observed_at timestamptz;
create unique index if not exists sde_candidate_identity on public.market_link_candidates(event_id,market_id);
create unique index if not exists sde_link_identity on public.market_match_links(event_id,market_id);
create index if not exists sde_price_market_time on public.prediction_market_snapshots(market_id,effective_at);
create index if not exists sde_football_event on public.football_match_events(event_id);
create index if not exists sde_tennis_event on public.tennis_match_events(event_id);
create index if not exists sde_trade_market on public.prediction_market_trades(market_id);
create index if not exists sde_book_market on public.prediction_market_books(market_id);
-- Date-only archives do not establish an actual match start or result time.
update public.events set metadata=metadata||jsonb_build_object(
 'source_date',starts_at::date,'time_precision',case when metadata->>'source'='tennis_archive' then 'tournament_date' else 'date' end),
 starts_at=null where metadata->>'source' in ('tennis_archive','tennisvisuals') and starts_at is not null;
update public.tennis_match_events set effective_at=null where provider='tennis_archive';
-- Old doubles parsing discarded partners and serving rotation. Quarantine unsupported states.
update public.tennis_match_events set server_name=null,receiver_name=null,
 payload=payload||'{"legacy_parser_incomplete":true,"server_state_unavailable":true}'::jsonb where provider='tennisvisuals';
update public.events set metadata=metadata||'{"legacy_parser_incomplete":true,"discipline":"doubles","participant_names_incomplete":true}'::jsonb
 where id in(select canonical_id from public.provider_mappings where provider='tennisvisuals' and provider_entity_id like '%Doubles%');
create table if not exists public.sde_provider_runs(
 id uuid primary key default gen_random_uuid(),provider text not null,sport text not null,
 started_at timestamptz not null default now(),finished_at timestamptz,status text not null default 'running',
 records integer not null default 0,error text,details jsonb not null default '{}'::jsonb
);
alter table public.sde_provider_runs enable row level security;
revoke all on public.sde_provider_runs from anon,authenticated;
grant all on public.sde_provider_runs to service_role;
create index if not exists sde_run_health on public.sde_provider_runs(sport,provider,started_at desc);
create table if not exists public.sde_coverage_universes(
 provider text not null,sport text not null,universe_key text not null,
 expected_matches integer not null,imported_matches integer not null default 0,
 description text not null,observed_at timestamptz not null default now(),
 primary key(provider,sport,universe_key)
);
alter table public.sde_coverage_universes enable row level security;
revoke all on public.sde_coverage_universes from anon,authenticated;
grant all on public.sde_coverage_universes to service_role;
create or replace view public.sde_sport_coverage with(security_invoker=true) as
select s.slug sport,
 count(e.id) filter(where e.metadata->>'source' is distinct from 'polymarket') canonical_matches,
 count(e.id) filter(where e.metadata->>'source' is distinct from 'polymarket' and exists(select 1 from public.football_match_events f where f.event_id=e.id) or e.metadata->>'source' is distinct from 'polymarket' and exists(select 1 from public.tennis_match_events t where t.event_id=e.id)) matches_with_states,
 count(e.id) filter(where e.metadata->>'source' is distinct from 'polymarket' and exists(select 1 from public.market_match_links l where l.event_id=e.id)) linked_matches,
 count(e.id) filter(where e.metadata->>'source' is distinct from 'polymarket' and exists(select 1 from public.event_market_timeline l where l.event_id=e.id and l.market_price is not null)) matches_with_prices,
 (select count(*) from public.polymarket_market_catalog c where c.sport=s.slug) catalog_markets,
 (select sum(expected_matches) from public.sde_coverage_universes u where u.sport=s.slug) target_matches,
 (select sum(imported_matches) from public.sde_coverage_universes u where u.sport=s.slug) imported_target_matches
from public.sports s left join public.events e on e.sport_id=s.id
where s.slug in ('football','tennis') group by s.slug;
revoke all on public.sde_sport_coverage from anon,authenticated;
grant select on public.sde_sport_coverage to service_role;

create or replace function public.sde_row_identity(t text,r jsonb) returns text language sql immutable set search_path='' as $$
 select md5((case
 when t='prediction_market_snapshots' then jsonb_build_array(r->'venue',r->'market_id',coalesce(nullif(r->>'token_id',''),r->'payload'->>'token_id',r->>'outcome'),coalesce(r->>'effective_at',r->>'observed_at'),r->'price')
 when t='prediction_market_trades' then jsonb_build_array(r->'venue',r->'market_id',r->'token_id',r->'trade_id',r->'trader',r->'side',r->'outcome',r->'price',r->'size',r->'executed_at')
 when t='prediction_market_books' then jsonb_build_array(r->'venue',r->'market_id',r->'token_id',r->'observed_at',r->'bids',r->'asks')
 else jsonb_build_array(r->'event_id',r->'market_id',r->'token_id',r->'effective_at',r->'source') end)::text)
$$;
revoke all on function public.sde_row_identity(text,jsonb) from public;
grant execute on function public.sde_row_identity(text,jsonb) to service_role;
create or replace function public.sde_set_identity() returns trigger language plpgsql set search_path='' as $$
begin new.ingest_key:=public.sde_row_identity(tg_table_name,to_jsonb(new)); return new; end $$;
revoke all on function public.sde_set_identity() from public;
do $$declare t text; begin
foreach t in array array['prediction_market_snapshots','prediction_market_trades','prediction_market_books','event_market_timeline'] loop
execute format('create unique index if not exists %I on public.%I(ingest_key)',t||'_sde_identity',t);
execute format('drop trigger if exists sde_identity on public.%I',t);
execute format('create trigger sde_identity before insert on public.%I for each row execute function public.sde_set_identity()',t);
end loop; end $$;
-- Small bounded repair transactions; duplicate payloads retained in the audit table.
create or replace function public.sde_repair_batch(t text) returns integer language plpgsql set search_path='' as $$
declare n integer;
begin
 if t not in ('prediction_market_snapshots','prediction_market_trades','prediction_market_books','event_market_timeline') then raise exception 'unsupported table'; end if;
 perform pg_advisory_xact_lock(hashtext('sde_repair_'||t));
 create temporary table if not exists sde_batch(id bigint,k text,r jsonb,duplicate boolean) on commit drop;
 truncate pg_temp.sde_batch;
 execute format('insert into pg_temp.sde_batch(id,k,r)
 select id,public.sde_row_identity(%L,to_jsonb(x)),to_jsonb(x) from public.%I x where ingest_key is null order by id limit 1000',t,t);
 get diagnostics n=row_count;
 execute format('update pg_temp.sde_batch b set duplicate=exists(select 1 from public.%I x where x.ingest_key=b.k and x.id<>b.id) or exists(select 1 from pg_temp.sde_batch a where a.k=b.k and a.id<b.id)',t);
 insert into public.sde_duplicate_archive(table_name,row_id,row_data) select t,id,r from pg_temp.sde_batch where duplicate on conflict do nothing;
 execute format('delete from public.%I x using pg_temp.sde_batch b where x.id=b.id and b.duplicate',t);
 execute format('update public.%I x set ingest_key=b.k from pg_temp.sde_batch b where x.id=b.id and not b.duplicate',t);
 return n;
end $$;
revoke all on function public.sde_repair_batch(text) from public;
grant execute on function public.sde_repair_batch(text) to service_role;
create index if not exists sde_pending_price_repair on public.prediction_market_snapshots(id) where ingest_key is null;
create index if not exists sde_pending_trade_repair on public.prediction_market_trades(id) where ingest_key is null;
