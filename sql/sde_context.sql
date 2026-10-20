-- Append-only, source-attributed evidence. No public client access.
create table if not exists public.sde_context_evidence (
 id uuid primary key,
 sport text not null check(sport in ('football','tennis')),
 entity_key text not null, entity_name text not null,
 event_id uuid references public.events(id),
 category text not null check(category in ('fixture','profile','appearance','availability','match_stat','collection')),
 metric text not null, value jsonb not null, unit text,
 effective_at timestamptz not null,
 published_at timestamptz, source_updated_at timestamptz,
 observed_at timestamptz not null,
 provider text not null, source_url text not null,
 metadata jsonb not null default '{}'::jsonb
);
alter table public.sde_context_evidence enable row level security;
revoke all on public.sde_context_evidence from public,anon,authenticated;
grant select,insert on public.sde_context_evidence to service_role;
create index if not exists sde_context_entity_time on public.sde_context_evidence(entity_key,effective_at desc);
create index if not exists sde_context_event on public.sde_context_evidence(event_id,category);
create index if not exists sde_context_recent on public.sde_context_evidence(sport,observed_at desc);
notify pgrst,'reload schema';
