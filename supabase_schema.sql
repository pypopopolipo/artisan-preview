-- =============================================
-- ARTISAN DATA — Supabase Schema
-- =============================================

-- 1. Table clients (les acheteurs : Rémi, Julia, etc.)
create table clients (
  id uuid default gen_random_uuid() primary key,
  nom text not null,
  email text unique not null,
  departements text[] not null,        -- ex: {'30', '84'}
  code_acces text unique not null,     -- code unique pour login
  created_at timestamptz default now()
);

-- 2. Table fiches (les artisans, liées à un client)
create table fiches (
  id bigint generated always as identity primary key,
  client_id uuid references clients(id) on delete cascade,
  siret text,
  nom_entreprise text,
  forme_juridique text,
  code_ape text,
  activite_naf text,
  date_creation text,
  telephone text,
  email_generique text,
  site_web text,
  website_emails text,
  website_mobiles text,
  website_fixes text,
  dirigeant_nom_complet text,
  dirigeant_prenom text,
  dirigeant_nom text,
  dirigeant_telephone text,
  email_dirigeant text,
  linkedin_fonction text,
  linkedin_profil text,
  adresse text,
  code_postal text,
  ville text,
  specialite text,
  activites_principales text,
  activites_secondaires text,
  is_rge text,
  qualibat text,
  qualibat_detail text,
  qualienr text,
  certifs_enr text,
  qualifelec text,
  assurance_rc text,
  assurance_dc text,
  assureur text,
  chiffre_affaires text,
  effectif text,
  tranche_effectif text,
  categorie_entreprise text,
  solvabilite text,
  sources text,
  nb_sources integer,
  score_completude real
);

-- 3. Table markers (suivi "contacté" par le client)
create table markers (
  id bigint generated always as identity primary key,
  client_id uuid references clients(id) on delete cascade,
  fiche_id bigint references fiches(id) on delete cascade,
  contacted boolean default false,
  notes text default '',
  updated_at timestamptz default now(),
  unique(client_id, fiche_id)
);

-- 4. Index pour la perf
create index idx_fiches_client on fiches(client_id);
create index idx_fiches_cp on fiches(code_postal);
create index idx_markers_client on markers(client_id);

-- 5. Row Level Security (chaque client voit QUE ses données)
alter table fiches enable row level security;
alter table markers enable row level security;

-- Policy : lecture fiches selon le code_acces passé en header
create or replace function get_current_client_id()
returns uuid as $$
  select id from clients where code_acces = current_setting('request.headers', true)::json->>'x-client-code'
$$ language sql security definer;

create policy "Clients see own fiches"
  on fiches for select
  using (client_id = get_current_client_id());

create policy "Clients see own markers"
  on markers for select
  using (client_id = get_current_client_id());

create policy "Clients can insert markers"
  on markers for insert
  with check (client_id = get_current_client_id());

create policy "Clients can update own markers"
  on markers for update
  using (client_id = get_current_client_id());
