-- Minimal audit schema (v1). Optional until we start writing trades/events.

create extension if not exists "uuid-ossp";

create table if not exists trades (
  id uuid primary key default uuid_generate_v4(),
  created_at timestamptz not null default now(),
  wallet text not null,
  signature text not null,
  side text not null, -- buy/sell
  mint text not null,
  notional_sol double precision not null,
  status text not null, -- submitted/confirmed/failed
  error text
);

create index if not exists trades_created_at_idx on trades (created_at desc);
create index if not exists trades_wallet_idx on trades (wallet);
create index if not exists trades_mint_idx on trades (mint);
create unique index if not exists trades_signature_unique on trades (signature);

create table if not exists positions (
  id uuid primary key default uuid_generate_v4(),
  opened_at timestamptz not null default now(),
  closed_at timestamptz,
  wallet text not null,
  mint text not null,
  buy_signature text not null,
  sell_signature text,
  size_sol double precision not null,
  token_amount text not null,
  realized_pnl_sol double precision
);

create index if not exists positions_opened_at_idx on positions (opened_at desc);
create index if not exists positions_wallet_idx on positions (wallet);
create index if not exists positions_mint_idx on positions (mint);

create table if not exists errors (
  id uuid primary key default uuid_generate_v4(),
  created_at timestamptz not null default now(),
  component text not null,
  message text not null,
  context jsonb
);

create index if not exists errors_created_at_idx on errors (created_at desc);
create index if not exists errors_component_idx on errors (component);


