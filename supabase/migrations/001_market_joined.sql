-- View base: market_joined
-- Ajuste se seus nomes/tipos diferirem. Este stub mapeia a partir da tabela unificada public.market_data

create or replace view public.market_joined as
with src as (
  select
    md.item_key,
    md.name_base,
    md.stattrak      as is_stattrak,
    md.souvenir      as is_souvenir,
    md.condition,
    -- Buff (venda e buy order)
    md.price_buff163     ::numeric as price_buff,
    md.highest_offer_buff163::numeric as best_buy_buff,
    -- Listagens aproximadas: White + CSFloat
    coalesce(md.qty_whitemarket, 0)::int as listings_white,
    coalesce(md.qty_csfloat, 0)::int    as listings_csfloat,
    -- Doppler (stub) – ajuste para uma métrica real se tiver
    0::int as doppler_buff_count
  from public.market_data md
)
select * from src;


