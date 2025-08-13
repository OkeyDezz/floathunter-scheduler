# FloatHunter Scheduler

Serviço dedicado à atualização das tabelas `market_data` e `liquidity`.

- Executa `scheduler_refresh.py --once` por execução.
- Compartilha as mesmas variáveis do backend de licenças:
  - `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE` (ou `SUPABASE_SERVICE_KEY`), `SUPABASE_ANON_KEY`
  - `SUPABASE_MARKET_TABLE`
  - `REFRESH_INTERVAL_SECONDS` (apenas se utilizar em loop)

No Railway, use um “Restart Schedule” de 6h para reexecutar automaticamente.
