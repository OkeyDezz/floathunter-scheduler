#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import time
import argparse
from datetime import datetime

from dotenv import load_dotenv

# Carrega .env local
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=ENV_PATH)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE") or os.environ.get("SUPABASE_ANON_KEY")
MARKET_TABLE = os.environ.get("SUPABASE_MARKET_TABLE", "market_data")
INTERVAL_SECONDS = int(os.environ.get("REFRESH_INTERVAL_SECONDS", str(3 * 60 * 60)))  # 3h default

os.environ.setdefault("HTTPX_DISABLE_HTTP2", "1")

def get_sb():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL/SUPABASE_SERVICE_ROLE não configurados no .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def clean_market_table(sb):
    print("[clean] Apagando dados antigos de", MARKET_TABLE)
    # Remoção ampla – evita WHERE vazio proibido
    sb.table(MARKET_TABLE).delete().neq("item_key", "__never__").execute()


def refresh_sources():
    # Import on demand para evitar custos quando scheduler inicia
    import whitemarket_fetcher as wm
    import csfloat_fetcher as cf
    import buff163_fetcher as bf

    total = 0
    print("[run] Whitemarket...")
    total += wm.run_whitemarket_ingest()
    print("[run] CSFloat...")
    total += cf.run_csfloat_ingest()
    print("[run] Buff163...")
    total += bf.run_buff163_ingest()
    print(f"[run] Total upserts: {total}")
    return total


def refresh_liquidity(sb):
    print("[liquidity] Atualizando MV...")
    # Chama função SQL com SECURITY DEFINER
    res = sb.rpc("refresh_liquidity_mv").execute()
    if getattr(res, "error", None):
        raise RuntimeError(f"refresh_liquidity_mv error: {res.error}")
    print("[liquidity] OK")


def main():
    parser = argparse.ArgumentParser(description="Scheduler de coleta e liquidez (3h loop)")
    parser.add_argument("--once", action="store_true", help="Executa apenas uma vez e sai")
    parser.add_argument("--clean", action="store_true", help="Limpa a tabela unificada antes de recarregar")
    args = parser.parse_args()

    while True:
        start = datetime.utcnow().isoformat()
        print(f"\n===== REFRESH RUN @ {start}Z =====")
        sb = get_sb()
        if args.clean:
            clean_market_table(sb)
        total = refresh_sources()
        refresh_liquidity(sb)
        print(f"===== DONE (rows touched: {total}) =====\n")

        if args.once:
            break
        # Dorme intervalo (3h por padrão)
        time.sleep(INTERVAL_SECONDS)


if __name__ == "__main__":
    main()


