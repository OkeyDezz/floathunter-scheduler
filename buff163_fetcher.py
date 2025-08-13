#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import typing as t
from datetime import datetime, timezone

import requests
import ijson
from dotenv import load_dotenv
import gzip
import io

BUFF163_URL = "https://prices.csgotrader.app/latest/buff163.json"
os.environ.setdefault("HTTPX_DISABLE_HTTP2", "1")

# Load .env next to this file
ENV_PATH = os.path.join(os.path.dirname(__file__), ".env")
load_dotenv(dotenv_path=ENV_PATH)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE") or os.environ.get("SUPABASE_ANON_KEY")
MARKET_TABLE = os.environ.get("SUPABASE_MARKET_TABLE", "market_data")
UPSERT_BATCH = int(os.environ.get("SUPABASE_UPSERT_BATCH", "500"))

CONDITION_NAMES = [
    "Factory New",
    "Minimal Wear",
    "Field-Tested",
    "Well-Worn",
    "Battle-Scarred",
]

PHASE_TOKENS = [
    "Ruby", "Sapphire", "Black Pearl", "Emerald",
    "Phase 1", "Phase 2", "Phase 3", "Phase 4",
]


def parse_market_hash_name(name: str) -> t.Tuple[str, bool, bool, t.Optional[str]]:
    if not name:
        return "", False, False, None
    s = name
    stattrak = "StatTrak" in s or "StatTrak™" in s
    souvenir = "Souvenir" in s
    condition = None
    for cond in CONDITION_NAMES:
        if s.endswith(f"({cond})"):
            condition = cond
            s = s[: -(len(cond) + 2)].strip()
            break
    base = s.replace("StatTrak™ ", "").replace("StatTrak ", "").replace("Souvenir ", "").strip()
    return base, stattrak, souvenir, condition


def detect_phase(name: str) -> t.Optional[str]:
    for token in PHASE_TOKENS:
        # basic contains check (case sensitive to match CSGO naming)
        if token in name:
            return token
    return None


def build_item_key(name_base: str, stattrak: bool, souvenir: bool, condition: t.Optional[str], phase: t.Optional[str]) -> str:
    parts = [
        name_base or "",
        ("StatTrak" if stattrak else ""),
        ("Souvenir" if souvenir else ""),
        condition or "",
        phase or "",
    ]
    return "|".join([p for p in parts if p]).strip()


def chunked(iterable, size: int):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def get_supabase_client():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL/SUPABASE_SERVICE_ROLE não configurados no ambiente")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


class PrependStream:
    def __init__(self, head: bytes, base):
        self.buf = io.BytesIO(head)
        self.base = base
    def read(self, n: int = -1):
        b = self.buf.read(n)
        if n == -1 or len(b) == n:
            return b
        rest = self.base.read(n - len(b))
        return b + rest


def open_source_stream(url: str):
    headers = {"Accept": "application/json"}
    resp = requests.get(url, headers=headers, stream=True, timeout=180)
    resp.raise_for_status()
    resp.raw.decode_content = True
    head = resp.raw.read(4)
    base = PrependStream(head, resp.raw)
    if head.startswith(b"\x1f\x8b"):
        return gzip.GzipFile(fileobj=base, mode="rb")
    return base


def fetch_buff163(url: str = BUFF163_URL) -> t.Iterable[t.Tuple[str, dict]]:
    """Stream top-level mapping of name -> {starting_at: {price}, highest_order: {price}}"""
    stream = open_source_stream(url)
    # kvitems with empty prefix to iterate top-level keys
    for name, entry in ijson.kvitems(stream, ""):
        yield str(name), entry if isinstance(entry, dict) else {}


def aggregate_buff163(pairs: t.Iterable[t.Tuple[str, dict]]) -> t.Dict[str, dict]:
    now = datetime.now(timezone.utc)
    acc: t.Dict[str, dict] = {}
    for name, entry in pairs:
        start_obj = entry.get("starting_at") or entry.get("startingAt") or {}
        buy_obj = entry.get("highest_order") or entry.get("highets_offer") or entry.get("highestOrder") or {}
        start_price = start_obj.get("price")
        buy_price = buy_obj.get("price")
        try:
            p_start = float(start_price) if start_price is not None else None
        except Exception:
            p_start = None
        try:
            p_buy = float(buy_price) if buy_price is not None else None
        except Exception:
            p_buy = None

        name_base, stattrak, souvenir, condition = parse_market_hash_name(name)
        phase = detect_phase(name)
        item_key = build_item_key(name_base, stattrak, souvenir, condition, phase)

        # Aggregate duplicates: take min(start), max(buy)
        rec = acc.get(item_key)
        if not rec:
            acc[item_key] = rec = {
                "item_key": item_key,
                "name_base": name_base,
                "stattrak": stattrak,
                "souvenir": souvenir,
                "condition": condition,
                "phase": phase,
                "price_buff163": p_start,
                "highest_offer_buff163": p_buy,
                "fetched_at": now,
            }
        else:
            if p_start is not None:
                if rec.get("price_buff163") is None or p_start < rec["price_buff163"]:
                    rec["price_buff163"] = p_start
            if p_buy is not None:
                if rec.get("highest_offer_buff163") is None or p_buy > rec["highest_offer_buff163"]:
                    rec["highest_offer_buff163"] = p_buy
    return acc


def upsert_market_rows(sb, rows: list[dict]):
    for batch in chunked(rows, UPSERT_BATCH):
        sb.table(MARKET_TABLE).upsert(batch, on_conflict="item_key").execute()


def run_buff163_ingest(url: str = BUFF163_URL) -> int:
    sb = get_supabase_client()
    pairs = fetch_buff163(url)
    aggregated = aggregate_buff163(pairs)
    rows = []
    for _, rec in aggregated.items():
        rows.append({
            "item_key": rec["item_key"],
            "name_base": rec["name_base"],
            "stattrak": bool(rec["stattrak"]),
            "souvenir": bool(rec["souvenir"]),
            "condition": rec["condition"],
            "price_buff163": rec["price_buff163"],
            "highest_offer_buff163": rec["highest_offer_buff163"],
            "fetched_at": rec["fetched_at"].isoformat(),
        })
    if rows:
        upsert_market_rows(sb, rows)
    return len(rows)


if __name__ == "__main__":
    count = run_buff163_ingest()
    print(f"[buff163] itens agregados: {count}")


