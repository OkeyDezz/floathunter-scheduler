#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import typing as t
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
import ijson

CSFLOAT_URL = "https://csfloat.com/api/v1/listings/price-list"
os.environ.setdefault("HTTPX_DISABLE_HTTP2", "1")

# Load .env beside this file
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


def build_item_key(name_base: str, stattrak: bool, souvenir: bool, condition: t.Optional[str], phase: t.Optional[str]) -> str:
    parts = [
        name_base or "",
        ("StatTrak" if stattrak else ""),
        ("Souvenir" if souvenir else ""),
        condition or "",
        phase or "",
    ]
    return "|".join([p for p in parts if p != ""]).strip()


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


import gzip
import io


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


def fetch_csfloat(url: str = CSFLOAT_URL) -> t.Iterable[dict]:
    # Try stream as JSON array
    stream = open_source_stream(url)
    try:
        for obj in ijson.items(stream, "item"):
            if isinstance(obj, dict):
                yield obj
        return
    except Exception:
        pass
    # Fallback: NDJSON per line
    stream = open_source_stream(url)
    for line in stream:
        try:
            import json
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
        except Exception:
            continue


def aggregate_csfloat(items: t.Iterable[dict]) -> t.Dict[str, dict]:
    now = datetime.now(timezone.utc)
    acc: t.Dict[str, dict] = {}
    for it in items:
        name = it.get("market_hash_name") or ""
        qty = it.get("qty") or 0
        min_price = it.get("min_price")
        try:
            qty = int(qty)
        except Exception:
            qty = 0
        try:
            # csfloat min_price pode vir em cents. Se for inteiro grande, converte para USD
            if isinstance(min_price, (int,)):
                price = float(min_price) / 100.0
            else:
                price = float(min_price) if min_price is not None else None
        except Exception:
            price = None
        name_base, stattrak, souvenir, condition = parse_market_hash_name(str(name))
        item_key = build_item_key(name_base, stattrak, souvenir, condition, None)
        rec = acc.get(item_key)
        if not rec:
            acc[item_key] = rec = {
                "item_key": item_key,
                "name_base": name_base,
                "stattrak": stattrak,
                "souvenir": souvenir,
                "condition": condition,
                "phase": None,
                "price_csfloat": price,
                "qty_csfloat": qty,
                "fetched_at": now,
            }
        else:
            # Aggregate duplicates: sum qty, keep min price
            rec["qty_csfloat"] = int(rec.get("qty_csfloat", 0)) + qty
            if price is not None:
                if rec.get("price_csfloat") is None or price < rec["price_csfloat"]:
                    rec["price_csfloat"] = price
    return acc


def upsert_market_rows(sb, rows: list[dict]):
    for batch in chunked(rows, UPSERT_BATCH):
        sb.table(MARKET_TABLE).upsert(batch, on_conflict="item_key").execute()


def run_csfloat_ingest(url: str = CSFLOAT_URL) -> int:
    sb = get_supabase_client()
    items = fetch_csfloat(url)
    aggregated = aggregate_csfloat(items)
    rows = []
    for _, rec in aggregated.items():
        rows.append({
            "item_key": rec["item_key"],
            "name_base": rec["name_base"],
            "stattrak": bool(rec["stattrak"]),
            "souvenir": bool(rec["souvenir"]),
            "condition": rec["condition"],
            "price_csfloat": rec["price_csfloat"],
            "qty_csfloat": int(rec["qty_csfloat"]),
            "fetched_at": rec["fetched_at"].isoformat(),
        })
    if rows:
        upsert_market_rows(sb, rows)
    return len(rows)


if __name__ == "__main__":
    count = run_csfloat_ingest()
    print(f"[csfloat] itens agregados: {count}")


