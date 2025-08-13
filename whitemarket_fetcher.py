#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import gzip
import io
import typing as t
from datetime import datetime, timezone

import requests
from dotenv import load_dotenv
import ijson

WHITEMARKET_URL = "https://s3.white.market/export/v1/products/730.json"
# Desabilita HTTP/2 no httpx/postgrest para evitar RemoteProtocolError em lotes grandes
os.environ.setdefault("HTTPX_DISABLE_HTTP2", "1")
# Carrega .env do diretório deste arquivo (robusto contra cwd diferente)
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
    # keep a technical key without special symbols; join with pipe and collapse empties
    return "|".join([p for p in parts if p != ""]).strip()


def build_display_name(name_base: str, stattrak: bool, souvenir: bool, condition: t.Optional[str], phase: t.Optional[str]) -> str:
    name = name_base
    if stattrak:
        # Insert StatTrak™ after the star or before base
        if name.startswith("★ "):
            name = name.replace("★ ", "★ StatTrak™ ", 1)
        else:
            name = f"StatTrak™ {name}"
    if souvenir and not stattrak:
        # Souvenir prefix only if not StatTrak
        name = f"Souvenir {name}"
    if condition:
        name = f"{name} ({condition})"
    if phase:
        name = f"{name} – {phase}"
    return name


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


def open_source_stream(url: str, retry_count: int = 3):
    """Open source stream with retry logic for resilience"""
    import time
    
    for attempt in range(retry_count):
        try:
            print(f"[whitemarket] Tentativa {attempt + 1}/{retry_count} para {url}")
            
            headers = {"Accept": "application/json"}
            api_token = os.environ.get("WHITEMARKET_API_TOKEN")
            if api_token:
                headers["Authorization"] = f"Bearer {api_token}"
            
            # Primeiro fazer HEAD request para verificar se a API está respondendo
            head_resp = requests.head(url, headers=headers, timeout=30)
            print(f"[whitemarket] HEAD response: {head_resp.status_code}")
            
            if head_resp.status_code == 404:
                print(f"[whitemarket] API endpoint não encontrado: 404")
                raise requests.exceptions.HTTPError("API endpoint não encontrado")
            
            # Fazer o request real
            resp = requests.get(url, headers=headers, stream=True, timeout=120)  # Timeout maior
            resp.raise_for_status()
            resp.raw.decode_content = True
            
            # Ler os primeiros bytes para verificar formato
            head = resp.raw.read(4)
            base = PrependStream(head, resp.raw)
            
            if head.startswith(b"\x1f\x8b"):
                print(f"[whitemarket] Arquivo GZIP detectado")
                return gzip.GzipFile(fileobj=base, mode="rb")
            
            print(f"[whitemarket] Stream aberto com sucesso")
            return base
            
        except requests.exceptions.Timeout:
            print(f"[whitemarket] Timeout na tentativa {attempt + 1}")
            if attempt == retry_count - 1:
                raise
            time.sleep(10)  # Aguardar 10s antes de tentar novamente
            
        except requests.exceptions.HTTPError as e:
            print(f"[whitemarket] Erro HTTP na tentativa {attempt + 1}: {e}")
            if attempt == retry_count - 1:
                raise
            time.sleep(5)
            
        except Exception as e:
            print(f"[whitemarket] Erro inesperado na tentativa {attempt + 1}: {e}")
            if attempt == retry_count - 1:
                raise
            time.sleep(5)
    
    raise RuntimeError(f"Falha após {retry_count} tentativas")


def get_supabase_client():
    from supabase import create_client
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("SUPABASE_URL/SUPABASE_SERVICE_ROLE não configurados no ambiente")
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def chunked(iterable, size: int):
    buf = []
    for x in iterable:
        buf.append(x)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def upsert_market_rows(sb, rows: list[dict]):
    for batch in chunked(rows, UPSERT_BATCH):
        sb.table(MARKET_TABLE).upsert(batch, on_conflict="item_key").execute()


def insert_price_snapshot(*args, **kwargs):
    # no-op kept for compatibility if referenced elsewhere
    return None


def iter_json_items(stream) -> t.Iterable[dict]:
    """Yield items from common root layouts: array root (item), products.item, data.item.
    Falls back to NDJSON (one JSON object per line)."""
    tried = False
    # 1) Root array
    try:
        for obj in ijson.items(stream, "item"):
            tried = True
            if isinstance(obj, dict):
                yield obj
        if tried:
            return
    except Exception:
        pass

    # If we got here, reopen and try alternative roots
    
def fetch_whitemarket(url: str = WHITEMARKET_URL) -> t.Iterable[dict]:
    """Fetch WhiteMarket data with enhanced error handling"""
    import json
    
    # Método 1: Carregamento direto (mais confiável para JSON grandes)
    try:
        print(f"[whitemarket] Tentando carregamento direto de {url}")
        
        headers = {"Accept": "application/json"}
        api_token = os.environ.get("WHITEMARKET_API_TOKEN")
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"
        
        resp = requests.get(url, headers=headers, timeout=180)
        resp.raise_for_status()
        
        print(f"[whitemarket] Response recebido: {len(resp.content)} bytes")
        
        # Descomprimir se necessário
        if resp.content.startswith(b"\x1f\x8b"):
            content = gzip.decompress(resp.content).decode('utf-8')
            print(f"[whitemarket] GZIP descomprimido: {len(content)} chars")
        else:
            content = resp.text
            print(f"[whitemarket] Texto direto: {len(content)} chars")
        
        if not content.strip():
            print(f"[whitemarket] ERRO: Conteúdo vazio")
            raise ValueError("Conteúdo vazio")
        
        # Verificar integridade do JSON
        content = content.strip()
        if not (content.endswith('}') or content.endswith(']')):
            print(f"[whitemarket] AVISO: JSON incompleto, últimos chars: {content[-50:]}")
        
        data = json.loads(content)
        print(f"[whitemarket] JSON válido: {type(data).__name__}")
        
        # Processar estruturas conhecidas
        if isinstance(data, list):
            print(f"[whitemarket] Array direto: {len(data)} itens")
            for item in data:
                if isinstance(item, dict):
                    yield item
        elif isinstance(data, dict):
            for key in ['products', 'data', 'items', 'result']:
                if key in data and isinstance(data[key], list):
                    print(f"[whitemarket] Array em '{key}': {len(data[key])} itens")
                    for item in data[key]:
                        if isinstance(item, dict):
                            yield item
                    return
            
            if 'market_hash_name' in data:
                yield data
        
        return
        
    except (json.JSONDecodeError, ValueError) as e:
        print(f"[whitemarket] Erro JSON: {e}. Tentando streaming...")
    except Exception as e:
        print(f"[whitemarket] Erro direto: {e}. Tentando streaming...")
    
    # Método 2: Streaming (fallback)
    root_paths = [
        "item",
        "products.item", 
        "data.item"
    ]

    for root in root_paths:
        try:
            print(f"[whitemarket] Streaming com root: {root}")
            stream = open_source_stream(url)
            got_any = False
            
            for obj in ijson.items(stream, root):
                got_any = True
                if isinstance(obj, dict):
                    yield obj
                    
            if got_any:
                print(f"[whitemarket] Sucesso com root: {root}")
                return
                
        except ijson.common.IncompleteJSONError as e:
            print(f"[whitemarket] JSON incompleto com {root}: {e}")
            continue
        except Exception as e:
            print(f"[whitemarket] Erro streaming {root}: {e}")
            continue

    # Fallback: NDJSON (one JSON object per line)
    stream = open_source_stream(url)
    text_stream = io.TextIOWrapper(stream, encoding='utf-8', errors='ignore')
    for line in text_stream:
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                yield obj
        except Exception:
            continue


def aggregate_whitemarket(products: t.Iterable[dict]) -> t.Dict[str, dict]:
    """Agrega diretamente por item normalizado (name_base/st/sv/condition) e usa SEMPRE o menor preço.

    Evita agrupar por product_class_id para não mesclar variantes diferentes acidentalmente.
    """
    def to_usd(val, field_name: str):
        v = val
        if isinstance(v, str):
            try:
                v = float(v.replace(",", ".").strip())
            except Exception:
                return None
        # heurística: campos *_cents são centavos
        if field_name.endswith("_cents") and isinstance(v, (int, float)):
            return float(v) / 100.0
        # heurística adicional: inteiros muito grandes podem ser centavos
        if isinstance(v, int) and v >= 1000:
            return float(v) / 100.0
        try:
            return float(v)
        except Exception:
            return None

    acc: t.Dict[str, dict] = {}
    now = datetime.now(timezone.utc)
    for p in products:
        name = (
            p.get("name_hash")
            or p.get("market_hash_name")
            or p.get("hash_name")
            or p.get("name")
            or ""
        )
        # preço desta linha
        line_price = None
        for f in ("price", "price_usd", "price_cents", "amount", "value"):
            if f in p and p[f] is not None:
                line_price = to_usd(p[f], f)
                if line_price is not None:
                    break

        # normalização do item
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
                "price_whitemarket": line_price,
                "qty_whitemarket": 1,
                "fetched_at": now,
            }
        else:
            rec["qty_whitemarket"] = int(rec.get("qty_whitemarket", 0)) + 1
            if line_price is not None:
                cur = rec.get("price_whitemarket")
                if cur is None or line_price < cur:
                    rec["price_whitemarket"] = line_price
    return acc


def run_whitemarket_ingest(url: str = WHITEMARKET_URL) -> int:
    """Executa ingestão otimizada para economia de memória"""
    import gc
    
    sb = get_supabase_client()
    batch_size = int(os.environ.get('SUPABASE_UPSERT_BATCH', '200'))
    total_processed = 0
    
    print(f"[whitemarket] Iniciando com batch_size={batch_size}")
    
    try:
        # Processar com streaming real para 27k+ itens
        products = fetch_whitemarket(url)
        aggregated = {}
        raw_count = 0
        
        for product in products:
            if not product:
                continue
                
            try:
                # Processar item individual
                market_hash_name = product.get("market_hash_name", "")
                price = product.get("price")
                qty = product.get("qty", 1)
                
                if not market_hash_name or not isinstance(price, (int, float)):
                    continue
                    
                name_base, stattrak, souvenir, condition = parse_market_hash_name(market_hash_name)
                if not name_base:
                    continue
                    
                item_key = build_item_key(name_base, stattrak, souvenir, condition, None)
                
                # Agregar na memória temporária (limitada)
                if item_key in aggregated:
                    aggregated[item_key]["price_whitemarket"] = min(
                        aggregated[item_key]["price_whitemarket"], price
                    )
                    aggregated[item_key]["qty_whitemarket"] += qty
                else:
                    aggregated[item_key] = {
                        "item_key": item_key,
                        "name_base": name_base,
                        "stattrak": stattrak,
                        "souvenir": souvenir,
                        "condition": condition,
                        "price_whitemarket": price,
                        "qty_whitemarket": qty,
                        "fetched_at": datetime.now(timezone.utc),
                    }
                
                raw_count += 1
                
                # CRÍTICO: Limitar tamanho do dict agregado
                if len(aggregated) >= batch_size:
                    rows = []
                    for _, rec in aggregated.items():
                        rows.append({
                            "item_key": rec["item_key"],
                            "name_base": rec["name_base"],
                            "stattrak": bool(rec["stattrak"]),
                            "souvenir": bool(rec["souvenir"]),
                            "condition": rec["condition"],
                            "price_whitemarket": rec["price_whitemarket"],
                            "qty_whitemarket": int(rec["qty_whitemarket"]),
                            "fetched_at": rec["fetched_at"].isoformat(),
                        })
                    
                    if rows:
                        upsert_market_rows(sb, rows)
                        total_processed += len(rows)
                        print(f"[whitemarket] Batch {total_processed//batch_size}: {len(rows)} itens únicos de {raw_count} processados")
                    
                    # Limpar memória agressivamente
                    aggregated.clear()
                    rows.clear()
                    gc.collect()
                    
                    # Log de memória a cada batch
                    if total_processed % (batch_size * 3) == 0:  # A cada 3 batches
                        try:
                            import memory_optimizer
                            memory_optimizer.log_memory_usage(f"WhiteMarket batch {total_processed//batch_size}")
                            memory_optimizer.memory_limit_check(350)  # Limite mais baixo durante processamento
                        except:
                            pass
                    
            except Exception as e:
                print(f"[whitemarket] Erro ao processar item: {e}")
                continue
        
        # Processar itens restantes
        if aggregated:
            rows = []
            for _, rec in aggregated.items():
                rows.append({
                    "item_key": rec["item_key"],
                    "name_base": rec["name_base"],
                    "stattrak": bool(rec["stattrak"]),
                    "souvenir": bool(rec["souvenir"]),
                    "condition": rec["condition"],
                    "price_whitemarket": rec["price_whitemarket"],
                    "qty_whitemarket": int(rec["qty_whitemarket"]),
                    "fetched_at": rec["fetched_at"].isoformat(),
                })
            
            if rows:
                upsert_market_rows(sb, rows)
                total_processed += len(rows)
        
        print(f"[whitemarket] Finalizado: {total_processed} itens total")
        return total_processed
        
    except Exception as e:
        print(f"[whitemarket] Erro crítico: {e}")
        return total_processed


if __name__ == "__main__":
    count = run_whitemarket_ingest()
    print(f"[whitemarket] itens agregados: {count}")


