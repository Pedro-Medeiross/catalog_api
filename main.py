from fastapi import FastAPI, HTTPException
import aiohttp
import asyncio
import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
import time

from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

CATALOG_BASE = "https://catalog.roblox.com"
AUTH_BASE = "https://auth.roblox.com"

# onde vamos salvar o cookie em disco (opcional, se quiser persistir)
COOKIE_FILE = Path("roblox_cookie.json")

# se ainda quiser manter essas envs para futuro, ok
ROBLOX_BOT_USERNAME = os.getenv("ROBLOX_BOT_USERNAME")
ROBLOX_BOT_PASSWORD = os.getenv("ROBLOX_BOT_PASSWORD")

# novo: cookie fixo vindo do .env
ROBLOSECURITY_COOKIE = os.getenv("ROBLOSECURITY_COOKIE", "").strip()
if not ROBLOSECURITY_COOKIE:
    raise RuntimeError("ROBLOSECURITY_COOKIE não configurado no .env")

# cache em memória do cookie da sessão atual (caso você queira trocar em runtime)
_current_roblosecurity: Optional[str] = ROBLOSECURITY_COOKIE

# ---------------- Rolimons ----------------

ROLIMONS_URL = "https://www.rolimons.com/itemapi/itemdetails"

class RolimonsCache:
    def __init__(self, ttl_seconds: int = 600):
        self.items: Dict[str, List[Any]] = {}
        self.last_update: float = 0.0
        self.ttl = ttl_seconds

    async def _refresh(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(ROLIMONS_URL, timeout=15) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise RuntimeError(f"Falha ao puxar Rolimons: {resp.status} - {text[:200]}")
                data = await resp.json()
        if not data.get("success"):
            raise RuntimeError(f"Resposta Rolimons inválida: {data}")
        self.items = data.get("items", {})
        self.last_update = time.time()

    async def get_items(self) -> Dict[str, List[Any]]:
        now = time.time()
        if not self.items or now - self.last_update > self.ttl:
            await self._refresh()
        return self.items

    async def get_limited_price(self, asset_id: int) -> Optional[int]:
        items = await self.get_items()
        entry = items.get(str(asset_id))
        if not entry:
            return None
        # [Name, Acronym, Rap, Value, Default Value, ...] [web:484]
        rap = entry[2] if len(entry) > 2 else None
        if isinstance(rap, int) and rap > 0:
            return rap
        return None

rolimons_cache = RolimonsCache(ttl_seconds=600)

# ---------------- Cookie helpers (sem login automático) ----------------

def load_cookie_from_disk() -> Optional[str]:
    if not COOKIE_FILE.exists():
        return None
    try:
        data = json.loads(COOKIE_FILE.read_text(encoding="utf-8"))
        return data.get("ROBLOSECURITY")
    except Exception:
        return None

def save_cookie_to_disk(cookie: str) -> None:
    data = {"ROBLOSECURITY": cookie}
    COOKIE_FILE.write_text(json.dumps(data), encoding="utf-8")

# se quiser que o cookie do .env seja espelhado em disco:
if _current_roblosecurity:
    save_cookie_to_disk(_current_roblosecurity)

async def ensure_cookie() -> str:
    """
    Agora só garante que temos um .ROBLOSECURITY em memória / disco.
    Não tenta mais logar (login quebra por challenge).
    """
    global _current_roblosecurity

    if _current_roblosecurity:
        return _current_roblosecurity

    saved = load_cookie_from_disk()
    if saved:
        _current_roblosecurity = saved
        return _current_roblosecurity

    # fallback final: usa o do .env
    if ROBLOSECURITY_COOKIE:
        _current_roblosecurity = ROBLOSECURITY_COOKIE
        save_cookie_to_disk(ROBLOSECURITY_COOKIE)
        return _current_roblosecurity

    raise RuntimeError("Nenhum .ROBLOSECURITY disponível")

# ---------------- HTTP helper ----------------

async def fetch_json(
    method: str,
    url: str,
    json_body: Dict[str, Any] | None = None,
    require_auth: bool = False,
    _retry: bool = False,
) -> Any:
    """
    Faz uma requisição HTTP simples com aiohttp e valida status.
    Se require_auth=True, usa .ROBLOSECURITY fixo.
    """
    cookies = {}

    if require_auth:
        roblosec = await ensure_cookie()
        cookies[".ROBLOSECURITY"] = roblosec

    async with aiohttp.ClientSession(cookies=cookies) as session:
        if method.upper() == "GET":
            async with session.get(url, timeout=10) as resp:
                status = resp.status
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = await resp.text()
        else:
            async with session.post(url, json=json_body, timeout=10) as resp:
                status = resp.status
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = await resp.text()

    # sem mais retry baseado em login; se der 401/403 você sabe que o cookie venceu
    if status == 401 or status == 403:
        raise HTTPException(status_code=500, detail=f"Roblox auth falhou {status}: {data}")
    if status == 404:
        raise HTTPException(status_code=404, detail=f"Not found: {url}")
    if status >= 500:
        raise HTTPException(status_code=502, detail=f"Upstream error {status} for {url}")
    if status >= 400:
        raise HTTPException(status_code=status, detail=f"Error {status} for {url}: {data}")

    return data

# ---------------- Endpoint bundle (mantido) ----------------

@app.get("/asset-to-bundle/{asset_id}")
async def asset_to_bundle(asset_id: int):
    """
    Dado o assetId de QUALQUER asset (cabeça, animação, camisa, etc),
    tenta descobrir um bundle que contenha esse asset
    e devolve infos básicas (bundleId + preço).
    """
    # https://catalog.roblox.com/v1/assets/{assetId}/bundles
    bundles_data = await fetch_json(
        "GET",
        f"{CATALOG_BASE}/v1/assets/{asset_id}/bundles",
        require_auth=False,  # essa rota é pública
    )

    bundles = bundles_data.get("data") or []
    if not bundles:
        raise HTTPException(status_code=404, detail="No bundles for this asset")

    bundle = bundles[0]
    bundle_id = bundle.get("id")

    # https://catalog.roblox.com/v1/bundles/{bundleId}/details
    details = await fetch_json(
        "GET",
        f"{CATALOG_BASE}/v1/bundles/{bundle_id}/details",
        require_auth=False,  # essa também é pública
    )

    product = details.get("product") or {}
    price = product.get("priceInRobux")

    return {
        "assetId": asset_id,
        "bundleId": bundle_id,
        "bundleName": bundle.get("name"),
        "priceInRobux": price,
    }

# ---------------- Endpoint items/details + Rolimons ----------------

@app.post("/items/details")
async def items_details(asset_ids: List[int]):
    """
    Pega detalhes (incluindo preço atual / lowestPrice de limiteds) de múltiplos assets via
    POST /v1/catalog/items/details.

    Agora:
    - Usa cookie fixo para autenticar.
    - Para limiteds, preenche lowestPrice/priceInRobux com RAP da Rolimons se Roblox não der.
    """
    if not asset_ids:
        return []

    items_payload = {
        "items": [
            {
                "itemType": "Asset",
                "id": asset_id,
            }
            for asset_id in asset_ids
        ]
    }

    details = await fetch_json(
        "POST",
        f"{CATALOG_BASE}/v1/catalog/items/details",
        json_body=items_payload,
        require_auth=True,
    )

    data = details.get("data") or []

    normalized = []
    for item in data:
        product = item.get("product") or {}

        is_limited = bool(product.get("IsLimited") or product.get("IsLimitedUnique") or
                          product.get("isLimited") or product.get("isLimitedUnique"))
        price_in_robux = product.get("priceInRobux")
        lowest_price = product.get("lowestPrice")

        # se for limited e Roblox não trouxe lowestPrice, tenta Rolimons (RAP)
        if is_limited and (not isinstance(lowest_price, int) or lowest_price <= 0):
            try:
                rap = await rolimons_cache.get_limited_price(item.get("id"))
            except Exception as e:
                print(f"[Rolimons] erro ao buscar {item.get('id')}: {e}")
                rap = None

            if rap and rap > 0:
                lowest_price = rap
                price_in_robux = rap

        normalized.append(
            {
                "id": item.get("id"),
                "itemType": item.get("itemType"),
                "name": item.get("name"),
                "priceInRobux": price_in_robux,
                "lowestPrice": lowest_price,
                "isLimited": is_limited,
                "isLimitedUnique": bool(product.get("IsLimitedUnique") or product.get("isLimitedUnique")),
            }
        )

    return normalized
