from fastapi import FastAPI, HTTPException
import aiohttp
import asyncio
import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List

from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

CATALOG_BASE = "https://catalog.roblox.com"
AUTH_BASE = "https://auth.roblox.com"

# onde vamos salvar o cookie em disco
COOKIE_FILE = Path("roblox_cookie.json")

ROBLOX_BOT_USERNAME = os.getenv("ROBLOX_BOT_USERNAME")
ROBLOX_BOT_PASSWORD = os.getenv("ROBLOX_BOT_PASSWORD")

# cache em memória do cookie da sessão atual
_current_roblosecurity: Optional[str] = None


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


async def login_and_get_cookie() -> str:
    """
    Faz login na conta bot via auth.roblox.com e retorna o .ROBLOSECURITY.
    Usa a dança do x-csrf-token (primeiro POST falha, depois repete com header).
    """
    if not ROBLOX_BOT_USERNAME or not ROBLOX_BOT_PASSWORD:
        raise RuntimeError("ROBLOX_BOT_USERNAME / ROBLOX_BOT_PASSWORD não configurados")

    async with aiohttp.ClientSession() as session:
        # 1) tentativa inicial pra pegar x-csrf-token
        resp1 = await session.post(f"{AUTH_BASE}/v2/login")
        csrf_token = resp1.headers.get("x-csrf-token")
        if not csrf_token:
            raise RuntimeError("Não consegui obter x-csrf-token do Roblox")

        # 2) login real
        headers = {
            "x-csrf-token": csrf_token,
            "Content-Type": "application/json",
        }
        payload = {
            "ctype": "Username",
            "cvalue": ROBLOX_BOT_USERNAME,
            "password": ROBLOX_BOT_PASSWORD,
        }

        resp2 = await session.post(f"{AUTH_BASE}/v2/login", json=payload, headers=headers)
        if resp2.status >= 400:
            text = await resp2.text()
            raise RuntimeError(f"Falha no login Roblox: {resp2.status} - {text}")

        # aiohttp session agora deve ter o cookie .ROBLOSECURITY
        jar = session.cookie_jar
        roblosec = None
        for cookie in jar:
            if cookie.key == ".ROBLOSECURITY":
                roblosec = cookie.value
                break

        if not roblosec:
            raise RuntimeError("Login OK mas não encontrei .ROBLOSECURITY nos cookies")

        return roblosec


async def ensure_cookie() -> str:
    """
    Garante que temos um .ROBLOSECURITY válido em memória.
    Tenta carregar de memória -> disco -> login.
    """
    global _current_roblosecurity

    if _current_roblosecurity:
        return _current_roblosecurity

    # tenta disco
    saved = load_cookie_from_disk()
    if saved:
        _current_roblosecurity = saved
        return _current_roblosecurity

    # se não tiver, faz login
    roblosec = await login_and_get_cookie()
    _current_roblosecurity = roblosec
    save_cookie_to_disk(roblosec)
    return _current_roblosecurity


async def fetch_json(
    method: str,
    url: str,
    json_body: Dict[str, Any] | None = None,
    require_auth: bool = False,
    _retry: bool = False,
) -> Any:
    """
    Faz uma requisição HTTP simples com aiohttp e valida status.
    Se require_auth=True e der 401/403, renova cookie e tenta 1x de novo.
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

    # se precisar de auth e deu 401/403, tenta renovar cookie uma vez
    if require_auth and status in (401, 403) and not _retry:
        # força novo login
        new_cookie = await login_and_get_cookie()
        global _current_roblosecurity
        _current_roblosecurity = new_cookie
        save_cookie_to_disk(new_cookie)

        # tenta de novo uma vez
        return await fetch_json(
            method,
            url,
            json_body=json_body,
            require_auth=require_auth,
            _retry=True,
        )

    if status == 404:
        raise HTTPException(status_code=404, detail=f"Not found: {url}")
    if status >= 500:
        raise HTTPException(status_code=502, detail=f"Upstream error {status} for {url}")
    if status >= 400:
        # status 4xx genérico
        raise HTTPException(status_code=status, detail=f"Error {status} for {url}: {data}")

    return data


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


@app.post("/items/details")
async def items_details(asset_ids: List[int]):
    """
    Pega detalhes (incluindo preço atual / lowestPrice de limiteds) de múltiplos assets via
    POST /v1/catalog/items/details.

    Aqui usamos require_auth=True porque essa rota é a que vinha dando 403.
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
        normalized.append(
            {
                "id": item.get("id"),
                "itemType": item.get("itemType"),
                "name": item.get("name"),
                "priceInRobux": product.get("priceInRobux"),
                "lowestPrice": product.get("lowestPrice"),
                "isLimited": product.get("isLimited"),
                "isLimitedUnique": product.get("isLimitedUnique"),
            }
        )

    return normalized
