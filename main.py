from fastapi import FastAPI, HTTPException
import aiohttp
import os
import json
from pathlib import Path
from typing import Optional, Dict, Any, List
import time

from dotenv import load_dotenv

load_dotenv()

app = FastAPI()

CATALOG_BASE = "https://catalog.roblox.com"
ROLIMONS_URL = "https://www.rolimons.com/itemapi/itemdetails"

# ---------------- HTTP helper genérico (sem auth) ----------------

async def fetch_json(
    method: str,
    url: str,
    json_body: Dict[str, Any] | None = None,
) -> Any:
    async with aiohttp.ClientSession() as session:
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

    if status == 404:
        raise HTTPException(status_code=404, detail=f"Not found: {url}")
    if status >= 500:
        raise HTTPException(status_code=502, detail=f"Upstream error {status} for {url}")
    if status >= 400:
        raise HTTPException(status_code=status, detail=f"Error {status} for {url}: {data}")

    return data

# ---------------- Rolimons cache ----------------

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
        # Item ID: [Name, Acronym, Rap, Value, Default Value, ...][web:484]
        rap = entry[2] if len(entry) > 2 else None
        if isinstance(rap, int) and rap > 0:
            return rap
        return None

rolimons_cache = RolimonsCache(ttl_seconds=600)

# ---------------- Endpoint bundle (mantido) ----------------

@app.get("/asset-to-bundle/{asset_id}")
async def asset_to_bundle(asset_id: int):
    """
    Dado o assetId de QUALQUER asset (cabeça, animação, camisa, etc),
    tenta descobrir um bundle que contenha esse asset
    e devolve infos básicas (bundleId + preço oficial).
    """
    # https://catalog.roblox.com/v1/assets/{assetId}/bundles
    bundles_data = await fetch_json(
        "GET",
        f"{CATALOG_BASE}/v1/assets/{asset_id}/bundles",
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
    )

    product = details.get("product") or {}
    price = product.get("priceInRobux")

    return {
        "assetId": asset_id,
        "bundleId": bundle_id,
        "bundleName": bundle.get("name"),
        "priceInRobux": price,
    }

# ---------------- Endpoint preço limited via Rolimons ----------------

@app.get("/rolimons/limited-price/{asset_id}")
async def rolimons_limited_price(asset_id: int):
    """
    Retorna RAP de um limited via Rolimons (ou null se não for limited / não encontrado).
    """
    try:
        rap = await rolimons_cache.get_limited_price(asset_id)
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erro ao consultar Rolimons: {e}")

    return {
        "assetId": asset_id,
        "rap": rap,
    }
