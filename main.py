from fastapi import FastAPI, HTTPException
import aiohttp
from aiohttp_retry import RetryClient, RetryOptions

app = FastAPI()

CATALOG_BASE = "https://catalog.roblox.com"


async def get_retry_client():
    """
    Cria um cliente aiohttp com retry básico.
    """
    retry_options = RetryOptions(
        attempts=3,
        factor=0.5,
        statuses={500, 502, 503, 504},
        allowed_methods=["GET", "POST"],
    )
    client = RetryClient(
        retry_options=retry_options,
        raise_for_status=False,
    )
    return client


async def fetch_json(client: RetryClient, method: str, url: str, json_body: dict | None = None):
    """
    Wrapper genérico para GET/POST com retry + validação de status.
    """
    if method.upper() == "GET":
        async with client.get(url, timeout=10) as resp:
            status = resp.status
            data = await resp.json(content_type=None)
    else:
        async with client.post(url, json=json_body, timeout=10) as resp:
            status = resp.status
            data = await resp.json(content_type=None)

    if status == 404:
        raise HTTPException(status_code=404, detail=f"Not found: {url}")

    if status >= 500:
        raise HTTPException(status_code=502, detail=f"Upstream error {status} for {url}")

    if status >= 400:
        raise HTTPException(status_code=status, detail=f"Error {status} for {url}")

    return data


@app.get("/asset-to-bundle/{asset_id}")
async def asset_to_bundle(asset_id: int):
    """
    Dado o assetId de QUALQUER asset (cabeça, animação, camisa, etc),
    tenta descobrir um bundle que contenha esse asset
    e devolve infos básicas (bundleId + preço) [web:363].
    """
    client = await get_retry_client()
    try:
        # https://catalog.roblox.com/v1/assets/{assetId}/bundles
        bundles_data = await fetch_json(
            client,
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
            client,
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

    finally:
        await client.close()


@app.post("/items/details")
async def items_details(asset_ids: list[int]):
    """
    Pega detalhes (incluindo preço atual) de múltiplos assets via
    POST /v1/catalog/items/details [web:349][web:355].
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

    client = await get_retry_client()
    try:
        details = await fetch_json(
            client,
            "POST",
            f"{CATALOG_BASE}/v1/catalog/items/details",
            json_body=items_payload,
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

    finally:
        await client.close()