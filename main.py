from fastapi import FastAPI, HTTPException
import httpx

app = FastAPI()

# base do catálogo oficial do Roblox
CATALOG_BASE = "https://catalog.roblox.com"

async def fetch_json(client: httpx.AsyncClient, url: str):
    resp = await client.get(url, timeout=10)
    resp.raise_for_status()
    return resp.json()

@app.get("/head-to-bundle/{asset_id}")
async def head_to_bundle(asset_id: int):
    """
    Dado o assetId de uma cabeça (Dynamic Head, etc),
    tenta descobrir um bundle que contenha esse asset
    e devolve infos básicas (bundleId + preço).
    """

    async with httpx.AsyncClient() as client:
        # 1) bundles que contêm esse asset
        # endpoint oficial (sem proxy):
        # https://catalog.roblox.com/v1/assets/{assetId}/bundles
        try:
            bundles_data = await fetch_json(
                client, f"{CATALOG_BASE}/v1/assets/{asset_id}/bundles"
            )
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Error fetching bundles: {e}")

        # estrutura típica: { "data": [ { "id": 742198, "name": "...", ... }, ... ] }
        bundles = bundles_data.get("data") or []
        if not bundles:
            raise HTTPException(status_code=404, detail="No bundles for this asset")

        # pega o primeiro bundle da lista (ou você pode aplicar alguma regra)
        bundle = bundles[0]
        bundle_id = bundle.get("id")

        # 2) detalhes do bundle pra pegar preço
        # https://catalog.roblox.com/v1/bundles/{bundleId}/details
        try:
            details = await fetch_json(
                client, f"{CATALOG_BASE}/v1/bundles/{bundle_id}/details"
            )
        except httpx.HTTPError as e:
            raise HTTPException(status_code=502, detail=f"Error fetching bundle details: {e}")

        # você pode inspecionar 'details' no log e refinar isso depois
        # normalmente tem algo como "product" -> "priceInRobux"
        product = details.get("product") or {}
        price = product.get("priceInRobux")

        return {
            "assetId": asset_id,
            "bundleId": bundle_id,
            "bundleName": bundle.get("name"),
            "priceInRobux": price,
        }
