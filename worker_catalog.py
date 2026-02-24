# worker_catalog.py
import asyncio
import json
import os

import aio_pika

from main import (
    CATALOG_QUEUE_NAME,
    RABBIT_URL,
    fetch_json,
    rolimons_cache,
    job_results,
    CATALOG_BASE,
)

async def process_bundle_batch(job_id: str, asset_ids):
    async def handle_one(asset_id: int):
        try:
            bundles_data = await fetch_json(
                "GET",
                f"{CATALOG_BASE}/v1/assets/{asset_id}/bundles",
            )
            bundles = bundles_data.get("data") or []
            if not bundles:
                return {"error": "No bundles for this asset"}

            bundle = bundles[0]
            bundle_id = bundle.get("id")

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
        except Exception as e:
            return {"error": f"{e}"}

    tasks = [handle_one(aid) for aid in asset_ids]
    results_list = await asyncio.gather(*tasks)

    out = {}
    for aid, result in zip(asset_ids, results_list):
        out[str(aid)] = result

    job_results[job_id] = out
    print(f"[worker] bundle_batch job {job_id} concluído ({len(asset_ids)} assets)")


async def process_rap_batch(job_id: str, asset_ids):
    async def handle_one(asset_id: int):
        try:
            rap = await rolimons_cache.get_limited_price(asset_id)
            return {"assetId": asset_id, "rap": rap}
        except Exception as e:
            return {"error": f"{e}"}

    tasks = [handle_one(aid) for aid in asset_ids]
    results_list = await asyncio.gather(*tasks)

    out = {}
    for aid, result in zip(asset_ids, results_list):
        out[str(aid)] = result

    job_results[job_id] = out
    print(f"[worker] rap_batch job {job_id} concluído ({len(asset_ids)} assets)")


async def main():
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=20)

    queue = await channel.declare_queue(
        CATALOG_QUEUE_NAME,
        durable=True,
    )

    print("[worker] aguardando mensagens...")

    async with queue.iterator() as q_iter:
        async for message in q_iter:
            async with message.process():
                try:
                    payload = json.loads(message.body.decode("utf-8"))
                    job_type = payload.get("type")
                    job_id = payload.get("job_id")
                    asset_ids = payload.get("asset_ids") or []

                    if not job_id or not asset_ids:
                        print("[worker] mensagem inválida:", payload)
                        continue

                    if job_type == "bundle_batch":
                        await process_bundle_batch(job_id, asset_ids)
                    elif job_type == "rap_batch":
                        await process_rap_batch(job_id, asset_ids)
                    else:
                        print("[worker] tipo de job desconhecido:", job_type)

                except Exception as e:
                    print("[worker] erro ao processar mensagem:", e)

if __name__ == "__main__":
    asyncio.run(main())
