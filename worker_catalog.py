# worker_catalog.py
import asyncio
import json
import os

import aio_pika
import redis.asyncio as aioredis

from main import (
    CATALOG_QUEUE_NAME,
    RABBIT_URL,
    fetch_json,
    rolimons_cache,
    catalog_client,
    rolimons_client,
    CATALOG_BASE,
)

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
JOB_TTL_SECONDS = 300

redis_client: aioredis.Redis | None = None


async def job_set_result(job_id: str, result: dict):
    await redis_client.set(
        f"job:{job_id}",
        json.dumps(result),
        ex=JOB_TTL_SECONDS,
    )


async def process_bundle_batch(job_id: str, asset_ids: list):
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

    results_list = await asyncio.gather(*[handle_one(aid) for aid in asset_ids])

    out = {str(aid): result for aid, result in zip(asset_ids, results_list)}
    await job_set_result(job_id, out)
    print(f"[worker] bundle_batch job {job_id} concluído ({len(asset_ids)} assets)")


async def process_rap_batch(job_id: str, asset_ids: list):
    async def handle_one(asset_id: int):
        try:
            rap = await rolimons_cache.get_limited_price(asset_id)
            return {"assetId": asset_id, "rap": rap}
        except Exception as e:
            return {"error": f"{e}"}

    results_list = await asyncio.gather(*[handle_one(aid) for aid in asset_ids])

    out = {str(aid): result for aid, result in zip(asset_ids, results_list)}
    await job_set_result(job_id, out)
    print(f"[worker] rap_batch job {job_id} concluído ({len(asset_ids)} assets)")


async def main():
    global redis_client

    # inicia clientes HTTP (necessário para fetch_json funcionar)
    await catalog_client.startup()
    await rolimons_client.startup()

    # conecta ao Redis
    redis_client = aioredis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        decode_responses=True,
    )
    await redis_client.ping()
    print("[worker] Redis conectado")

    # conecta ao RabbitMQ
    connection = await aio_pika.connect_robust(RABBIT_URL)
    channel = await connection.channel()
    await channel.set_qos(prefetch_count=20)

    queue = await channel.declare_queue(CATALOG_QUEUE_NAME, durable=True)

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