from fastapi import FastAPI, HTTPException
import aiohttp
import os
import json
from typing import Optional, Dict, Any, List
import time
import asyncio

from dotenv import load_dotenv
from pydantic import BaseModel
import aio_pika
from uuid import uuid4

load_dotenv()

app = FastAPI()

CATALOG_BASE = "https://catalog.roblox.com"
ROLIMONS_URL = "https://www.rolimons.com/itemapi/itemdetails"

# ---------------- Config RabbitMQ ----------------

RABBIT_USER = os.getenv("RABBITMQ_DEFAULT_USER", "guest")
RABBIT_PASS = os.getenv("RABBITMQ_DEFAULT_PASS", "guest")
RABBIT_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBIT_PORT = os.getenv("RABBITMQ_PORT", "5672")

RABBIT_URL = f"amqp://{RABBIT_USER}:{RABBIT_PASS}@{RABBIT_HOST}:{RABBIT_PORT}/"
CATALOG_QUEUE_NAME = "catalog_jobs"

rabbit_connection: aio_pika.RobustConnection | None = None
rabbit_channel: aio_pika.Channel | None = None

# storage simples em memória: job_id -> resultado ou None (pendente)
job_results: Dict[str, Any] = {}


# ---------------- HTTP client com retry + concorrência ----------------


class HTTPClient:
    def __init__(
        self,
        base_url: str | None = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        max_concurrent: int = 20,
        request_timeout: float = 10.0,
    ):
        self.base_url = base_url.rstrip("/") if base_url else None
        self.max_retries = max_retries
        self.backoff_base = backoff_base
        self._session: aiohttp.ClientSession | None = None
        self._sem = asyncio.Semaphore(max_concurrent)
        self._timeout = aiohttp.ClientTimeout(total=request_timeout)

    async def startup(self):
        if self._session is None:
            connector = aiohttp.TCPConnector(
                limit=self._sem._value,
                limit_per_host=self._sem._value,
            )
            self._session = aiohttp.ClientSession(
                connector=connector,
                timeout=self._timeout,
            )

    async def shutdown(self):
        if self._session is not None:
            await self._session.close()
            self._session = None

    async def _request_once(
        self,
        method: str,
        url: str,
        json_body: Dict[str, Any] | None = None,
    ) -> tuple[int, Any]:
        if self._session is None:
            raise RuntimeError("HTTPClient session not initialized")
        async with self._sem:
            async with self._session.request(
                method.upper(),
                url,
                json=json_body,
            ) as resp:
                status = resp.status
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = await resp.text()
                return status, data

    async def request(
        self,
        method: str,
        url: str,
        json_body: Dict[str, Any] | None = None,
    ) -> Any:
        if self.base_url and not url.startswith("http"):
            url = f"{self.base_url}{url}"

        last_status: int | None = None
        last_data: Any = None

        for attempt in range(1, self.max_retries + 1):
            try:
                status, data = await self._request_once(method, url, json_body)
                last_status, last_data = status, data

                if status == 404:
                    raise HTTPException(status_code=404, detail=f"Not found: {url}")
                if status >= 500:
                    # tenta de novo
                    raise aiohttp.ClientResponseError(
                        request_info=None,
                        history=(),
                        status=status,
                        message=str(data),
                        headers=None,
                    )
                if status >= 400:
                    raise HTTPException(
                        status_code=status,
                        detail=f"Error {status} for {url}: {data}",
                    )

                return data

            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                if attempt == self.max_retries:
                    raise HTTPException(
                        status_code=502,
                        detail=f"Upstream error after {self.max_retries} attempts for {url}: {e}",
                    )
                delay = self.backoff_base * (2 ** (attempt - 1))
                await asyncio.sleep(delay)

        raise HTTPException(
            status_code=502,
            detail=f"Upstream error for {url}: {last_status} {last_data}",
        )


# instâncias globais
catalog_client = HTTPClient(base_url=CATALOG_BASE, max_concurrent=20)
rolimons_client = HTTPClient(max_concurrent=5)


@app.on_event("startup")
async def _startup():
    # HTTP clients
    await catalog_client.startup()
    await rolimons_client.startup()

    # RabbitMQ
    global rabbit_connection, rabbit_channel
    rabbit_connection = await aio_pika.connect_robust(RABBIT_URL)
    rabbit_channel = await rabbit_connection.channel()
    await rabbit_channel.set_qos(prefetch_count=50)
    await rabbit_channel.declare_queue(
        CATALOG_QUEUE_NAME,
        durable=True,
    )


@app.on_event("shutdown")
async def _shutdown():
    await catalog_client.shutdown()
    await rolimons_client.shutdown()

    global rabbit_connection, rabbit_channel
    if rabbit_channel:
        await rabbit_channel.close()
    if rabbit_connection:
        await rabbit_connection.close()


# helper compatível com seu código antigo
async def fetch_json(
    method: str,
    url: str,
    json_body: Dict[str, Any] | None = None,
) -> Any:
    if url.startswith(CATALOG_BASE):
        return await catalog_client.request(method, url, json_body)
    elif url.startswith("https://www.rolimons.com"):
        return await rolimons_client.request(method, url, json_body)
    else:
        return await catalog_client.request(method, url, json_body)


# ---------------- Rolimons cache ----------------


class RolimonsCache:
    def __init__(self, ttl_seconds: int = 600):
        self.items: Dict[str, List[Any]] = {}
        self.last_update: float = 0.0
        self.ttl = ttl_seconds
        self._lock = asyncio.Lock()

    async def _refresh(self):
        async with self._lock:
            now = time.time()
            if self.items and now - self.last_update <= self.ttl:
                return

            data = await rolimons_client.request("GET", ROLIMONS_URL)

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
        # Item ID: [Name, Acronym, Rap, Value, Default Value, ...][web:69]
        rap = entry[2] if len(entry) > 2 else None
        if isinstance(rap, int) and rap > 0:
            return rap
        return None


rolimons_cache = RolimonsCache(ttl_seconds=600)


# ---------------- Models para batch ----------------


class AssetIdsPayload(BaseModel):
    asset_ids: List[int]


# ---------------- Endpoint bundle (unitário) ----------------


@app.get("/asset-to-bundle/{asset_id}")
async def asset_to_bundle(asset_id: int):
    """
    Dado o assetId de QUALQUER asset (cabeça, animação, camisa, etc),
    tenta descobrir um bundle que contenha esse asset
    e devolve infos básicas (bundleId + preço oficial).
    """
    bundles_data = await fetch_json(
        "GET",
        f"{CATALOG_BASE}/v1/assets/{asset_id}/bundles",
    )

    bundles = bundles_data.get("data") or []
    if not bundles:
        raise HTTPException(status_code=404, detail="No bundles for this asset")

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


# ---------------- Endpoint preço limited via Rolimons (unitário) ----------------


@app.get("/rolimons/limited-price/{asset_id}")
async def rolimons_limited_price(asset_id: int):
    """
    Retorna RAP de um limited via Rolimons (ou null se não for limited / não encontrado).
    """
    try:
        rap = await rolimons_cache.get_limited_price(asset_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Erro ao consultar Rolimons: {e}")

    return {
        "assetId": asset_id,
        "rap": rap,
    }


# ---------------- Endpoints batch com RabbitMQ (enqueue) ----------------


@app.post("/asset-to-bundle/batch/enqueue")
async def asset_to_bundle_batch_enqueue(payload: AssetIdsPayload):
    if rabbit_channel is None:
        raise HTTPException(status_code=500, detail="RabbitMQ não inicializado")

    asset_ids = payload.asset_ids or []
    if not asset_ids:
        raise HTTPException(status_code=400, detail="asset_ids vazio")

    job_id = str(uuid4())
    job_results[job_id] = None  # pendente

    body = {
        "type": "bundle_batch",
        "job_id": job_id,
        "asset_ids": asset_ids,
    }

    await rabbit_channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(body).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=CATALOG_QUEUE_NAME,
    )

    return {"status": "queued", "job_id": job_id}


@app.post("/rolimons/limited-price/batch/enqueue")
async def rolimons_limited_price_batch_enqueue(payload: AssetIdsPayload):
    if rabbit_channel is None:
        raise HTTPException(status_code=500, detail="RabbitMQ não inicializado")

    asset_ids = payload.asset_ids or []
    if not asset_ids:
        raise HTTPException(status_code=400, detail="asset_ids vazio")

    job_id = str(uuid4())
    job_results[job_id] = None

    body = {
        "type": "rap_batch",
        "job_id": job_id,
        "asset_ids": asset_ids,
    }

    await rabbit_channel.default_exchange.publish(
        aio_pika.Message(
            body=json.dumps(body).encode("utf-8"),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        ),
        routing_key=CATALOG_QUEUE_NAME,
    )

    return {"status": "queued", "job_id": job_id}


# ---------------- Endpoint para consultar resultado do job ----------------


@app.get("/catalog/job-result/{job_id}")
async def get_catalog_job_result(job_id: str):
    if job_id not in job_results:
        raise HTTPException(status_code=404, detail="job_id desconhecido")

    result = job_results[job_id]
    if result is None:
        return {"status": "pending"}

    return {"status": "done", "result": result}
