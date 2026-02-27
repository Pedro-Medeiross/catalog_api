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

job_results: Dict[str, Any] = {}


# ---------------- HTTP client com retry + concorrência ----------------


class HTTPClient:
    def __init__(
        self,
        base_url: str | None = None,
        max_retries: int = 3,
        backoff_base: float = 0.5,
        # CORREÇÃO 1: max_concurrent reduzido de 20 → 5 para não sobrecarregar o Roblox Catalog
        max_concurrent: int = 5,
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

    # CORREÇÃO 2: _request_once agora também retorna os headers para ler Retry-After
    async def _request_once(
        self,
        method: str,
        url: str,
        json_body: Dict[str, Any] | None = None,
    ) -> tuple[int, Any, aiohttp.CIMultiDictProxy]:
        if self._session is None:
            raise RuntimeError("HTTPClient session not initialized")
        async with self._sem:
            async with self._session.request(
                method.upper(),
                url,
                json=json_body,
            ) as resp:
                status = resp.status
                headers = resp.headers
                try:
                    data = await resp.json(content_type=None)
                except Exception:
                    data = await resp.text()
                return status, data, headers

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
                status, data, headers = await self._request_once(method, url, json_body)
                last_status, last_data = status, data

                if status == 404:
                    raise HTTPException(status_code=404, detail=f"Not found: {url}")

                # CORREÇÃO 3: 429 agora faz retry com backoff exponencial
                # respeitando o header Retry-After quando presente
                if status == 429:
                    if attempt == self.max_retries:
                        raise HTTPException(
                            status_code=429,
                            detail=f"Rate limited after {self.max_retries} attempts: {url}",
                        )
                    retry_after = headers.get("Retry-After")
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            delay = self.backoff_base * (2 ** (attempt - 1))
                    else:
                        # sem header: backoff exponencial 0.5s, 1s, 2s, 4s...
                        delay = self.backoff_base * (2 ** (attempt - 1))
                    await asyncio.sleep(delay)
                    continue

                if status >= 500:
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

            except HTTPException:
                raise

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


# instâncias globais — catalog_client com max_concurrent=5 (era 20)
catalog_client = HTTPClient(base_url=CATALOG_BASE, max_concurrent=5)
rolimons_client = HTTPClient(max_concurrent=5)


@app.on_event("startup")
async def _startup():
    await catalog_client.startup()
    await rolimons_client.startup()

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
    job_results[job_id] = None

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