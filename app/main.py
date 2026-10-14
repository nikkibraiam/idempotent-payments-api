from contextlib import asynccontextmanager
from pathlib import Path
from uuid import UUID

from fastapi import FastAPI, Header, Request
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import Settings
from .payments import PaymentService
from .provider import FakeProvider
from .schemas import CreatePayment

SCHEMA = (Path(__file__).parent / "schema.sql").read_text()


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        pool = AsyncConnectionPool(
            settings.database_url,
            min_size=1,
            max_size=settings.pool_size,
            kwargs={"row_factory": dict_row},
            open=False,
        )
        await pool.open()
        async with pool.connection() as conn:
            await conn.execute(SCHEMA)
        app.state.pool = pool
        app.state.provider = FakeProvider(pool)
        app.state.payments = PaymentService(pool, app.state.provider, settings.lock_timeout_seconds)
        yield
        await pool.close()

    app = FastAPI(title="Idempotent Payments API", version="1.0.0", lifespan=lifespan)

    @app.post("/v1/payments")
    async def create_payment(
        body: CreatePayment,
        request: Request,
        idempotency_key: str | None = Header(default=None, max_length=255),
        # Stand-in for the authenticated principal; keys are scoped per client.
        x_client_id: str = Header(min_length=1),
    ) -> JSONResponse:
        if not idempotency_key:
            return JSONResponse(
                {"error": "idempotency_key_required", "message": "Send an Idempotency-Key header."},
                status_code=400,
            )
        result = await request.app.state.payments.create_payment(x_client_id, idempotency_key, body)
        headers = {}
        if result.replayed:
            headers["Idempotent-Replayed"] = "true"
        if result.retry_after is not None:
            headers["Retry-After"] = str(result.retry_after)
        return JSONResponse(result.body, status_code=result.status, headers=headers)

    @app.get("/v1/payments/{payment_id}")
    async def get_payment(payment_id: UUID, request: Request, x_client_id: str = Header(min_length=1)):
        payment = await request.app.state.payments.get_payment(x_client_id, str(payment_id))
        if payment is None:
            return JSONResponse({"error": "not_found"}, status_code=404)
        return payment

    return app
