"""
Feature Serving API — FastAPI application.

Serves the latest ML feature vectors from Redis (cache) with Postgres fallback.
Exposes Prometheus metrics at /metrics.
"""

import logging
import sys
import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
import uvicorn

sys.path.insert(0, "/app")
from shared.logging_config import setup_logging
from api.config import APIConfig
from api.cache import FeatureCache
from api.routes import router

logger = setup_logging("api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and tear down connections on startup/shutdown."""
    config: APIConfig = app.state.config
    cache = FeatureCache(
        redis_url=config.REDIS_URL,
        postgres_dsn=config.POSTGRES_DSN,
        cache_enabled=config.CACHE_ENABLED,
        ttl_seconds=config.CACHE_TTL_SECONDS,
    )
    try:
        cache.connect()
    except Exception as e:
        logger.error(f"Failed to connect to backends at startup: {e}")
        raise

    app.state.cache = cache
    logger.info(
        f"API started — cache_enabled={config.CACHE_ENABLED}, "
        f"postgres={config.POSTGRES_DSN!r}"
    )
    yield

    # Shutdown
    cache.close()
    logger.info("API shut down cleanly")


def create_app() -> FastAPI:
    config = APIConfig()
    app = FastAPI(
        title="ML Feature Serving API",
        description="Low-latency feature vector serving with Redis cache and Postgres fallback",
        version="1.0.0",
        lifespan=lifespan,
    )
    app.state.config = config
    app.include_router(router)

    @app.get("/metrics", include_in_schema=False)
    async def metrics():
        return Response(
            content=generate_latest(),
            media_type=CONTENT_TYPE_LATEST,
        )

    @app.exception_handler(Exception)
    async def generic_exception_handler(request: Request, exc: Exception):
        logger.error(f"Unhandled error on {request.url}: {exc}", exc_info=True)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"},
        )

    return app


def main():
    config = APIConfig()
    app = create_app()
    uvicorn.run(
        app,
        host=config.HOST,
        port=config.PORT,
        log_config=None,  # Use our structured logger
        access_log=False,
    )


if __name__ == "__main__":
    main()
