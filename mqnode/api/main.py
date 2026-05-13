from fastapi import FastAPI

from mqnode.api.routes.btc_metrics import router as metric_router
from mqnode.api.routes.checkpoints import router as checkpoint_router
from mqnode.api.routes.health import router as health_router
from mqnode.api.routes.internal_price_ingest import router as internal_price_ingest_router
from mqnode.api.routes.registry import router as registry_router
from mqnode.config.logging_config import configure_logging
from mqnode.config.settings import get_settings

settings = get_settings()
configure_logging(settings.log_level)

app = FastAPI(title='MQNODE API', version='0.1.0')
app.include_router(health_router)
app.include_router(registry_router)
app.include_router(checkpoint_router)
app.include_router(metric_router)
app.include_router(internal_price_ingest_router)
