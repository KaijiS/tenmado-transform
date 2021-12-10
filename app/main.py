from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from utils import logger
from routers import samplerouter
from routers import checkrouter
from routers import transformrouter
from routers import firestorerouter

app = FastAPI(
    title="tenmado-transform",
    description="tenmadoプロジェクトのELTのTの処理を行う",
    version="1.0",
    # デフォルトの応答クラスを指定: ORJSONResponseｰ>パフォーマンス高い
    default_response_class=ORJSONResponse,
)

logger.setup_logger()

# ルーティングをinclude
app.include_router(samplerouter.router)
app.include_router(checkrouter.router, prefix="/check")
app.include_router(transformrouter.router, prefix="/transform")
app.include_router(firestorerouter.router, prefix="/firestore")
