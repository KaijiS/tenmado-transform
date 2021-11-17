from fastapi import FastAPI
from fastapi.responses import ORJSONResponse

from routers import samplerouter
from routers import checkrouter

app = FastAPI(
    title="tenmado-transform",
    description="tenmadoプロジェクトのELTのTの処理を行う",
    version="1.0",
    # デフォルトの応答クラスを指定: ORJSONResponseｰ>パフォーマンス高い
    default_response_class=ORJSONResponse,
)

# ルーティングをinclude
app.include_router(samplerouter.router)
app.include_router(checkrouter.router, prefix="/check")
