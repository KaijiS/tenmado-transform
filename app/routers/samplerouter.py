from fastapi import APIRouter

router = APIRouter()


@router.get("/")
async def connection_check() -> dict[str:str]:
    """
    意思疎通確認用API
    """
    return {"接続": "確認！"}
