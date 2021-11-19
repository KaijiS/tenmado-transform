from fastapi import APIRouter
from services import checkservice

router = APIRouter()


@router.get("/load")
async def check_load() -> dict[str:bool]:
    """
    load処理が完了したかの確認を行う
    """

    check_flag: bool = checkservice.check_load()

    return {"checkLoad": check_flag}
