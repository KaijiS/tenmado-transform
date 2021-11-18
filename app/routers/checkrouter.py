from fastapi import APIRouter

router = APIRouter()


@router.get("/load")
async def check_load() -> dict[str:bool]:
    """
    load処理が完了したかの確認を行う
    """

    check_flag: bool = checkservice.check_load()

    return {"check_load": check_flag}
