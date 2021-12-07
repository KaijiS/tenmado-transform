from fastapi import APIRouter
from services import transformservice

router = APIRouter()


@router.post("/fillna")
async def fillna() -> dict[str:bool]:
    """
    週情報の翌日分が抜けている情報を数日予報の値でうめていく
    """

    transformservice.fillna()

    return {"coomp_fillna": True}


@router.post("/concat")
async def concat() -> dict[str:bool]:
    """
    週天気と週気温をjoinする
    """

    transformservice.concat()

    return {"comp_concat": True}
