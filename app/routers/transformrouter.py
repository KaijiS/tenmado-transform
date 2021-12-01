from fastapi import APIRouter
from services import transformservice

router = APIRouter()


@router.post("/fillna")
async def fillna() -> dict[str:bool]:
    """
    週情報の翌日分が抜けている情報を数日予報の値でうめていく
    """

    comp_fillna: bool = transformservice.fillna()

    return {"coomp_fillna": comp_fillna}
