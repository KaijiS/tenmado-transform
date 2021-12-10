from fastapi import APIRouter
from services import firestoreservice

router = APIRouter()


@router.post("/insert")
async def insert() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insert()

    return {"insert_to_firestore": True}
