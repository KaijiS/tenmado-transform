from fastapi import APIRouter
from services import firestoreservice

router = APIRouter()


@router.post("/insert/weekweatner")
async def insert_weekweatner() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insert_weekweatner()

    return {"insert_to_firestore": True}


@router.post("/insert/molargearea")
async def insert_molargearea() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insert_molargearea()

    return {"insert_meteorologicalobservatorylargearea_to_firestore": True}
