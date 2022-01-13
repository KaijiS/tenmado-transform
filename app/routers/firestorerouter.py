from fastapi import APIRouter
from services import firestoreservice

router = APIRouter()


@router.post("/insert/weekweatner")
async def insertweekweatner() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insertweekweatner()

    return {"insert_to_firestore": True}


@router.post("/insert/molargearea")
async def insertmolargearea() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insertmolargearea()

    return {"insert_meteorologicalobservatorylargearea_to_firestore": True}
