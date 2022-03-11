from fastapi import APIRouter
from services import firestoreservice

router = APIRouter()


@router.post("/insert/weekweatner")
async def insert_weekweatner() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insert_weekweather()

    return {"insert_to_firestore": True}


@router.post("/insert/molargearea")
async def insert_molargearea() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insert_molargearea()

    return {"insert_meteorologicalobservatorylargearea_to_firestore": True}


@router.post("/insert/kubunmolargearea")
async def insert_kubunmolargearea() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insert_kubunmolargearea()

    return {"insert_kubunmeteorologicalobservatorylargearea_to_firestore": True}
