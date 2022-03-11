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


@router.post("/insert/kubun")
async def insert_kubun() -> dict[str:bool]:
    """
    集計結果をfirestoreにinsertしていく
    """

    firestoreservice.insert_kubun()

    return {"insert_kubun_to_firestore": True}
