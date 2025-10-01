from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app.models import FinancialReport

router = APIRouter()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.get("/report/")
def get_report(
    ticker: str,
    report_type: str = Query(..., regex="(balance_sheet|income_statement|cash_flow)"),
    report_year: int = None,
    report_quarter: int = None,
    db: Session = Depends(get_db)
):
    query = db.query(FinancialReport.data).filter(
        FinancialReport.ticker == ticker,
        FinancialReport.report_type == report_type.lower()
    )
    if report_year:
        query = query.filter(FinancialReport.report_year == report_year)
    if report_quarter:
        query = query.filter(FinancialReport.report_quarter == report_quarter)

    result = query.all()
    if not result:
        raise HTTPException(status_code=404, detail="Data not found")

    return [row[0] for row in result]
