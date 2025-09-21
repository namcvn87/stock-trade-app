from pydantic import BaseModel
from datetime import date

class StockPriceBase(BaseModel):
    symbol: str
    date: date
    open: float
    high: float
    low: float
    close: float
    volume: float

    class Config:
        orm_mode = True
