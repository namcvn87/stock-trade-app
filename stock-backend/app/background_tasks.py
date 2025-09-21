from apscheduler.schedulers.background import BackgroundScheduler
from datetime import date
from vnstock import Vnstock
from .database import SessionLocal
from . import models

def update_daily():
    db = SessionLocal()
    symbols = db.query(models.Company.symbol).all()

    for (symbol,) in symbols:
        stock = Vnstock().stock(symbol=symbol, source="VCI")
        try:
            df = stock.quote.history(start=str(date.today()), end=str(date.today()), interval="1D")
            for _, p in df.iterrows():
                price = models.StockPrice(
                    symbol=symbol,
                    date=p["time"],
                    open=p["open"],
                    high=p["high"],
                    low=p["low"],
                    close=p["close"],
                    volume=p["volume"],
                    value=p.get("value"),
                    change=p.get("change")
                )
                db.merge(price)
        except:
            continue

    db.commit()
    db.close()

def start_scheduler():
    scheduler = BackgroundScheduler()
    scheduler.add_job(update_daily, "cron", hour=13, minute=0)  # chạy mỗi 13h
    scheduler.start()
