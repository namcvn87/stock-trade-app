import time, random
from fastapi import APIRouter
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor, as_completed
from vnstock import Vnstock, Listing
from datetime import date
from sqlalchemy import func
from .. import models, database

router = APIRouter(prefix="/stocks", tags=["Stocks"])


# Dependency để lấy session DB
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


def fetch_and_save_symbol(symbol: str, row: dict):
    """Xử lý 1 mã cổ phiếu: lưu company + giá"""
    db = database.SessionLocal()
    inserted_companies = 0
    inserted_prices = 0

    try:
        # Insert thông tin công ty
        try:
            company = models.Company(
                symbol=symbol,
                exchange=row.get("exchange") or row.get("comGroupCode"),
                name=row.get("organName") or row.get("companyName"),
                industry=row.get("industryName") or row.get("icbName"),
                website=None,
                listing_date=None
            )
            db.merge(company)
            db.commit()
            inserted_companies += 1
        except Exception as e:
            db.rollback()
            print(f"❌ Lỗi lưu company {symbol}: {e}")

        # Xác định ngày bắt đầu (ngày cuối cùng đã lưu trong DB)
        last_date = db.query(func.max(models.StockPrice.date))\
                      .filter(models.StockPrice.symbol == symbol)\
                      .scalar()

        if last_date:
            start_date = last_date.strftime("%Y-%m-%d")
        else:
            start_date = "2008-01-01"

        # Delay ngẫu nhiên tránh rate-limit
        time.sleep(random.uniform(2, 4))

        try:
            stock = Vnstock().stock(symbol=symbol, source="VCI")
            df_prices = stock.quote.history(
                start=start_date,
                end=str(date.today()),
                interval="1D"
            )

            if df_prices is None or df_prices.empty:
                print(f"⚠️ Không có dữ liệu giá cho {symbol}")
            else:
                for _, p in df_prices.iterrows():
                    try:
                        price = models.StockPrice(
                            symbol=symbol,
                            date=p["time"],
                            open=p.get("open"),
                            high=p.get("high"),
                            low=p.get("low"),
                            close=p.get("close"),
                            volume=p.get("volume"),
                            value=p.get("value"),
                            change=p.get("change")
                        )
                        db.merge(price)
                        db.commit()   # commit từng dòng
                        inserted_prices += 1
                    except Exception as e:
                        db.rollback()
                        print(f"❌ Lỗi lưu giá {symbol}: {e}")

                print(f"✅ {symbol}: thêm {inserted_prices} bản ghi từ {start_date}")

        except Exception as e:
            print(f"❌ Lỗi fetch giá {symbol}: {e}")

    finally:
        db.close()

    return inserted_companies, inserted_prices


@router.post("/full_load")
def full_load():
    listing = Listing()
    df_symbols = listing.all_symbols(to_df=True)  # lấy toàn bộ HOSE, HNX, UPCOM

    # Detect tên cột mã cổ phiếu
    if "ticker" in df_symbols.columns:
        code_col = "ticker"
    elif "symbol" in df_symbols.columns:
        code_col = "symbol"
    elif "stockCode" in df_symbols.columns:
        code_col = "stockCode"
    else:
        return {"error": f"Không tìm thấy cột mã cổ phiếu trong {df_symbols.columns.tolist()}"}

    total_companies, total_prices = 0, 0

    # chạy song song với thread pool
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = []
        for _, row in df_symbols.iterrows():
            symbol = row[code_col]
            futures.append(executor.submit(fetch_and_save_symbol, symbol, row))

        for future in as_completed(futures):
            c, p = future.result()
            total_companies += c
            total_prices += p

    return {"companies": total_companies, "prices": total_prices}
