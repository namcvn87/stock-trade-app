import time, random
from fastapi import APIRouter, Query
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor, as_completed
from vnstock import Vnstock, Listing
from datetime import date, timedelta
from sqlalchemy import func
from .. import models, database
from typing import List, Optional
from app.fa_full_load import fetch_finance_by_year, full_load, fetch_and_save_finance
from fastapi import HTTPException
import traceback

router = APIRouter(prefix="/stocks", tags=["Stocks"])


# Dependency để lấy session DB
def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()


def fetch_and_save_symbol(symbol: str, row: dict, start_date: str):
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

        # Delay ngẫu nhiên tránh rate-limit
        time.sleep(random.uniform(0.5, 2.0))

        stock = Vnstock().stock(symbol=symbol, source="VCI")

        try:
            df_prices = stock.quote.history(
                start=start_date,
                end=str(date.today()),
                interval="1D"
            )
        except ValueError as e:
            print(f"⚠️ {symbol}: không có dữ liệu hợp lệ ({e})")
            df_prices = None
        except Exception as e:
            print(f"❌ Lỗi fetch giá {symbol}: {e}")
            df_prices = None

        if df_prices is None or df_prices.empty:
            print(f"⚠️ Không có dữ liệu mới cho {symbol} từ {start_date}")
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
                    db.commit()
                    inserted_prices += 1
                except Exception as e:
                    db.rollback()
                    print(f"❌ Lỗi lưu giá {symbol}: {e}")

            print(f"✅ {symbol}: thêm {inserted_prices} bản ghi từ {start_date}")

    finally:
        db.close()

    return inserted_companies, inserted_prices


# ===================== API FULL LOAD =====================
@router.post("/full_load")
def full_load():
    listing = Listing()
    try:
        df_symbols = listing.all_symbols(to_df=True)  # lấy toàn bộ HOSE, HNX, UPCOM
    except Exception as e:
        return {"error": f"Không fetch được danh sách symbols: {e}"}

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
    errors = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for _, row in df_symbols.iterrows():
            symbol = row[code_col]
            futures[executor.submit(fetch_and_save_symbol, symbol, row, "2008-01-01")] = symbol

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                c, p = future.result()
                total_companies += c
                total_prices += p
            except Exception as e:
                print(f"❌ Không fetch được {symbol}: {e}")
                errors.append(symbol)

    return {
        "companies": total_companies,
        "prices": total_prices,
        "errors": errors
    }


# ===================== API DELTA LOAD =====================
@router.post("/delta_load")
def delta_load():
    listing = Listing()
    try:
        df_symbols = listing.all_symbols(to_df=True)
    except Exception as e:
        return {"error": f"Không fetch được danh sách symbols: {e}"}

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
    errors = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for _, row in df_symbols.iterrows():
            symbol = row[code_col]

            # Xác định ngày bắt đầu = ngày cuối cùng trong DB +1
            db = database.SessionLocal()
            last_date = db.query(func.max(models.StockPrice.date)) \
                          .filter(models.StockPrice.symbol == symbol) \
                          .scalar()
            db.close()

            if last_date:
                start_date = (last_date + timedelta(days=1)).strftime("%Y-%m-%d")
            else:
                start_date = "2008-01-01"

            futures[executor.submit(fetch_and_save_symbol, symbol, row, start_date)] = symbol

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                c, p = future.result()
                total_companies += c
                total_prices += p
            except Exception as e:
                print(f"❌ Không fetch được {symbol}: {e}")
                errors.append(symbol)

    return {
        "companies": total_companies,
        "prices": total_prices,
        "errors": errors
    }

# =====================FA API DELTA LOAD =====================
@router.post("/FA_delta-load/")
def delta_load(
    tickers: Optional[List[str]] = Query(None, description="Danh sách mã cổ phiếu, ví dụ: VNM,VCB"),
    year: Optional[int] = Query(None, description="Năm cần load, ví dụ: 2023")
):
    results = {}

    try:
        # 1. Không nhập tickers nhưng có year → toàn thị trường cho năm đó
        if not tickers and year:
            df_symbols = Listing().all_symbols(to_df=True)
            if df_symbols is None or df_symbols.empty:
                raise HTTPException(status_code=400, detail="Không lấy được danh sách symbol")

            code_col = "ticker" if "ticker" in df_symbols.columns else "symbol"
            if code_col not in df_symbols.columns:
                raise HTTPException(status_code=400, detail=f"Không tìm thấy cột {code_col} trong danh sách symbol")

            for _, row in df_symbols.iterrows():
                symbol = row[code_col]
                results[symbol] = fetch_finance_by_year(symbol, year)

            return {"year": year, "results": results}

        # 2. Không nhập tickers và không nhập year → full load
        if not tickers and not year:
            return {"mode": "full_load", "results": full_load()}

        # 3. Có tickers
        for ticker in tickers:
            if year:
                results[ticker] = fetch_finance_by_year(ticker, year)
            else:
                results[ticker] = fetch_and_save_finance(ticker)

        return {"year": year, "results": results}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=f"Lỗi server: {str(e)}")
