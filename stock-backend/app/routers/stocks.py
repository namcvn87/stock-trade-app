import time, random
from fastapi import APIRouter, Query
from sqlalchemy.orm import Session
from concurrent.futures import ThreadPoolExecutor, as_completed
from vnstock import Vnstock, Listing
from datetime import date, timedelta
from sqlalchemy import func
from .. import models, database

router = APIRouter(prefix="/stocks", tags=["Stocks"])

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import func, or_
from datetime import date, timedelta, datetime


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
        time.sleep(random.uniform(1.0, 3.0))

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

# ===================== API TODAY LOAD =====================
@router.post("/today_load")
def today_load():
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

    today = date.today().strftime("%Y-%m-%d")
    total_companies, total_prices = 0, 0
    errors = []

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for _, row in df_symbols.iterrows():
            symbol = row[code_col]
            # chỉ lấy dữ liệu ngày hôm nay
            futures[executor.submit(fetch_and_save_symbol, symbol, row, today)] = symbol

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
        "date": today,
        "companies": total_companies,
        "prices": total_prices,
        "errors": errors
    }



# ===================== GET API: COMPANIES =====================
@router.get("/companies")
def list_companies(
    q: str | None = Query(default=None, description="Tìm theo mã hoặc tên"),
    exchange: str | None = Query(default=None),
    industry: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db)
):
    query = db.query(models.Company)
    if q:
        like = f"%{q.strip()}%"
        query = query.filter(or_(models.Company.symbol.ilike(like),
                                 models.Company.name.ilike(like)))
    if exchange:
        query = query.filter(models.Company.exchange == exchange)
    if industry:
        query = query.filter(models.Company.industry == industry)

    total = query.count()
    rows = query.order_by(models.Company.symbol.asc()).limit(limit).offset(offset).all()
    return {"data": rows, "meta": {"total": total, "limit": limit, "offset": offset}}


@router.get("/companies/{symbol}")
def get_company(symbol: str, db: Session = Depends(get_db)):
    row = db.query(models.Company).get(symbol.upper())
    if not row:
        raise HTTPException(status_code=404, detail="Company not found")
    return row


# ===================== GET API: DAILY PRICES (OHLCV) =====================
@router.get("/prices/{symbol}")
def get_prices(
    symbol: str,
    start: str | None = Query(default=None, description="YYYY-MM-DD"),
    end: str | None = Query(default=None, description="YYYY-MM-DD"),
    order: str = Query(default="asc", regex="^(?i)(asc|desc)$"),
    limit: int = Query(default=200, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db)
):
    q = db.query(models.StockPrice).filter(models.StockPrice.symbol == symbol.upper())
    if start:
        q = q.filter(models.StockPrice.date >= datetime.strptime(start, "%Y-%m-%d").date())
    if end:
        q = q.filter(models.StockPrice.date <= datetime.strptime(end, "%Y-%m-%d").date())

    q = q.order_by(models.StockPrice.date.desc() if order.lower() == "desc" else models.StockPrice.date.asc())
    total = q.count()
    rows = q.limit(limit).offset(offset).all()
    return {"data": rows, "meta": {"total": total, "limit": limit, "offset": offset}}


@router.get("/prices/{symbol}/latest")
def get_latest_price(symbol: str, db: Session = Depends(get_db)):
    row = (db.query(models.StockPrice)
           .filter(models.StockPrice.symbol == symbol.upper())
           .order_by(models.StockPrice.date.desc())
           .first())
    if not row:
        raise HTTPException(status_code=404, detail="No price found")
    return row


# ===================== GET API: TỔNG HỢP THEO NGÀY & SÀN =====================
@router.get("/daily")
def get_daily_by_exchange(
    date_str: str = Query(..., alias="date", description="YYYY-MM-DD"),
    exchange: str | None = Query(default=None, description="HOSE|HNX|UPCOM"),
    order_by: str = Query(default="symbol", regex="^(?i)(symbol|volume|value|change|close)$"),
    order: str = Query(default="asc", regex="^(?i)(asc|desc)$"),
    limit: int = Query(default=500, ge=1, le=5000),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db)
):
    """
    Trả về toàn bộ thông tin giao dịch (OHLCV) của *một ngày*,
    có thể lọc theo sàn (exchange). Kết hợp bảng companies + stock_prices.
    """
    try:
        day = datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=422, detail="Sai định dạng ngày, dùng YYYY-MM-DD")

    # Chọn cột cần thiết
    q = (db.query(
            models.StockPrice.symbol,
            models.Company.name.label("company_name"),
            models.Company.exchange,
            models.StockPrice.date,
            models.StockPrice.open,
            models.StockPrice.high,
            models.StockPrice.low,
            models.StockPrice.close,
            models.StockPrice.volume,
            models.StockPrice.value,
            models.StockPrice.change
        )
        .join(models.Company, models.Company.symbol == models.StockPrice.symbol)
        .filter(models.StockPrice.date == day)
    )

    if exchange:
        q = q.filter(models.Company.exchange == exchange)

    # Map trường sắp xếp
    order_map = {
        "symbol": models.StockPrice.symbol,
        "volume": models.StockPrice.volume,
        "value":  models.StockPrice.value,
        "change": models.StockPrice.change,
        "close":  models.StockPrice.close,
    }
    order_col = order_map[order_by.lower()]
    q = q.order_by(order_col.desc() if order.lower() == "desc" else order_col.asc())

    total = q.count()
    rows = q.limit(limit).offset(offset).all()

    # Chuẩn hóa output thành list[dict]
    data = []
    for r in rows:
        data.append({
            "symbol": r.symbol,
            "company_name": r.company_name,
            "exchange": r.exchange,
            "date": r.date,
            "open": r.open,
            "high": r.high,
            "low": r.low,
            "close": r.close,
            "volume": r.volume,
            "value": r.value,
            "change": r.change
        })

    return {"data": data, "meta": {"total": total, "limit": limit, "offset": offset}}
