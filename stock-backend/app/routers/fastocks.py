from fastapi import Query
from fastapi import APIRouter
from vnstock import Vnstock, Listing
from typing import List, Optional
from app.fa_full_load import fetch_finance_by_year, full_load, fetch_and_save_finance
from fastapi import HTTPException
import traceback

# =====================FA API DELTA LOAD =====================

router = APIRouter(prefix="/fastocks", tags=["FaStocks"])
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