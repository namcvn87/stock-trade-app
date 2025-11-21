import time
import logging
import pandas as pd
from vnstock import Listing, Finance
from app.database import SessionLocal
from app.models import Base, FinancialReport
import json
import os
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

CHECKPOINT_FILE = "fa_checkpoint.json"

# ================== Checkpoint ==================
def load_checkpoint():
    """Đọc checkpoint từ file JSON"""
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            data = json.load(f)
            return data.get("last_ticker")
    return None

def save_checkpoint(ticker):
    """Lưu checkpoint (ticker cuối cùng xử lý)"""
    with open(CHECKPOINT_FILE, "w") as f:
        json.dump({"last_ticker": ticker}, f)


# ================== Fetch dữ liệu từ vnstock ==================
def fetch_financial_df_for_ticker(ticker, source="VCI", period="quarter", lang="vi"):
    try:
        f = Finance(symbol=ticker, source=source)
        return {
            "income_statement": f.income_statement(period=period, lang=lang),
            "balance_sheet": f.balance_sheet(period=period, lang=lang),
            "cash_flow": f.cash_flow(period=period, lang=lang)
        }
    except Exception as e:
        raise e


# ================== DB Helpers ==================
def get_max_report_period(db: Session, ticker: str, report_type: str, period_type="quarter"):
    query = (
        db.query(FinancialReport)
        .filter(
            FinancialReport.ticker == ticker,
            FinancialReport.report_type == report_type,
            FinancialReport.period_type == period_type,
            FinancialReport.report_year != None,
            FinancialReport.report_year > 0,
        )
        .order_by(FinancialReport.report_year.desc())
    )

    if period_type == "quarter":
        query = query.order_by(FinancialReport.report_quarter.desc())

    record = query.first()

    if record:
        if period_type == "quarter":
            return record.report_year, record.report_quarter
        return record.report_year, 0

    return 0, 0

def get_all_tickers():
    """Thử nhiều cách lấy list tickers, trả về list string."""
    try:
        df = Listing.all_symbols()
        if isinstance(df, pd.DataFrame) and "symbol" in df.columns:
            return df['symbol'].dropna().unique().tolist()
    except Exception:
        pass

    try:
        listing = Listing()
        df = listing.all_symbols()
        if isinstance(df, pd.DataFrame) and "symbol" in df.columns:
            return df['symbol'].dropna().unique().tolist()
    except Exception:
        pass

    logger.error("Không thể lấy danh sách tickers từ vnstock.")
    return []


def compute_record_key(ticker, report_type, period_type, report_year, report_quarter, lang, item_name, idx):
    item_part = (str(item_name).strip() if item_name else f"row_{idx}")
    return f"{ticker}|{report_type}|{period_type}|{report_year}|{report_quarter}|{lang}|{item_part}"


# ================== Chuẩn hóa DataFrame ==================
def normalize_financial_df(df: pd.DataFrame, period_type="quarter") -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()
    df_new = df.copy()

    # Chuẩn hóa cột report_year
    if 'report_year' not in df_new.columns:
        if 'Năm' in df_new.columns:
            df_new['report_year'] = df_new['Năm']
        elif 'year' in df_new.columns:
            df_new['report_year'] = df_new['year']
        elif 'yearReport' in df_new.columns:
            df_new['report_year'] = df_new['yearReport']
        else:
            df_new['report_year'] = 0

    # Chuẩn hóa cột report_quarter
    if period_type == "quarter":
        if 'report_quarter' not in df_new.columns:
            if 'Kỳ' in df_new.columns:
                df_new['report_quarter'] = df_new['Kỳ'].astype(str).str.extract(r'(\d+)')
            elif 'quarter' in df_new.columns:
                df_new['report_quarter'] = df_new['quarter']
            elif 'lengthReport' in df_new.columns:
                df_new['report_quarter'] = df_new['lengthReport'].astype(str).str.extract(r'(\d+)')
            else:
                df_new['report_quarter'] = 0
    else:  # year
        df_new['report_quarter'] = 0

    # Ép kiểu int
    df_new['report_year'] = df_new['report_year'].fillna(0).astype(int)
    df_new['report_quarter'] = df_new['report_quarter'].fillna(0).astype(int)

    return df_new



# ================== Save vào DB ==================
def save_to_db(df: pd.DataFrame):
    """Lưu từng row vào bảng 'financial_reports'."""
    if df is None or df.empty:
        logger.info("Empty df -> skip save_to_db")
        return

    session = SessionLocal()
    skipped = 0
    try:
        for idx, row in df.reset_index(drop=True).iterrows():
            rowdict = row.to_dict()
            ticker = rowdict.get("ticker")
            report_type = rowdict.get("report_type")
            period_type = rowdict.get("period_type")
            report_year = rowdict.get("report_year")
            report_quarter = rowdict.get("report_quarter")
            lang = rowdict.get("lang")

            # tìm tên chỉ tiêu
            item_name = rowdict.get("Chỉ tiêu") or rowdict.get("item") or rowdict.get("label") or None
            record_key = compute_record_key(
                ticker, report_type, period_type, report_year, report_quarter, lang, item_name, idx
            )

            existing = session.query(FinancialReport).filter(
                FinancialReport.ticker == ticker,
                FinancialReport.report_type == report_type,
                FinancialReport.period_type == period_type,
                FinancialReport.report_year == report_year,
                FinancialReport.report_quarter == report_quarter
            ).first()

            if existing:
                skipped += 1
                continue

            new = FinancialReport(
                ticker=ticker,
                report_type=report_type,
                period_type=period_type,
                report_year=report_year,
                report_quarter=report_quarter,
                lang=lang,
                data=rowdict
            )
            session.add(new)

        session.commit()
        logger.info("Saved %d rows to DB (skipped %d)", len(df), skipped)
    except Exception as e:
        session.rollback()
        logger.exception("Error saving to DB: %s", e)
    finally:
        session.close()


# ================== Delta Load ==================
def delta_load_financials(tickers, source="VCI", period_types=["quarter"], symbol=None, lang="vi"):
    if not tickers:
        logger.error("Empty tickers list -> abort delta load")
        return

    if symbol:
        tickers = [t for t in tickers if t == symbol]
        if not tickers:
            logger.warning("Symbol %s không có trong danh sách tickers", symbol)
            return
        last_ticker = None
        skip_mode = False
        logger.info("Chạy riêng cho ticker %s (bỏ qua checkpoint)", symbol)
    else:
        last_ticker = load_checkpoint()
        skip_mode = bool(last_ticker)
        logger.info("Checkpoint last_ticker = %s", last_ticker)

    db = SessionLocal()
    for i, t in enumerate(tickers):
        if skip_mode:
            if t == last_ticker:
                skip_mode = False
            continue

        logger.info("Delta processing %d/%d: %s", i+1, len(tickers), t)

        for period in period_types:   # loop cả quarter và year
            try:
                reports = fetch_financial_df_for_ticker(t, source=source, period=period, lang=lang)
                if not reports:
                    continue

                for rname, df in reports.items():
                    if df is None or df.empty:
                        continue

                    # Lấy max period trong DB theo từng loại
                    max_year, max_quarter = get_max_report_period(db, t, rname, period)
                    if period == "quarter":
                        logger.info("Ticker %s %s quarter max in DB = %s-Q%s", t, rname, max_year, max_quarter)
                    else:
                        logger.info("Ticker %s %s year max in DB = %s", t, rname, max_year)

                    df_new = normalize_financial_df(df, period_type=period)
                    # Logic filter theo period_type
                    if period == "quarter":
                        if max_year == 0 and max_quarter == 0:
                            logger.info("Ticker %s %s quarter chưa có trong DB -> insert toàn bộ", t, rname)
                        else:
                            mask = (df_new['report_year'] > max_year) | (
                                (df_new['report_year'] == max_year) &
                                (df_new['report_quarter'] > max_quarter)
                            )
                            df_new = df_new.loc[mask]

                    elif period == "year":
                        if max_year == 0:
                            logger.info("Ticker %s %s year chưa có trong DB -> insert toàn bộ", t, rname)
                        else:
                            mask = df_new['report_year'] > max_year
                            df_new = df_new.loc[mask]

                    if df_new.empty:
                        logger.info("Không có dữ liệu mới cho %s %s %s", t, rname, period)
                        continue

                    df_new['lang'] = lang
                    df_new['ticker'] = t
                    df_new['report_type'] = rname
                    df_new['period_type'] = period

                    save_to_db(df_new)
                    time.sleep(1)
            except Exception as e:
                logger.exception("Lỗi khi delta-load %s: %s", t, e)

        if not symbol:
            save_checkpoint(t)
        time.sleep(1)
