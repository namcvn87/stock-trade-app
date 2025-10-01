# loader.py
import logging, time, re 
import pandas as pd
from vnstock import Listing, Finance
from app.database import SessionLocal, engine
from app.models import Base, FinancialReport
import json, os

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# # Tạo bảng nếu chưa có
# Base.metadata.create_all(bind=engine)

CHECKPOINT_FILE = "fa_checkpoint.json"

def load_checkpoint():
    """Đọc checkpoint từ file JSON"""
    if not os.path.exists(CHECKPOINT_FILE):
        return None
    try:
        with open(CHECKPOINT_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("last_ticker")
    except Exception:
        return None

def save_checkpoint(ticker):
    """Ghi checkpoint (last_ticker) ra file JSON"""
    with open(CHECKPOINT_FILE, "w", encoding="utf-8") as f:
        json.dump({"last_ticker": ticker}, f)

def get_all_tickers():
    """Thử nhiều cách lấy list tickers, trả về list string."""
    # Cách 1: Listing.all_symbols (class method)
    try:
        df = Listing.all_symbols()
        if isinstance(df, pd.DataFrame) and "symbol" in df.columns:
            return df['symbol'].dropna().unique().tolist()
    except Exception:
        pass

    # Cách 2: Listing() instance .all_symbols()
    try:
        listing = Listing()
        df = listing.all_symbols()
        if isinstance(df, pd.DataFrame) and "symbol" in df.columns:
            return df['symbol'].dropna().unique().tolist()
    except Exception:
        pass

    # Thất bại -> trả về danh sách rỗng (caller phải xử lý)
    logger.error("Không thể lấy danh sách tickers từ vnstock.")
    return []

# def compute_record_key(ticker, report_type, period_type, report_year, report_quarter, item_name, idx):
#     item_part = (str(item_name).strip() if item_name else f"row_{idx}")
#     return f"{ticker}|{report_type}|{period_type}|{report_year}|{report_quarter}|{item_part}"

def save_to_db(df: pd.DataFrame):
    """
    Lưu từng row (row -> JSON) vào bảng 'financial_reports'.
    Trước khi insert, kiểm tra nếu đã có record (bằng record_key) thì update.
    """
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
            # tìm tên chỉ tiêu (nhiều version có cột 'Chỉ tiêu'/'item'/'label'...)
            # item_name = rowdict.get("Chỉ tiêu") or rowdict.get("item") or rowdict.get("label") or None
            # record_key = compute_record_key(ticker, report_type, period_type, report_year, report_quarter, lang, item_name, idx)

            # Dò xem đã có bản ghi này chưa: ta dùng điều kiện tương đương record_key bằng chuỗi
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

            # Nếu muốn chính xác theo item / record_key thì thay bằng filter tương ứng;
            # để đơn giản ở đây: nếu đã có một bản ghi cho ticker+type+period+year thì append (không trùng)
            # => ta chèn mới mỗi dòng (nhưng có thể nâng cấp thành kiểm tra record_key)
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
        logger.info("Saved %d rows to DB", len(df))
    except Exception as e:
        session.rollback()
        logger.exception("Error saving to DB: %s", e)
    finally:
        session.close()

def fetch_financial_df_for_ticker(ticker: str, source="VCI", period="quarter", lang="vi"):
    """
    Khởi tạo Finance đúng cách và gọi các method hiện có.
    Trả về dict of DataFrame: {'income_statement': df, ...}
    """
    results = {}
    try:
        finance_client = Finance(symbol=ticker, source=source)
    except TypeError as e:
        # Nếu Finance __init__ khác signature, thử khác tạo instance
        finance_client = Finance(ticker)  # fallback (nếu signature khác)

    # Các report types và các method tên khả dĩ
    map_methods = {
        "income_statement": ["income_statement", "income", "statement_income"],
        "balance_sheet": ["balance_sheet", "balance", "statement_balance"],
        "cash_flow": ["cash_flow", "cashflow", "cash"],
    }

    for rtype, candidates in map_methods.items():
        df = None
        for name in candidates:
            if hasattr(finance_client, name):
                func = getattr(finance_client, name)
                # gọi thử với nhiều signature
                try:
                    df = func(period=period, lang=lang, symbol=ticker)
                except TypeError:
                    try:
                        df = func(period=period, lang=lang)
                    except TypeError:
                        try:
                            df = func(period)
                        except TypeError:
                            try:
                                df = func()
                            except Exception:
                                df = None
                except Exception as e:
                    logger.debug("call %s failed: %s", name, e)
                if isinstance(df, pd.DataFrame):
                    break
        results[rtype] = df
    return results

# def test_fetch_one(ticker="VCB"):
#     """
#     Debug helper: fetch for one ticker and print shapes/cols.
#     """
#     results = fetch_financial_df_for_ticker(ticker, source="VCI", period="quarter")
#     for k, df in results.items():
#         if df is None:
#             print(f"{ticker} - {k}: None")
#         else:
#             print(f"{ticker} - {k}: rows={len(df)}, cols={df.columns.tolist()}")
#     return results

def full_load_financials(tickers, source="VCI", period_types=["quarter"], lang="vi"):
    if not tickers:
        logger.error("Empty tickers list -> abort full load")
        return
    
    # Load checkpoint
    last_ticker = load_checkpoint()
    skip_mode = bool(last_ticker)
    logger.info("[FULL LOAD] Checkpoint last_ticker = %s", last_ticker)

    for i, t in enumerate(tickers):
        if skip_mode:
            if t == last_ticker:
                skip_mode = False
            else:
                continue

        logger.info("Processing %d/%d: %s", i+1, len(tickers), t)
        for period in period_types:
            retry = 0
            while retry < 5:  # tối đa thử lại 5 lần
                try:
                    reports = fetch_financial_df_for_ticker(t, source=source, period=period)
                    if not reports:
                        logger.warning("Không có dữ liệu cho %s %s", t, period)
                        break

                    for rname, df in reports.items():
                        if df is None or df.empty:
                            logger.info("No data for %s %s %s", t, rname, period)
                            continue
                        df = df.copy()
                        # Chuẩn hóa cột report_year
                        if 'report_year' not in df.columns:
                            if 'Năm' in df.columns:  
                                df['report_year'] = df['Năm']
                            elif 'year' in df.columns:
                                df['report_year'] = df['year']
                            else:
                                raise ValueError("Không tìm thấy cột năm trong dữ liệu đầu vào")

                        # Chuẩn hóa cột report_quarter
                        if 'report_quarter' not in df.columns:
                            if 'Kỳ' in df.columns:  
                                # Lấy số từ chuỗi "Kỳ 2" -> 2
                                df['report_quarter'] = df['Kỳ'].astype(str).str.extract(r'(\d+)')
                            elif 'quarter' in df.columns:
                                df['report_quarter'] = df['quarter']
                            else:
                                # Nếu không có quarter (chẳng hạn báo cáo năm), gán mặc định = 0
                                df['report_quarter'] = 0

                        # Convert về int để chắc chắn không null
                        df['report_year'] = df['report_year'].astype(int)
                        df['report_quarter'] = df['report_quarter'].astype(int)
                        df['ticker'] = t
                        df['lang']=lang
                        df['report_type'] = rname
                        df['period_type'] = period
                        save_to_db(df)

                    break  # thành công thì thoát vòng retry

                except Exception as e:
                    err = str(e)
                    if "Rate limit exceeded" in err:
                        # tìm số giây trong message
                        wait_time = 30
                        match = re.search(r"(\d+)\s*giây", err)
                        if match:
                            wait_time = int(match.group(1))
                        logger.warning("⚠️ Bị rate limit khi fetch %s. Chờ %d giây...", t, wait_time)
                        time.sleep(wait_time + 2)  # chờ thêm cho chắc
                        retry += 1
                    else:
                        logger.exception("Lỗi khác với %s: %s", t, e)
                        break
        # ✅ Lưu checkpoint sau khi xong ticker
        save_checkpoint(t)
        time.sleep(1)  # nghỉ 1s giữa các ticker