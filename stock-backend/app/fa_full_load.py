import datetime, time, random
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from vnstock import Vnstock, Listing

from .database import SessionLocal
from . import models
import pandas as pd


# ===================== Helpers =====================
def normalize_report_date(value, period_type="year"):
    try:
        year = int(value)
        if period_type == "year":
            return datetime.date(year, 12, 31)
        return datetime.date(year, 12, 31)  # fallback
    except Exception:
        return None


# ===== Mapping cho IncomeStatement =====
def map_income_row(row):
    return {
        "doanh_thu": row.get("Doanh thu (đồng)"),
        "tang_truong_doanh_thu": row.get("Tăng trưởng doanh thu (%)"),
        "loi_nhuan_sau_thue_cty_me": row.get("Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)"),
        "tang_truong_loi_nhuan": row.get("Tăng trưởng lợi nhuận (%)"),
        "thu_nhap_lai_va_tuong_tu": row.get("Thu nhập lãi và các khoản tương tự"),
        "chi_phi_lai_va_tuong_tu": row.get("Chi phí lãi và các khoản tương tự"),
        "thu_nhap_lai_thuan": row.get("Thu nhập lãi thuần"),
        "thu_nhap_dich_vu": row.get("Thu nhập từ hoạt động dịch vụ"),
        "chi_phi_dich_vu": row.get("Chi phí hoạt động dịch vụ"),
        "lai_thuan_dich_vu": row.get("Lãi thuần từ hoạt động dịch vụ"),
        "kinh_doanh_ngoai_hoi_vang": row.get("Kinh doanh ngoại hối và vàng"),
        "chung_khoan_kinh_doanh": row.get("Chứng khoán kinh doanh"),
        "chung_khoan_dau_tu": row.get("Chứng khoán đầu tư"),
        "hoat_dong_khac": row.get("Hoạt động khác"),
        "chi_phi_hoat_dong_khac": row.get("Chi phí hoạt động khác"),
        "lai_lo_thuan_hd_khac": row.get("Lãi/lỗ thuần từ hoạt động khác"),
        "co_tuc_da_nhan": row.get("Cố tức đã nhận"),
        "tong_thu_nhap_hd": row.get("Tổng thu nhập hoạt động"),
        "chi_phi_quan_ly_dn": row.get("Chi phí quản lý DN"),
        "ln_truoc_du_phong": row.get("LN từ HĐKD trước CF dự phòng"),
        "chi_phi_du_phong_rui_ro": row.get("Chi phí dự phòng rủi ro tín dụng"),
        "ln_truoc_thue": row.get("LN trước thuế"),
        "thue_tndn": row.get("Thuế TNDN"),
        "thue_tndn_hien_hanh": row.get("Chi phí thuế TNDN hiện hành"),
        "thue_tndn_hoan_lai": row.get("Chi phí thuế TNDN hoãn lại"),
        "co_dong_thieu_so": row.get("Cổ đông thiểu số"),
        "loi_nhuan_thuan": row.get("Lợi nhuận thuần"),
        "loi_nhuan_cty_me": row.get("Cổ đông của Công ty mẹ"),
        "eps": row.get("Lãi cơ bản trên cổ phiếu"),
    }


# ===== Mapping cho BalanceSheet =====
def map_balance_row(row):
    return {
        "tong_tai_san": row.get("TỔNG CỘNG TÀI SẢN (đồng)"),
        "tien_va_tuong_duong": row.get("Tiền và tương đương tiền (đồng)"),
        "tien_gui_nhnn": row.get("Tiền gửi tại ngân hàng nhà nước Việt Nam"),
        "tien_gui_tctd": row.get("Tiền gửi tại các TCTD khác và cho vay các TCTD khác"),
        "ck_kinh_doanh": row.get("Chứng khoán kinh doanh"),
        "ck_kinh_doanh_khac": row.get("_Chứng khoán kinh doanh"),
        "du_phong_ck_kinh_doanh": row.get("Dự phòng giảm giá chứng khoán kinh doanh"),
        "cong_cu_phai_sinh": row.get("Các công cụ tài chính phái sinh và khoản nợ tài chính khác"),
        "cho_vay_khach_hang": row.get("Cho vay khách hàng"),
        "cho_vay_khach_hang_khac": row.get("_Cho vay khách hàng"),
        "du_phong_cho_vay": row.get("Dự phòng rủi ro cho vay khách hàng"),
        "ck_dau_tu": row.get("Chứng khoán đầu tư"),
        "ck_san_sang_ban": row.get("Chứng khoán đầu tư sẵn sàng để bán"),
        "ck_giu_den_dao_han": row.get("Chứng khoán đầu tư giữ đến ngày đáo hạn"),
        "du_phong_ck_dau_tu": row.get("Dự phòng giảm giá chứng khoán đầu tư"),
        "dau_tu_dai_han": row.get("Đầu tư dài hạn (đồng)"),
        "dau_tu_lien_doanh": row.get("Đầu tư vào công ty liên doanh"),
        "tai_san_dai_han_khac": row.get("Tài sản dài hạn khác (đồng)"),
        "du_phong_dau_tu_dai_han": row.get("Dự phòng giảm giá đầu tư dài hạn"),
        "tai_san_co_dinh": row.get("Tài sản cố định (đồng)"),
        "tscd_huu_hinh": row.get("Tài sản cố định hữu hình"),
        "tscd_vo_hinh": row.get("Tài sản cố định vô hình"),
        "tai_san_co_khac": row.get("Tài sản Có khác"),
        "tong_nguon_von": row.get("TỔNG CỘNG NGUỒN VỐN (đồng)"),
        "no_phai_tra": row.get("NỢ PHẢI TRẢ (đồng)"),
        "no_cp_va_nhnn": row.get("Các khoản nợ chính phủ và NHNN Việt Nam"),
        "no_tctd": row.get("Tiền gửi và vay các Tổ chức tín dụng khác"),
        "tien_gui_khach_hang": row.get("Tiền gửi của khách hàng"),
        "cong_cu_phai_sinh_no": row.get("_Các công cụ tài chính phái sinh và khoản nợ tài chính khác"),
        "von_tai_tro_uy_thac": row.get("Vốn tài trợ, uỷ thác đầu tư của CP và các tổ chức TD khác"),
        "phat_hanh_giay_to": row.get("Phát hành giấy tờ có giá"),
        "no_khac": row.get("Các khoản nợ khác"),
        "von_chu_so_huu": row.get("VỐN CHỦ SỞ HỮU (đồng)"),
        "von_to_chuc_td": row.get("Vốn của tổ chức tín dụng"),
        "quy_to_chuc_td": row.get("Quỹ của tổ chức tín dụng"),
        "chenh_lech_ty_gia": row.get("Chênh lệch tỷ giá hối đoái"),
        "chenh_lech_danh_gia": row.get("Chênh lệch đánh giá lại tài sản"),
        "loi_nhuan_chua_pp": row.get("Lãi chưa phân phối (đồng)"),
        "von_gop_chu_so_huu": row.get("Vốn góp của chủ sở hữu (đồng)"),
        "cac_quy_khac": row.get("Các quỹ khác"),
        "loi_ich_cd_thieu_so": row.get("LỢI ÍCH CỦA CỔ ĐÔNG THIỂU SỐ"),
    }


# ===== Mapping cho CashFlow =====
def map_cashflow_row(row):
    return {
        "lai_lo_hd_khac": row.get("(Lãi)/lỗ các hoạt động khác"),
        "lc_tu_hd_kd_truoc_vld": row.get("Lưu chuyển tiền thuần từ HĐKD trước thay đổi VLĐ"),
        "lc_tu_hd_kd_truoc_thue": row.get("Lưu chuyển tiền thuần từ HĐKD trước thuế"),
        "chi_tu_cac_quy_tctd": row.get("Chi từ các quỹ của TCTD"),
        "mua_sam_tscd": row.get("Mua sắm TSCĐ"),
        "tien_thu_co_tuc": row.get("Tiền thu cổ tức và lợi nhuận được chia"),
        "lc_tu_hd_dau_tu": row.get("Lưu chuyển từ hoạt động đầu tư"),
        "tang_von_co_phan": row.get("Tăng vốn cổ phần từ góp vốn và/hoặc phát hành cổ phiếu"),
        "lc_tu_hd_tc": row.get("Lưu chuyển tiền từ hoạt động tài chính"),
        "lc_thuan_trong_ky": row.get("Lưu chuyển tiền thuần trong kỳ"),
        "tien_va_tuong_duong": row.get("Tiền và tương đương tiền"),
        "tien_va_tuong_duong_ck": row.get("Tiền và tương đương tiền cuối kỳ"),
        "lc_rong_hd_sxkd": row.get("Lưu chuyển tiền tệ ròng từ các hoạt động SXKD"),
        "tien_thu_thanh_ly_tscd": row.get("Tiền thu được từ thanh lý tài sản cố định"),
        "dau_tu_dn_khac": row.get("Đầu tư vào các doanh nghiệp khác"),
        "tien_thu_ban_dau_tu": row.get("Tiền thu từ việc bán các khoản đầu tư vào doanh nghiệp khác"),
        "co_tuc_da_tra": row.get("Cổ tức đã trả"),
    }


# ===================== Fetch + Save =====================
def save_finance(symbol: str, df_income=None, df_balance=None, df_cashflow=None, period="year", lang="vi"):
    db = SessionLocal()
    rows = {"income": 0, "balance": 0, "cashflow": 0}


    try:
        # Income
        if df_income is not None and not df_income.empty:
            for _, row in df_income.iterrows():
                insert_stmt = insert(models.IncomeStatement).values(
                    symbol=symbol,
                    report_date=normalize_report_date(row["Năm"], period),
                    period_type=period,
                    lang=lang,
                    source="VCI",
                    **map_income_row(row)
                )
                stmt = insert_stmt.on_conflict_do_update(
                    constraint="uq_income_unique", set_=map_income_row(row)
                )
                db.execute(stmt)
                rows["income"] += 1

        # Balance
        if df_balance is not None and not df_balance.empty:
            for _, row in df_balance.iterrows():
                insert_stmt = insert(models.BalanceSheet).values(
                    symbol=symbol,
                    report_date=normalize_report_date(row["Năm"], period),
                    period_type=period,
                    lang=lang,
                    source="VCI",
                    **map_balance_row(row)
                )
                stmt = insert_stmt.on_conflict_do_update(
                    constraint="uq_balance_unique", set_=map_balance_row(row)
                )
                db.execute(stmt)
                rows["balance"] += 1

        # CashFlow
        if df_cashflow is not None and not df_cashflow.empty:
            for _, row in df_cashflow.iterrows():
                insert_stmt = insert(models.CashFlow).values(
                    symbol=symbol,
                    report_date=normalize_report_date(row["Năm"], period),
                    period_type=period,
                    lang=lang,
                    source="VCI",
                    **map_cashflow_row(row)
                )
                stmt = insert_stmt.on_conflict_do_update(
                    constraint="uq_cashflow_unique", set_=map_cashflow_row(row)
                )
                db.execute(stmt)
                rows["cashflow"] += 1

        db.commit()

    except Exception as e:
        db.rollback()
        print(f"❌ {symbol} Error saving: {e}")
    finally:
        db.close()

    return rows

def fetch_and_save_finance(symbol: str, period="year", lang="vi"):
    
    # Delay ngẫu nhiên tránh rate-limit
    time.sleep(random.uniform(0.5, 2.0))
    stock = Vnstock().stock(symbol=symbol, source="VCI")
    df_income = stock.finance.income_statement(period=period, lang=lang)
    df_balance = stock.finance.balance_sheet(period=period, lang=lang)
    df_cf = stock.finance.cash_flow(period=period, lang=lang)
    return save_finance(symbol, df_income, df_balance, df_cf, period, lang)

# ===================== FULL LOAD =====================
def full_load():
    listing = Listing()
    try:
        df_symbols = listing.all_symbols(to_df=True)
    except Exception as e:
        print(f"❌ Không fetch được danh sách symbols: {e}")
        return

    code_col = "ticker" if "ticker" in df_symbols.columns else "symbol"

    total = {"income": 0, "balance": 0, "cashflow": 0}

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = {}
        for _, row in df_symbols.iterrows():
            symbol = row[code_col]
            futures[executor.submit(fetch_and_save_finance, symbol)] = symbol

        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                total["income"] += result["income"]
                total["balance"] += result["balance"]
                total["cashflow"] += result["cashflow"]
                print(f"✅ {symbol}: Income={result['income']}, Balance={result['balance']}, CF={result['cashflow']}")
            except Exception as e:
                print(f"❌ {symbol} Error: {e}")

    print("===== FULL LOAD DONE =====")
    print(total)
    return total


if __name__ == "__main__":
    full_load()

# Delta load fetch finance by year and ma co phieu
def fetch_finance_by_year(symbol: str, year: int, lang="vi"):
    
    # Delay ngẫu nhiên tránh rate-limit
    time.sleep(random.uniform(0.5, 2.0))
    stock = Vnstock().stock(symbol=symbol, source="VCI")
    period = "year"

    # Income
    df_income = stock.finance.income_statement(period=period, lang=lang)
    if df_income is not None and not df_income.empty:
        df_income["Năm"] = df_income["Năm"].astype(str)
        df_income = df_income[df_income["Năm"] == str(year)]
    else:
        df_income = None

    # Balance
    df_balance = stock.finance.balance_sheet(period=period, lang=lang)
    if df_balance is not None and not df_balance.empty:
        df_balance["Năm"] = df_balance["Năm"].astype(str)
        df_balance = df_balance[df_balance["Năm"] == str(year)]
    else:
        df_balance = None

    # CashFlow
    df_cf = stock.finance.cash_flow(period=period, lang=lang)
    if df_cf is not None and not df_cf.empty:
        df_cf["Năm"] = df_cf["Năm"].astype(str)
        df_cf = df_cf[df_cf["Năm"] == str(year)]
    else:
        df_cf = None

    return save_finance(symbol, df_income, df_balance, df_cf, period, lang)

