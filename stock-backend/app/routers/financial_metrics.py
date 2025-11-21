from fastapi import APIRouter, Query
import psycopg2
from psycopg2.extras import RealDictCursor
from decimal import Decimal
from app.models import FinancialGrowthReport
from app.database import SessionLocal
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy import text

router = APIRouter(prefix="/metrics", tags=["Metrics"])

# 🧩 Kết nối database
def get_connection():
    return psycopg2.connect(
        host="localhost",
        dbname="stockdb",
        user="postgres",
        password="2110",
        port="5432"
    )

# 🧮 Hàm tính tăng trưởng
def calc_growth(ticker: str, year: int, quarter: int):
    conn = get_connection()
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 🟢 Hàm phụ để lấy LNST, Doanh thu, EPS cho 1 kỳ
    def get_income_data(y, q):
        cur.execute("""
            SELECT
                (fr.data ->> 'Lợi nhuận thuần')::numeric AS loi_nhuan_sau_thue_tndn,
                (fr.data ->> 'Doanh thu thuần')::numeric AS doanh_thu,
                CASE 
                    WHEN ish.issue_share > 0 
                    THEN ROUND(((fr.data ->> 'Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)')::numeric) / ish.issue_share, 2)
                    ELSE NULL
                END AS eps
            FROM financial_reports fr
            LEFT JOIN issue_shares ish 
                ON fr.ticker = ish.symbol
            WHERE fr.report_type = 'income_statement'
              AND fr.ticker = %s
              AND fr.report_year = %s
              AND fr.period_type = 'quarter'
              AND fr.report_quarter IN (1, 2, 3, 4)
              AND fr.report_quarter = %s;
        """, (ticker, y, q))
        data = cur.fetchone()
        return data

    # 🟢 Lấy YoY cho 1 quý (LNST, DT, EPS)
    def get_yoy_for_quarter(y, q):
        now = get_income_data(y, q)
        prev_y = get_income_data(y - 1, q)
        if not now or not prev_y:
            return (None, None, None)

        def calc_yoy(now_val, prev_val):
            if now_val is None or prev_val is None or Decimal(prev_val) == 0:
                return None
            return (Decimal(now_val) / Decimal(prev_val) - 1) * 100

        lnst_yoy = calc_yoy(now["loi_nhuan_sau_thue_tndn"], prev_y["loi_nhuan_sau_thue_tndn"])
        dt_yoy   = calc_yoy(now["doanh_thu"], prev_y["doanh_thu"])
        eps_yoy  = calc_yoy(now["eps"], prev_y["eps"])
        return (lnst_yoy, dt_yoy, eps_yoy)

    # ======================================================
    # 1️⃣ Tăng trưởng hiện tại và xu hướng (lợi nhuận, doanh thu, EPS)
    # ======================================================
    now = get_income_data(year, quarter)
    prev_yoy = get_income_data(year - 1, quarter)
    if not now or not prev_yoy:
        now = now or {"loi_nhuan_sau_thue_tndn": 0, "doanh_thu": 0, "eps": 0}
        prev_yoy = prev_yoy or {"loi_nhuan_sau_thue_tndn": 0, "doanh_thu": 0, "eps": 0}

    def safe_growth(now_val, prev_val):
        if prev_val and Decimal(prev_val) != 0:
            return (Decimal(now_val) / Decimal(prev_val) - 1) * 100
        return None

    lnst_growth_now = safe_growth(now["loi_nhuan_sau_thue_tndn"], prev_yoy["loi_nhuan_sau_thue_tndn"])
    dt_growth_now   = safe_growth(now["doanh_thu"], prev_yoy["doanh_thu"])
    eps_growth_now  = safe_growth(now["eps"], prev_yoy["eps"])

    # ======================================================
    # 2️⃣ Xu hướng so với quý trước
    # ======================================================
    prev_quarter = quarter - 1
    prev_quarter_year = year
    if prev_quarter == 0:
        prev_quarter = 4
        prev_quarter_year -= 1

    prev_q = get_income_data(prev_quarter_year, prev_quarter)
    prev_q_yoy = get_income_data(prev_quarter_year - 1, prev_quarter)

    lnst_growth_prev_q = safe_growth(prev_q["loi_nhuan_sau_thue_tndn"], prev_q_yoy["loi_nhuan_sau_thue_tndn"]) if prev_q and prev_q_yoy else None
    dt_growth_prev_q   = safe_growth(prev_q["doanh_thu"], prev_q_yoy["doanh_thu"]) if prev_q and prev_q_yoy else None

    lnst_trend = "Tăng tốc" if (lnst_growth_now and lnst_growth_prev_q and lnst_growth_now > lnst_growth_prev_q) else "Giảm tốc"
    dt_trend   = "Tăng tốc" if (dt_growth_now and dt_growth_prev_q and dt_growth_now > dt_growth_prev_q) else "Giảm tốc"

    # ======================================================
    # 3️⃣ Tốc độ tăng trưởng 3 quý gần nhất (LNST, DT, EPS)
    # ======================================================
    quarters = []
    y, q = year, quarter
    for _ in range(4):  # lấy 4 quý liên tiếp (bao gồm quý hiện tại)
        quarters.append((y, q))
        q -= 1
        if q == 0:
            q = 4
            y -= 1
    quarters.reverse()

    yoy_list = []
    for y, q in quarters:
        lnst_y, dt_y, eps_y = get_yoy_for_quarter(y, q)
        yoy_list.append({"year": y, "quarter": q, "lnst_yoy": lnst_y, "dt_yoy": dt_y, "eps_yoy": eps_y})

    statuses_ln, statuses_dt, statuses_eps = [], [], []

    for i in range(1, len(yoy_list)):
        prev, curq = yoy_list[i - 1], yoy_list[i]
        if prev["lnst_yoy"] is not None and curq["lnst_yoy"] is not None:
            statuses_ln.append("Tăng tốc" if curq["lnst_yoy"] > prev["lnst_yoy"] else "Giảm tốc")
        if prev["dt_yoy"] is not None and curq["dt_yoy"] is not None:
            statuses_dt.append("Tăng tốc" if curq["dt_yoy"] > prev["dt_yoy"] else "Giảm tốc")
        if prev["eps_yoy"] is not None and curq["eps_yoy"] is not None:
            statuses_eps.append("Tăng tốc" if curq["eps_yoy"] > prev["eps_yoy"] else "Giảm tốc")

    def majority_status(sts):
        if not sts or len(sts) < 3:
            return "Không đủ dữ liệu"
        up = sts.count("Tăng tốc")
        return "Tăng tốc" if up >= 2 else "Giảm tốc"

    toc_do_3q_ln  = majority_status(statuses_ln[-3:])
    toc_do_3q_dt  = majority_status(statuses_dt[-3:])
    toc_do_3q_eps = majority_status(statuses_eps[-3:])

    # 🔹 Đếm số quý EPS tăng tốc trong 3 quý gần nhất
    eps_up_count = statuses_eps[-3:].count("Tăng tốc") if len(statuses_eps) >= 3 else None
    so_quy_tang_ln = statuses_ln[-3:].count("Tăng tốc")
    so_quy_tang_dt = statuses_dt[-3:].count("Tăng tốc")

    # ======================================================
    # Tăng trưởng lợi nhuận năm gần nhất
    # ======================================================
    cur.execute("""
        SELECT 
            report_year,
            (data ->> 'Lợi nhuận thuần')::numeric AS loi_nhuan_sau_thue_tndn
        FROM financial_reports fr
        WHERE fr.ticker = %s
          AND fr.report_type = 'income_statement'
          AND fr.period_type = 'year'
          AND fr.report_year IN (%s, %s)
        ORDER BY report_year;
    """, (ticker, year - 2, year - 1))
    rows = cur.fetchall()
    nam_du_lieu_nam_gan_nhat = year - 1
    tang_truong_loi_nhuan_nam = None
    if len(rows) == 2:
        data = {r["report_year"]: r["loi_nhuan_sau_thue_tndn"] for r in rows}
        current = data.get(year - 1)
        prev = data.get(year - 2)
        if current and prev and Decimal(prev) != 0:
            tang_truong_loi_nhuan_nam = (Decimal(current) / Decimal(prev) - 1) * 100

# ======================================================
# Tốc độ tăng trưởng lợi nhuận 3 năm gần nhất
# ======================================================
    def get_income_data_year(y):
        cur.execute("""
            SELECT
                (fr.data ->> 'Doanh thu thuần')::numeric AS doanh_thu,
                (fr.data ->> 'Lợi nhuận thuần')::numeric AS loi_nhuan_sau_thue_tndn,
                CASE 
                    WHEN ish.issue_share > 0 
                    THEN ROUND(((fr.data ->> 'Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)')::numeric) / ish.issue_share, 2)
                    ELSE NULL
                END AS eps
            FROM financial_reports fr
            LEFT JOIN issue_shares ish 
                ON fr.ticker = ish.symbol
            WHERE fr.report_type = 'income_statement'
            AND fr.period_type = 'year'
            AND fr.ticker = %s
            AND fr.report_year = %s;
        """, (ticker, y))
        return cur.fetchone()

    # ✅ Luôn lùi 1 năm so với năm đang chọn (vì năm hiện tại chưa kết thúc)
    years = [year - 5, year - 4, year - 3, year - 2, year - 1]

    yoy_years_ln = []
    yoy_years_eps = []
    yoy_years_dt = []   

    # ✅ Tính tăng trưởng YoY từng năm cho LNST, EPS và Doanh thu
    for i in range(1, len(years)):
        now = get_income_data_year(years[i])
        prev = get_income_data_year(years[i - 1])
        yoy_ln, yoy_eps, yoy_dt = None, None, None

        if now and prev:
            # Lợi nhuận sau thuế YoY
            if now["loi_nhuan_sau_thue_tndn"] and prev["loi_nhuan_sau_thue_tndn"]:
                prev_val = Decimal(prev["loi_nhuan_sau_thue_tndn"])
                if prev_val != 0:
                    yoy_ln = (Decimal(now["loi_nhuan_sau_thue_tndn"]) / prev_val - 1) * 100

            # EPS YoY
            if now["eps"] and prev["eps"]:
                prev_eps = Decimal(prev["eps"])
                if prev_eps != 0:
                    yoy_eps = (Decimal(now["eps"]) / prev_eps - 1) * 100

            # ✅ Doanh thu YoY
            if now["doanh_thu"] and prev["doanh_thu"]:
                prev_dt = Decimal(prev["doanh_thu"])
                if prev_dt != 0:
                    yoy_dt = (Decimal(now["doanh_thu"]) / prev_dt - 1) * 100

        yoy_years_ln.append({"year": years[i], "lnst_yoy": yoy_ln})
        yoy_years_eps.append({"year": years[i], "eps_yoy": yoy_eps})
        yoy_years_dt.append({"year": years[i], "dt_yoy": yoy_dt})  

    # ✅ So sánh tốc độ tăng trưởng giữa các năm
    statuses_ln_year = []
    statuses_eps_year = []
    statuses_dt_year = []  

    for i in range(1, len(yoy_years_ln)):
        prev, cur_y = yoy_years_ln[i - 1], yoy_years_ln[i]
        if prev["lnst_yoy"] is None or cur_y["lnst_yoy"] is None:
            continue
        statuses_ln_year.append(
            "Tăng tốc" if cur_y["lnst_yoy"] > prev["lnst_yoy"] else "Giảm tốc"
        )

    for i in range(1, len(yoy_years_eps)):
        prev, cur_y = yoy_years_eps[i - 1], yoy_years_eps[i]
        if prev["eps_yoy"] is None or cur_y["eps_yoy"] is None:
            continue
        statuses_eps_year.append(
            "Tăng tốc" if cur_y["eps_yoy"] > prev["eps_yoy"] else "Giảm tốc"
        )

    # ✅ So sánh tốc độ tăng trưởng giữa các năm (Doanh thu)
    for i in range(1, len(yoy_years_dt)):
        prev, cur_y = yoy_years_dt[i - 1], yoy_years_dt[i]
        if prev["dt_yoy"] is None or cur_y["dt_yoy"] is None:
            continue
        statuses_dt_year.append(
            "Tăng tốc" if cur_y["dt_yoy"] > prev["dt_yoy"] else "Giảm tốc"
        )

    # ✅ Hàm tổng hợp xu hướng 3 năm gần nhất
    def get_trend_summary(status_list):
        if len(status_list) >= 3:
            up = status_list.count("Tăng tốc")
            return "Tăng tốc" if up >= len(status_list) / 2 else "Giảm tốc"
        return "Không đủ dữ liệu"

    toc_do_3n_ln  = get_trend_summary(statuses_ln_year)
    toc_do_3n_eps = get_trend_summary(statuses_eps_year)
    toc_do_3n_dt  = get_trend_summary(statuses_dt_year)  

    # ✅ Tăng trưởng EPS năm gần nhất
    eps_recent_growth = None
    if len(yoy_years_eps) > 0:
        eps_recent_growth = yoy_years_eps[-1]["eps_yoy"]
    eps_recent_growth = round(eps_recent_growth, 2) if eps_recent_growth is not None else None

    # ✅ Tăng trưởng lợi nhuận năm gần nhất
    lnst_recent_growth = None
    if len(yoy_years_ln) > 0:
        lnst_recent_growth = yoy_years_ln[-1]["lnst_yoy"]
    lnst_recent_growth = round(lnst_recent_growth, 2) if lnst_recent_growth is not None else None
    # ✅ Tăng trưởng doanh thu năm gần nhất
    dt_recent_growth = None
    if len(yoy_years_dt) > 0:
        dt_recent_growth = yoy_years_dt[-1]["dt_yoy"]
    dt_recent_growth = round(dt_recent_growth, 2) if dt_recent_growth is not None else None

    # ✅ Số năm có sự tăng tốc trong tăng trưởng
    so_nam_tang_toc_ln  = statuses_ln_year.count("Tăng tốc")
    so_nam_tang_toc_eps = statuses_eps_year.count("Tăng tốc")
    so_nam_tang_toc_dt  = statuses_dt_year.count("Tăng tốc")  

    # ======================================================
    # Lợi nhuận gộp biên (Gross Margin)
    # ======================================================
    def get_gross_margin(y):
        cur.execute("""
            SELECT
                (fr.data ->> 'Doanh thu thuần')::numeric(20) AS doanh_thu,
                (fr.data ->> 'Lãi gộp')::numeric AS loi_nhuan_gop
            FROM financial_reports fr
            WHERE fr.report_type = 'income_statement'
            AND fr.period_type = 'year'
            AND fr.ticker = %s
            AND fr.report_year = %s;
        """, (ticker, y))
        return cur.fetchone()
    
    # ======================================================
    # Lợi nhuận gộp biên 3 năm gần nhất
    # ======================================================
    gross_margins = []
    for y in years[-3:]:  # lấy 3 năm gần nhất
        gm_data = get_gross_margin(y)
        if gm_data and gm_data["doanh_thu"] and gm_data["loi_nhuan_gop"]:
            dt = Decimal(gm_data["doanh_thu"])
            ln_gop = Decimal(gm_data["loi_nhuan_gop"])
            if dt != 0:
                gm = (ln_gop / dt) * 100
                gross_margins.append({"year": y, "gross_margin": round(gm, 2)})
            else:
                gross_margins.append({"year": y, "gross_margin": None})
        else:
            gross_margins.append({"year": y, "gross_margin": None})

    # Lợi nhuận gộp biên năm gần nhất
    gross_margin_recent = None
    if len(gross_margins) > 0 and gross_margins[-1]["gross_margin"] is not None:
        gross_margin_recent = gross_margins[-1]["gross_margin"]
    else:
        gross_margin_recent = None

    # Tốc độ thay đổi lợi nhuận gộp biên 3 năm gần nhất (so sánh tăng/giảm)
    statuses_gross_margin = []
    for i in range(1, len(gross_margins)):
        prev = gross_margins[i - 1]["gross_margin"]
        cur_y = gross_margins[i]["gross_margin"]
        if prev is None or cur_y is None:
            continue
        statuses_gross_margin.append("Mở rộng" if cur_y > prev else "Thu hẹp")

    toc_do_3n_gross_margin = None
    if len(statuses_gross_margin) >= 2:
        up = statuses_gross_margin.count("Mở rộng")
        toc_do_3n_gross_margin = "Mở rộng" if up >= len(statuses_gross_margin) else "Thu hẹp"

     # ======================================================
    # Lợi nhuận biên ròng sau thuế (Net Profit Margin)
    # ======================================================
    def get_net_profit_margin(y):
        cur.execute("""
            SELECT
                (fr.data ->> 'Doanh thu thuần')::numeric(20) AS doanh_thu,
                (fr.data ->> 'Lợi nhuận thuần')::numeric(20) AS loi_nhuan_sau_thue_tndn
            FROM financial_reports fr
            WHERE fr.report_type = 'income_statement'
            AND fr.period_type = 'year'
            AND fr.ticker = %s
            AND fr.report_year = %s;
        """, (ticker, y))
        return cur.fetchone()

    # ======================================================
    # Tính toán Lợi nhuận biên ròng sau thuế 3 năm gần nhất
    # ======================================================
    net_margins = []
    for y in years[-3:]:  # chỉ lấy 3 năm gần nhất
        nm_data = get_net_profit_margin(y)
        if nm_data and nm_data["doanh_thu"] and nm_data["loi_nhuan_sau_thue_tndn"]:
            dt = Decimal(nm_data["doanh_thu"])
            ln_rong = Decimal(nm_data["loi_nhuan_sau_thue_tndn"])
            if dt != 0:
                nm = (ln_rong / dt) * 100
                net_margins.append({"year": y, "net_margin": round(nm, 2)})
            else:
                net_margins.append({"year": y, "net_margin": None})
        else:
            net_margins.append({"year": y, "net_margin": None})

    # ✅ Lợi nhuận biên ròng năm gần nhất
    net_margin_recent = None
    if len(net_margins) > 0 and net_margins[-1]["net_margin"] is not None:
        net_margin_recent = net_margins[-1]["net_margin"]
    else:
        net_margin_recent = None

    # ✅ Xác định xu hướng 3 năm gần nhất (Tăng / Giảm)
    statuses_net_margin = []
    for i in range(1, len(net_margins)):
        prev = net_margins[i - 1]["net_margin"]
        cur_y = net_margins[i]["net_margin"]
        if prev is None or cur_y is None:
            continue
        statuses_net_margin.append("Mở rộng" if cur_y > prev else "Thu hẹp")

    toc_do_3n_net_margin = None
    if len(statuses_net_margin) >= 2:
        up = statuses_net_margin.count("Mở rộng")
        toc_do_3n_net_margin = "Mở rộng" if up >= len(statuses_net_margin) else "Thu hẹp"

    # ======================================================
    # ROE (Return on Equity) – Tỷ suất lợi nhuận trên vốn chủ sở hữu
    # ======================================================
    def get_roe_data(y):
        # Lấy LNST từ báo cáo kết quả kinh doanh
        cur.execute("""
            SELECT
                (fr.data ->> 'Lợi nhuận sau thuế của Cổ đông công ty mẹ (đồng)')::numeric(20) AS lnst_cua_cdctyme
            FROM financial_reports fr
            WHERE fr.report_type = 'income_statement'
            AND fr.period_type = 'year'
            AND fr.ticker = %s
            AND fr.report_year = %s;
        """, (ticker, y))
        income_data = cur.fetchone()

        # Lấy vốn chủ sở hữu từ bảng cân đối kế toán
        cur.execute("""
            SELECT
                (fr.data ->> 'VỐN CHỦ SỞ HỮU (đồng)')::numeric AS von_chu_so_huu
            FROM financial_reports fr
            WHERE fr.report_type = 'balance_sheet'
            AND fr.period_type = 'year'
            AND fr.ticker = %s
            AND fr.report_year = %s;
        """, (ticker, y))
        bs_data = cur.fetchone()

        return {
            "lnst_cua_cdctyme": income_data["lnst_cua_cdctyme"] if income_data else None,
            "von_chu_so_huu": bs_data["von_chu_so_huu"] if bs_data else None
        }

    # ======================================================
    # Tính ROE cho 3 năm gần nhất (dựa trên 4 năm dữ liệu để tính trung bình vốn)
    # ======================================================
    roes = []
    for i in range(1, len(years)):
        now_y = years[i]
        prev_y = years[i - 1]

        now_data = get_roe_data(now_y)
        prev_data = get_roe_data(prev_y)

        if now_data and prev_data and now_data["lnst_cua_cdctyme"] and now_data["von_chu_so_huu"] and prev_data["von_chu_so_huu"]:
            avg_equity = (Decimal(now_data["von_chu_so_huu"]) + Decimal(prev_data["von_chu_so_huu"])) / 2
            if avg_equity != 0:
                roe = (Decimal(now_data["lnst_cua_cdctyme"]) / avg_equity) * 100
                roes.append({"year": now_y, "roe": round(roe, 2)})
            else:
                roes.append({"year": now_y, "roe": None})
        else:
            roes.append({"year": now_y, "roe": None})

    # ✅ ROE năm gần nhất (ví dụ chọn 2025 → lấy ROE năm 2024)
    roe_recent = None
    if len(roes) > 0 and roes[-1]["roe"] is not None:
        roe_recent = roes[-1]["roe"]
    else:
        roe_recent = None

    def fmt(d):
        return round(float(d), 2) if d is not None else None

    result_data = {
        "Mã chứng khoán": ticker,
        "Năm": year,
        "Quý": quarter,
        "EPS Quý hiện tại": "----------------",
        "Tăng trưởng lợi nhuận YoY (%)": fmt(lnst_growth_now),
        "Tốc độ tăng trưởng lợi nhuận 3 quý gần nhất": toc_do_3q_ln,
        "Số quý có tăng tốc lợi nhuận trong 3 quý gần nhất": so_quy_tang_ln,

        "Tăng trưởng doanh thu YoY (%)": fmt(dt_growth_now),
        "Tốc độ tăng trưởng doanh thu 3 quý gần nhất": toc_do_3q_dt,
        "Số quý có tăng tốc doanh thu trong 3 quý gần nhất": so_quy_tang_dt,

        "Tăng trưởng EPS YoY (%)": fmt(eps_growth_now),
        "Tốc độ tăng trưởng EPS 3 quý gần nhất": toc_do_3q_eps,
        "Số quý tăng tốc EPS trong 3 quý gần nhất": eps_up_count,

        "EPS HẰNG NĂM": nam_du_lieu_nam_gan_nhat,
        "Tăng trưởng lợi nhuận năm gần nhất (%)": fmt(tang_truong_loi_nhuan_nam),
        "Tốc độ tăng trưởng lợi nhuận 3 năm gần nhất": toc_do_3n_ln,
        "Số năm có sự tăng tốc trong tăng trưởng lợi nhuận": so_nam_tang_toc_ln,
        # "Chi tiết 3 năm gần nhất (LNST)": statuses_ln_year,

        "Tăng trưởng EPS năm gần nhất (%)": eps_recent_growth,
        "Tốc độ tăng trưởng EPS 3 năm gần nhất": toc_do_3n_eps,
        "Số năm có sự tăng tốc trong tăng trưởng EPS": so_nam_tang_toc_eps,
        # "Chi tiết 3 năm gần nhất (EPS)": statuses_eps_year,

        "CHỈ SỐ SMR(DOANH SỐ, LỢI NHUẬN BIÊN, ROE": nam_du_lieu_nam_gan_nhat,
        "Tăng trưởng doanh thu năm gần nhất (%)": fmt(dt_recent_growth),
        "Tốc độ tăng trưởng doanh thu 3 năm gần nhất": toc_do_3n_dt,
        # "Số năm có sự tăng tốc trong tăng trưởng doanh thu": so_nam_tang_toc_dt,
        # "Chi tiết 3 năm gần nhất (Doanh thu)": statuses_dt_year,
        "Lợi nhuận gộp biên năm gần nhất (%)": gross_margin_recent,
        "Tốc độ thay đổi lợi nhuận gộp biên 3 năm gần nhất": toc_do_3n_gross_margin,
        # "Chi tiết lợi nhuận gộp biên 3 năm gần nhất": gross_margins,
        "Lợi nhuận biên ròng sau thuế năm gần nhất (%)": net_margin_recent,
        "Tốc độ thay đổi lợi nhuận biên ròng sau thuế 3 năm gần nhất": toc_do_3n_net_margin,
        # "Chi tiết lợi nhuận biên ròng sau thuế 3 năm gần nhất": net_margins,
        "ROE năm gần nhất (%)": roe_recent,
        # "Chi tiết ROE 3 năm gần nhất": roes
    }
    # Đóng kết nối raw cursor nếu chưa đóng
    cur.close()
    conn.close()

    return result_data
    
def save_growth_summary_to_db(data: dict):
    db = SessionLocal()
    try:
        mapping = {
            "Mã chứng khoán": "ticker",
            "Năm": "year",
            "Quý": "quarter",

            # Các chỉ số quý
            "Tăng trưởng lợi nhuận YoY (%)": "loi_nhuan_sau_thue_quy",
            "Tốc độ tăng trưởng lợi nhuận 3 quý gần nhất": "lnst_toc_do_3quy",
            "Số quý có tăng tốc lợi nhuận trong 3 quý gần nhất": "lnst_so_quy_lien_tiep_tang_toc",

            "Tăng trưởng doanh thu YoY (%)": "doanh_thu_quy",
            "Tốc độ tăng trưởng doanh thu 3 quý gần nhất": "dt_toc_do_3quy",
            "Số quý có tăng tốc doanh thu trong 3 quý gần nhất": "dt_so_quy_lien_tiep_tang_toc",

            "Tăng trưởng EPS YoY (%)": "eps_quy",
            "Tốc độ tăng trưởng EPS 3 quý gần nhất": "eps_toc_do_3quy",
            "Số quý tăng tốc EPS trong 3 quý gần nhất": "eps_so_quy_lien_tiep_tang_toc",

            # Các chỉ số năm
            "Tăng trưởng lợi nhuận năm gần nhất (%)": "loi_nhuan_sau_thue_nam",
            "Tốc độ tăng trưởng lợi nhuận 3 năm gần nhất": "lnst_toc_do_3nam",
            "Số năm có sự tăng tốc trong tăng trưởng lợi nhuận": "lnst_so_nam_lien_tiep_tang_toc",

            "Tăng trưởng EPS năm gần nhất (%)": "eps_nam",
            "Tốc độ tăng trưởng EPS 3 năm gần nhất": "eps_toc_do_3nam",
            "Số năm có sự tăng tốc trong tăng trưởng EPS": "eps_so_nam_lien_tiep_tang_toc",

            "Tăng trưởng doanh thu năm gần nhất (%)": "dt_nam",
            "Tốc độ tăng trưởng doanh thu 3 năm gần nhất": "dt_toc_do_3nam",

            "Lợi nhuận gộp biên năm gần nhất (%)": "loi_nhuan_bien_gop_nam",
            "Tốc độ thay đổi lợi nhuận gộp biên 3 năm gần nhất": "su_mo_rong_lnbg",

            "Lợi nhuận biên ròng sau thuế năm gần nhất (%)": "loi_nhuan_bien_rong_st_nam",
            "Tốc độ thay đổi lợi nhuận biên ròng sau thuế 3 năm gần nhất": "su_mo_rong_lnbr_st",

            "ROE năm gần nhất (%)": "roe"
        }
        db_data = {}
        for vi_key, en_key in mapping.items():
            if vi_key in data:
                db_data[en_key] = data[vi_key]

        # 3️⃣ Lấy khóa chính để kiểm tra bản ghi tồn tại
        ticker = db_data.get("ticker")
        year = db_data.get("year")
        quarter = db_data.get("quarter")

        if not ticker:
            raise ValueError("Thiếu mã chứng khoán (ticker) trong dữ liệu.")

        existing = db.query(FinancialGrowthReport).filter_by(
            ticker=ticker, year=year, quarter=quarter
        ).first()

        # 4️⃣ Update nếu có, insert nếu chưa
        if existing:
            for k, v in db_data.items():
                if hasattr(existing, k):
                    setattr(existing, k, v)
        else:
            record = FinancialGrowthReport(**db_data)
            db.add(record)

        db.commit()

    except Exception as e:
        db.rollback()
        print(f"Lỗi khi lưu vào DB: {e}")
        raise
    finally:
        db.close()

def process_one_ticker(ticker, years, quarters):
    """Xử lý một ticker"""
    db = SessionLocal()
    results = []
    try:
        for year in years:
            for quarter in quarters:
                try:
                    result = calc_growth(ticker, year, quarter)
                    if result:
                        result["Mã chứng khoán"] = ticker
                        result["Năm"] = year
                        result["Quý"] = quarter
                        save_growth_summary_to_db(result)
                        results.append(result)
                except Exception as e:
                    print(f"❌ Lỗi {ticker}-{year}Q{quarter}: {e}")
                    db.rollback()
        return results
    finally:
        db.close()

def batch_calculate_growth_to_db(all_tickers, years, quarters, max_workers=8):
    """Chạy batch song song cho toàn bộ ticker"""
    print(f"🚀 Bắt đầu xử lý {len(all_tickers)} ticker...")
    total_jobs = len(all_tickers) * len(years) * len(quarters)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_one_ticker, t, years, quarters): t for t in all_tickers}
        for future in tqdm(as_completed(futures), total=len(all_tickers), desc="Đang xử lý ticker"):
            ticker = futures[future]
            try:
                future.result()
            except Exception as e:
                print(f"⚠️ Lỗi khi xử lý ticker {ticker}: {e}")

    print("✅ Hoàn tất lưu dữ liệu tăng trưởng vào DB.")



# 🧩 API Endpoint
@router.get("/profit_growth")
def get_profit_growth(
    ticker: str = Query(..., description="Mã cổ phiếu, ví dụ: FPT"),
    year: int = Query(..., description="Năm cần tính, ví dụ: 2025"),
    quarter: int = Query(..., description="Quý cần tính, ví dụ: 2")
):
    try:
        result = calc_growth(ticker, year, quarter)
        return result
    except Exception as e:
        return {"error": str(e)}