from sqlalchemy import Column, String, Date, Float, BigInteger, Integer, TIMESTAMP, Numeric, UniqueConstraint
from .database import Base

# Thông tin công ty
class Company(Base):
    __tablename__ = "companies"

    symbol = Column(String, primary_key=True)
    exchange = Column(String)
    name = Column(String)
    industry = Column(String)
    website = Column(String, nullable=True)
    listing_date = Column(Date, nullable=True)

# Giá cổ phiếu (OHLCV)
class StockPrice(Base):
    __tablename__ = "stock_prices"

    symbol = Column(String, primary_key=True)
    date = Column(Date, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(BigInteger)
    value = Column(BigInteger, nullable=True)
    change = Column(Float, nullable=True)

# Báo cáo tài chính
class Financial(Base):
    __tablename__ = "financials"

    id = Column(Integer, primary_key=True, autoincrement=True)
    symbol = Column(String)
    period = Column(Date)
    report_type = Column(String)   # balance_sheet, income_statement, cash_flow, ratio
    item = Column(String)
    value = Column(Float)

# Nhóm chỉ số (VN30, HNX30,…)
class Index(Base):
    __tablename__ = "indices"

    group_name = Column(String, primary_key=True)
    symbol = Column(String, primary_key=True)

# Intraday
class IntradayPrice(Base):
    __tablename__ = "intraday_prices"

    symbol = Column(String, primary_key=True)
    datetime = Column(TIMESTAMP, primary_key=True)
    price = Column(Float)
    volume = Column(BigInteger)

# -------------------------
# Income Statement
# -------------------------
class IncomeStatement(Base):
    __tablename__ = "income_statement"

    id = Column(Integer, primary_key=True, index=True)

    # Meta
    symbol = Column(String, index=True)
    report_date = Column(Date, index=True)   # Năm (YYYY)
    period_type = Column(String)                # year / quarter
    lang = Column(String)
    source = Column(String)

    # Các chỉ tiêu tài chính
    doanh_thu = Column(Numeric(20, 2))
    tang_truong_doanh_thu = Column(Numeric(10, 4))
    loi_nhuan_sau_thue_cty_me = Column(Numeric(20, 2))
    tang_truong_loi_nhuan = Column(Numeric(10, 4))
    thu_nhap_lai_va_tuong_tu = Column(Numeric(20, 2))
    chi_phi_lai_va_tuong_tu = Column(Numeric(20, 2))
    thu_nhap_lai_thuan = Column(Numeric(20, 2))
    thu_nhap_dich_vu = Column(Numeric(20, 2))
    chi_phi_dich_vu = Column(Numeric(20, 2))
    lai_thuan_dich_vu = Column(Numeric(20, 2))
    kinh_doanh_ngoai_hoi_vang = Column(Numeric(20, 2))
    chung_khoan_kinh_doanh = Column(Numeric(20, 2))
    chung_khoan_dau_tu = Column(Numeric(20, 2))
    hoat_dong_khac = Column(Numeric(20, 2))
    chi_phi_hoat_dong_khac = Column(Numeric(20, 2))
    lai_lo_thuan_hd_khac = Column(Numeric(20, 2))
    co_tuc_da_nhan = Column(Numeric(20, 2))
    tong_thu_nhap_hd = Column(Numeric(20, 2))
    chi_phi_quan_ly_dn = Column(Numeric(20, 2))
    ln_truoc_du_phong = Column(Numeric(20, 2))
    chi_phi_du_phong_rui_ro = Column(Numeric(20, 2))
    ln_truoc_thue = Column(Numeric(20, 2))
    thue_tndn = Column(Numeric(20, 2))
    thue_tndn_hien_hanh = Column(Numeric(20, 2))
    thue_tndn_hoan_lai = Column(Numeric(20, 2))
    co_dong_thieu_so = Column(Numeric(20, 2))
    loi_nhuan_thuan = Column(Numeric(20, 2))
    loi_nhuan_cty_me = Column(Numeric(20, 2))
    eps = Column(Numeric(10, 4))

    __table_args__ = (
        UniqueConstraint("symbol", "report_date", "period_type", "source", name="uq_income_unique"),
    )


# -------------------------
# Balance Sheet
# -------------------------
class BalanceSheet(Base):
    __tablename__ = "balance_sheet"

    id = Column(Integer, primary_key=True, index=True)

    # Meta
    symbol = Column(String, index=True)
    report_date = Column(Date, index=True)
    period_type = Column(String)
    lang = Column(String)
    source = Column(String)

    # Tài sản
    tong_tai_san = Column(Numeric(20, 2))
    tien_va_tuong_duong = Column(Numeric(20, 2))
    tien_gui_nhnn = Column(Numeric(20, 2))
    tien_gui_tctd = Column(Numeric(20, 2))
    ck_kinh_doanh = Column(Numeric(20, 2))
    ck_kinh_doanh_khac = Column(Numeric(20, 2))
    du_phong_ck_kinh_doanh = Column(Numeric(20, 2))
    cong_cu_phai_sinh = Column(Numeric(20, 2))
    cho_vay_khach_hang = Column(Numeric(20, 2))
    cho_vay_khach_hang_khac = Column(Numeric(20, 2))
    du_phong_cho_vay = Column(Numeric(20, 2))
    ck_dau_tu = Column(Numeric(20, 2))
    ck_san_sang_ban = Column(Numeric(20, 2))
    ck_giu_den_dao_han = Column(Numeric(20, 2))
    du_phong_ck_dau_tu = Column(Numeric(20, 2))
    dau_tu_dai_han = Column(Numeric(20, 2))
    dau_tu_lien_doanh = Column(Numeric(20, 2))
    tai_san_dai_han_khac = Column(Numeric(20, 2))
    du_phong_dau_tu_dai_han = Column(Numeric(20, 2))
    tai_san_co_dinh = Column(Numeric(20, 2))
    tscd_huu_hinh = Column(Numeric(20, 2))
    tscd_vo_hinh = Column(Numeric(20, 2))
    tai_san_co_khac = Column(Numeric(20, 2))

    # Nguồn vốn
    tong_nguon_von = Column(Numeric(20, 2))
    no_phai_tra = Column(Numeric(20, 2))
    no_cp_va_nhnn = Column(Numeric(20, 2))
    no_tctd = Column(Numeric(20, 2))
    tien_gui_khach_hang = Column(Numeric(20, 2))
    cong_cu_phai_sinh_no = Column(Numeric(20, 2))
    von_tai_tro_uy_thac = Column(Numeric(20, 2))
    phat_hanh_giay_to = Column(Numeric(20, 2))
    no_khac = Column(Numeric(20, 2))

    von_chu_so_huu = Column(Numeric(20, 2))
    von_to_chuc_td = Column(Numeric(20, 2))
    quy_to_chuc_td = Column(Numeric(20, 2))
    chenh_lech_ty_gia = Column(Numeric(20, 2))
    chenh_lech_danh_gia = Column(Numeric(20, 2))
    loi_nhuan_chua_pp = Column(Numeric(20, 2))
    von_gop_chu_so_huu = Column(Numeric(20, 2))
    cac_quy_khac = Column(Numeric(20, 2))
    loi_ich_cd_thieu_so = Column(Numeric(20, 2))

    __table_args__ = (
        UniqueConstraint("symbol", "report_date", "period_type", "source", name="uq_balance_unique"),
    )


# -------------------------
# Cash Flow
# -------------------------
class CashFlow(Base):
    __tablename__ = "cash_flow"

    id = Column(Integer, primary_key=True, index=True)

    # Meta
    symbol = Column(String, index=True)
    report_date = Column(Date, index=True)
    period_type = Column(String)
    lang = Column(String)
    source = Column(String)

    # Lưu chuyển tiền tệ
    lai_lo_hd_khac = Column(Numeric(20, 2))
    lc_tu_hd_kd_truoc_vld = Column(Numeric(20, 2))
    lc_tu_hd_kd_truoc_thue = Column(Numeric(20, 2))
    chi_tu_cac_quy_tctd = Column(Numeric(20, 2))
    mua_sam_tscd = Column(Numeric(20, 2))
    tien_thu_co_tuc = Column(Numeric(20, 2))
    lc_tu_hd_dau_tu = Column(Numeric(20, 2))
    tang_von_co_phan = Column(Numeric(20, 2))
    lc_tu_hd_tc = Column(Numeric(20, 2))
    lc_thuan_trong_ky = Column(Numeric(20, 2))
    tien_va_tuong_duong = Column(Numeric(20, 2))
    tien_va_tuong_duong_ck = Column(Numeric(20, 2))
    lc_rong_hd_sxkd = Column(Numeric(20, 2))
    tien_thu_thanh_ly_tscd = Column(Numeric(20, 2))
    dau_tu_dn_khac = Column(Numeric(20, 2))
    tien_thu_ban_dau_tu = Column(Numeric(20, 2))
    co_tuc_da_tra = Column(Numeric(20, 2))

    __table_args__ = (
        UniqueConstraint("symbol", "report_date", "period_type", "source", name="uq_cashflow_unique"),
    )

