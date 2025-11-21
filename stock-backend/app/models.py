from sqlalchemy import Column, String, Date, Float, BigInteger, Integer, TIMESTAMP, Numeric, UniqueConstraint, JSON, DateTime, func
from .database import Base
from datetime import datetime

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

class FinancialReport(Base):
    __tablename__ = "financial_reports"

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, nullable=False)
    report_type = Column(String, nullable=False)
    period_type = Column(String, nullable=False)
    report_year = Column(Integer, nullable=False)
    report_quarter = Column(Integer)   # cho phép NULL
    lang = Column(String)
    data = Column(JSON)

    __table_args__ = (
        UniqueConstraint("ticker", "report_type", "period_type", "report_year", "report_quarter", "lang", name="uniq_report"),
    )

class IssueShare(Base):
    __tablename__ = "issue_shares"

    symbol = Column(String, primary_key=True, index=True)
    issue_share = Column(BigInteger)
    updated_at = Column(DateTime, default=datetime.now)

class FinancialGrowthReport(Base):
    __tablename__ = "financial_growth_report"

    id = Column(Integer, primary_key=True, index=True)
    ticker = Column(String(10), nullable=False, index=True)
    year = Column(Integer, nullable=True)
    quarter = Column(Integer, nullable=True)

    # Các chỉ số quý
    loi_nhuan_sau_thue_quy = Column(Float, nullable=True)
    lnst_toc_do_3quy = Column(String(50), nullable=True)
    lnst_so_quy_lien_tiep_tang_toc = Column(Integer, nullable=True)

    doanh_thu_quy = Column(Float, nullable=True)
    dt_toc_do_3quy = Column(String(50), nullable=True)
    dt_so_quy_lien_tiep_tang_toc = Column(Integer, nullable=True)

    eps_quy = Column(Float, nullable=True)
    eps_toc_do_3quy = Column(String(50), nullable=True)
    eps_so_quy_lien_tiep_tang_toc = Column(Integer, nullable=True)

    # Các chỉ số năm
    loi_nhuan_sau_thue_nam = Column(Float, nullable=True)
    lnst_toc_do_3nam = Column(String(50), nullable=True)
    lnst_so_nam_lien_tiep_tang_toc = Column(Integer, nullable=True)

    eps_nam = Column(Float, nullable=True)
    eps_toc_do_3nam = Column(String(50), nullable=True)
    eps_so_nam_lien_tiep_tang_toc = Column(Integer, nullable=True)

    dt_nam = Column(Float, nullable=True)
    dt_toc_do_3nam = Column(String(50), nullable=True)

    loi_nhuan_bien_gop_nam = Column(Float, nullable=True)
    su_mo_rong_lnbg = Column(String(50), nullable=True)

    loi_nhuan_bien_rong_st_nam = Column(Float, nullable=True)
    su_mo_rong_lnbr_st = Column(String(50), nullable=True)

    roe = Column(Float, nullable=True)

    created_at = Column(TIMESTAMP, server_default=func.now())
    updated_at = Column(TIMESTAMP, onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("ticker", "year", "quarter", name="uq_growth_report_ticker_year_quarter"),
    )

    def __repr__(self):
        return f"<FinancialGrowthReport(ticker={self.ticker}, year={self.year}, quarter={self.quarter})>"
