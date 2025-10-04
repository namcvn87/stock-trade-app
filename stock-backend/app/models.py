from sqlalchemy import Column, String, Date, Float, BigInteger, Integer, TIMESTAMP, Numeric, UniqueConstraint, JSON, DateTime
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
