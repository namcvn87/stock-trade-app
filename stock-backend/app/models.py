from sqlalchemy import Column, String, Date, Float, BigInteger, Integer, TIMESTAMP
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
