import logging
import time
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd
from sqlalchemy.orm import Session
from vnstock import Company, Listing

from app.database import SessionLocal, engine, Base
from app.models import IssueShare

# Tạo bảng nếu chưa có
Base.metadata.create_all(bind=engine)

# Logger
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def safe_decimal(value, precision=6):
    """Chuyển đổi giá trị sang Decimal với số chữ thập phân cố định"""
    try:
        return Decimal(str(round(float(value), precision)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def full_load_issue_shares():
    db: Session = SessionLocal()
    try:
        # Lấy danh sách tất cả symbol
        listing = Listing(source="VCI")
        symbols_df = listing.all_symbols()
        symbols = symbols_df["symbol"].dropna().unique().tolist()
        logger.info(f"📌 Tìm thấy {len(symbols)} cổ phiếu để load issue_share")

        for i, symbol in enumerate(symbols, start=1):
            try:
                company = Company(symbol=symbol, source="TCBS")
                overview = company.overview()

                issue_share_value = overview.get("issue_share")

                # Nếu là Series hoặc DataFrame → lấy giá trị đầu tiên
                if isinstance(issue_share_value, (pd.Series, pd.DataFrame)):
                    issue_share_value = issue_share_value.iloc[0]

                if issue_share_value is None:
                    logger.warning(f"⚠️ Không có issue_share cho {symbol}")
                    continue

                # Convert sang Decimal với 6 chữ số thập phân
                raw_value_decimal = safe_decimal(issue_share_value, precision=6)
                if raw_value_decimal is None:
                    logger.warning(f"⚠️ issue_share không hợp lệ cho {symbol} ({issue_share_value})")
                    continue

                # Nhân 1_000_000 để chuyển từ triệu cp sang cp thực
                issue_share_value = int(raw_value_decimal * Decimal(1_000_000))

                # Insert or update
                existing = db.query(IssueShare).filter_by(symbol=symbol).first()
                if existing:
                    existing.issue_share = issue_share_value
                    existing.updated_at = datetime.now()
                    logger.info(f"🔁 Cập nhật {symbol}: {issue_share_value:,} cp")
                else:
                    new_entry = IssueShare(
                        symbol=symbol,
                        issue_share=issue_share_value,
                        updated_at=datetime.now()
                    )
                    db.add(new_entry)
                    logger.info(f"✅ Thêm {symbol}: {issue_share_value:,} cp")

                db.commit()

            except Exception as e:
                logger.error(f"❌ Lỗi khi xử lý {symbol}: {e}", exc_info=False)
                db.rollback()

            # Thêm delay để tránh quá tải API
            time.sleep(1)

        logger.info("🎉 Hoàn thành full_load issue_shares")

    finally:
        db.close()
