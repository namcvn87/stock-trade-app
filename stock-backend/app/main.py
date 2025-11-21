from fastapi import FastAPI
from .routers import stocks, fastocks, financial_metrics, financial_ranking
from . import models, database
from app.fa_full_load import get_all_tickers, full_load_financials
from app.fa_delta_load import delta_load_financials
from app.fa_shareholding import full_load_issue_shares
from .routers.financial_metrics import batch_calculate_growth_to_db
import sys
from datetime import datetime

# Tạo bảng (nếu chưa có)
models.Base.metadata.create_all(bind=database.engine)

app = FastAPI()

# Gắn router
app.include_router(stocks.router)
app.include_router(fastocks.router)
app.include_router(financial_metrics.router)
app.include_router(financial_ranking.router)

def main():
    print("=== Stock Data Loader ===")
    print("1. Full Load/ Delta Load")
    print("2. Shareholding Load")
    print("3. Saving Financial Metrics")
    print("0. Exit")

    choice = input("Chọn chức năng: ").strip()

    if choice == "1":
        tickers = get_all_tickers()
        print("Tổng số mã:", len(tickers))
        symbol = input("Nhập mã cổ phiếu cần Delta load (Enter để chạy Full load): ").strip().upper()
        if symbol == "":
            symbol = None

        delta_load_financials(
            tickers,
            source="VCI",
            lang="vi",
            period_types=["quarter", "year"],
            symbol=symbol
        )

    elif choice == "2":
        full_load_issue_shares()

    elif choice == "3":
        all_tickers = get_all_tickers()
        current_year = datetime.now().year
        years = list(range(2014, current_year + 1))
        quarters = [1, 2, 3, 4]

        # 🔹 In log và chạy batch song song
        print(f"🔄 Bắt đầu xử lý {len(all_tickers)} mã...")
        batch_calculate_growth_to_db(all_tickers, years, quarters, max_workers=5)

    elif choice == "0":
        print("Thoát...")
        sys.exit(0)

    else:
        print("Lựa chọn không hợp lệ")


if __name__ == "__main__":
    main()

