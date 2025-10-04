from fastapi import FastAPI
from .routers import stocks, fastocks
from . import models, database
from app.fa_full_load import get_all_tickers, full_load_financials
from app.fa_delta_load import delta_load_financials
import sys

# Tạo bảng (nếu chưa có)
models.Base.metadata.create_all(bind=database.engine)

app = FastAPI()

# Gắn router
app.include_router(stocks.router)
app.include_router(fastocks.router)

def main():
    print("=== Stock Data Loader ===")
    print("1. Full Load")
    print("2. Delta Load")
    print("0. Exit")

    choice = input("Chọn chức năng: ").strip()

    if choice == "1":
        tickers = get_all_tickers()
        full_load_financials(tickers, source="VCI", period_types=["quarter", "year"], lang="vi")

    elif choice == "2":
        tickers = get_all_tickers()
        print("Tổng số mã:", len(tickers))
        symbol = input("Nhập mã cổ phiếu cần delta load (Enter để chạy tất cả): ").strip().upper()
        if symbol == "":
            symbol = None

        delta_load_financials(
            tickers,
            source="VCI",
            lang="vi",
            period_types=["quarter", "year"],
            symbol=symbol
        )

    elif choice == "0":
        print("Thoát...")
        sys.exit(0)

    else:
        print("Lựa chọn không hợp lệ")


if __name__ == "__main__":
    main()

