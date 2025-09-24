from fastapi import FastAPI
from .routers import stocks, fastocks
from . import models, database

# Tạo bảng (nếu chưa có)
models.Base.metadata.create_all(bind=database.engine)

app = FastAPI()

# Gắn router
app.include_router(stocks.router)
app.include_router(fastocks.router)
