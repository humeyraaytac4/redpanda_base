from fastapi import FastAPI
from app.services.json_datas.json_data_generator_routers import json_data_router


app = FastAPI()

# Router'ları bağla
app.include_router(json_data_router, prefix="/api", tags=["json-data"])


@app.get("/")
def read_root():
    return {"message": "Welcome to the Redpanda Kafka API"}


