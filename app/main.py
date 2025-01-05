from fastapi import FastAPI
from app.services.json_datas.json_data_generator_routers import json_data_router
from app.services.topics.topics_routers import topics_router
from app.services.elastic.elastic_routers import elastic_router
from app.services.clickhouse.clickhouse_routers import clickhouse_router

app = FastAPI()

# Router'ları bağla
app.include_router(json_data_router, prefix="/api", tags=["json-data"])
app.include_router(topics_router, prefix="/api", tags=["topic"])
app.include_router(elastic_router, prefix="/api", tags=["elastic"])
app.include_router(clickhouse_router, prefix="/api", tags=["clickhouse"])

@app.get("/")
def read_root():
    return {"message": "Welcome to the Redpanda Kafka API"}


