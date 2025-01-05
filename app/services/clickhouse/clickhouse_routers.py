from fastapi import APIRouter, HTTPException
import httpx
from app.services.clickhouse.clickhouse_schemas import DatabaseRequest, QueryRequest, ConsumeRequest
from app.services.clickhouse.clickhouse_logger import consume_messages, create_table


clickhouse_router = APIRouter()

CLICKHOUSE_URL = "http://clickhouse-server:8123"   # ClickHouse HTTP API'sinin adresi


@clickhouse_router.get("/health")
async def check_clickhouse_health():
    """
    ClickHouse'un çalışıp çalışmadığını kontrol eder.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data="SELECT 1")
            if response.status_code == 200 and response.text.strip() == "1":
                return {"status": "healthy", "details": "ClickHouse is running and responding."}
            raise HTTPException(status_code=500, detail="ClickHouse did not respond as expected.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to ClickHouse: {e}")


@clickhouse_router.get("/databases")
async def list_databases():
    """
    ClickHouse üzerindeki veritabanlarını listeler.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data="SHOW DATABASES")
            if response.status_code == 200:
                databases = response.text.strip().split("\n")
                return {"status": "success", "databases": databases}
            raise HTTPException(status_code=500, detail="Failed to retrieve databases.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to ClickHouse: {e}")


@clickhouse_router.get("/tables/{database_name}")
async def list_tables(database_name: str):
    """
    Belirtilen veritabanındaki tabloları listeler.
    """
    query = f"SHOW TABLES FROM {database_name}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data=query)
            if response.status_code == 200:
                tables = response.text.strip().split("\n")
                return {"status": "success", "database": database_name, "tables": tables}
            raise HTTPException(status_code=500, detail=f"Failed to retrieve tables from {database_name}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to ClickHouse: {e}")


@clickhouse_router.get("/tables/{database_name}/{table_name}")
async def select_tables_all(database_name: str, table_name:str):
    """
    Belirtilen veritabanındaki tabloları listeler.
    """
    query = f"SELECT * FROM {database_name}.{table_name}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data=query)
            if response.status_code == 200:
                row = response.text.strip().split("\n")
                return {"status": "success", "database": database_name, "tables": table_name, "rows": row}
            raise HTTPException(status_code=500, detail=f"Failed to retrieve tables from {database_name}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to ClickHouse: {e}")


@clickhouse_router.get("/tables/{database_name}/{table_name}/{column_name}")
async def select_tables(database_name: str, table_name:str, column_name:str):
    """
    Belirtilen veritabanındaki tabloları listeler.
    """
    query = f"SELECT {column_name} FROM {database_name}.{table_name}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data=query)
            if response.status_code == 200:
                row = response.text.strip().split("\n")
                return {"status": "success", "database": database_name, "tables": table_name, "columns": column_name,  "rows": row}
            raise HTTPException(status_code=500, detail=f"Failed to retrieve tables from {database_name}.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to ClickHouse: {e}")


@clickhouse_router.post("/query")
async def run_custom_query(request: QueryRequest):
    """
    ClickHouse üzerinde özel bir sorgu çalıştırır.
    """
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data=request.query)
            if response.status_code == 200:
                return {"status": "success", "result": response.text.strip()}
            raise HTTPException(status_code=500, detail="Query execution failed.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error executing query: {e}")


@clickhouse_router.get("/users")
async def list_users():
    """
    ClickHouse üzerindeki kullanıcıları listeler.
    """
    query = "SHOW USERS"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data=query)
            if response.status_code == 200:
                users = response.text.strip().split("\n")
                return {"status": "success", "users": users}
            raise HTTPException(status_code=500, detail="Failed to retrieve users.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error connecting to ClickHouse: {e}")


@clickhouse_router.post("/add-database")
async def add_database(request: DatabaseRequest):
    """
    Yeni bir veritabanı ekler.
    """
    query = f"CREATE DATABASE IF NOT EXISTS {request.database_name}"
    try:
        async with httpx.AsyncClient() as client:
            response = await client.post(CLICKHOUSE_URL, data=query)
            if response.status_code == 200:
                return {"status": "success", "message": f"Database '{request.database_name}' created successfully."}
            raise HTTPException(status_code=500, detail=f"Failed to create database '{request.database_name}'.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating database: {e}")





@clickhouse_router.post("/consume/")
async def consume_kafka_messages(request: ConsumeRequest):
    """
    Verilen Kafka topiğinden mesaj tüketir ve ClickHouse'a kaydeder.
    """
    create_table(topic_name=request.topic)

    try:
        response = consume_messages(
            topic=request.topic,
            timeout=request.timeout,
            max_messages=request.max_messages
        )
        return {
            "status": "success",
            "message": f"{len(response['messages'])} messages consumed from topic '{request.topic}' and logged to ClickHouse.",
            "details": response,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))