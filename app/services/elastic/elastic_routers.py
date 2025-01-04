from fastapi import APIRouter, HTTPException, Query
from elasticsearch import Elasticsearch, exceptions as es_exceptions
from app.services.elastic.elastic_logger import consume_messages

elastic_router = APIRouter()


# Elasticsearch bağlantısını başlat
es = Elasticsearch(hosts=["http://elasticsearch:9200"])

# Elasticsearch bağlantısını kontrol eden bir endpoint
@elastic_router.get("/health")
async def check_elasticsearch_health():
    try:
        health = es.cluster.health()
        return {"status": health["status"]}
    except es_exceptions.ConnectionError:
        raise HTTPException(status_code=500, detail="Cannot connect to Elasticsearch")

# Elasticsearch'e veri ekleyen bir endpoint
@elastic_router.post("/index/{index_name}")
async def index_document(index_name: str, document: dict):
    try:
        response = es.index(index=index_name, document=document)
        return {"result": response["result"], "id": response["_id"]}
    except es_exceptions.ElasticsearchException as e:
        raise HTTPException(status_code=500, detail=f"Elasticsearch error: {str(e)}")


# Elasticsearch'ten veri sorgulayan bir endpoint
@elastic_router.get("/search-all/{index}/")
async def search_data(index: str, size: int = 10):
    """
    Elasticsearch'ten tüm verileri sorgular. Varsayılan olarak 10 belge döndürür.
    """
    try:
        # Elasticsearch'ten sorgu yapıyoruz
        response = es.search(index=index, body={
            "query": {
                "match_all": {}  # Tüm alanlarda arama yapar
            }
        }, size=size)

        # Sonuçları döndürüyoruz
        return {"hits": response["hits"]["hits"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying Elasticsearch: {str(e)}")



@elastic_router.get("/search/{index}/")
async def search_data(index: str, field: str = Query(...), value: str = Query(...)):
    try:
        # Elasticsearch'ten dinamik sorgu yapıyoruz
        response = es.search(index=index, body={
            "query": {
                "match": {  # Belirtilen alanda arama yapıyoruz
                    field: value
                }
            }
        })

        # Sonuçları döndürüyoruz
        return {"hits": response["hits"]["hits"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error querying Elasticsearch: {str(e)}")
    


@elastic_router.get("/consume/{topic}")
def consume_messages_router(topic: str, timeout: float = 1.0, max_messages: int = 10):
    """
    Kafka'dan mesajları tüketmek ve Elasticsearch'e kaydetmek için bir endpoint.
    """
    try:
        response = consume_messages(topic=topic, timeout=timeout, max_messages=max_messages)
        return response
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

