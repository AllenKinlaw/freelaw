import os
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

COLLECTION_NAME = "sc_legal_rag"
VECTOR_DIM = 768  # ModernBERT-base output dimension


def connect_milvus() -> None:
    uri = os.environ["MILVUS_URI"]
    token = os.environ["MILVUS_TOKEN"]
    connections.connect(alias="default", uri=uri, token=token)
    print(f"[Milvus] Connected to {uri}")


def get_or_create_collection() -> Collection:
    if utility.has_collection(COLLECTION_NAME):
        print(f"[Milvus] Loading existing collection: {COLLECTION_NAME}")
        col = Collection(COLLECTION_NAME)
        col.load()
        return col

    print(f"[Milvus] Creating collection: {COLLECTION_NAME}")
    fields = [
        FieldSchema(name="id",           dtype=DataType.INT64,         is_primary=True, auto_id=True),
        FieldSchema(name="opinion_id",   dtype=DataType.INT64),
        FieldSchema(name="cluster_id",   dtype=DataType.INT64),
        FieldSchema(name="case_name",    dtype=DataType.VARCHAR,       max_length=512),
        FieldSchema(name="court",        dtype=DataType.VARCHAR,       max_length=20),
        FieldSchema(name="year",         dtype=DataType.INT32),
        FieldSchema(name="opinion_type", dtype=DataType.VARCHAR,       max_length=50),
        FieldSchema(name="chunk_index",  dtype=DataType.INT32),
        FieldSchema(name="text",         dtype=DataType.VARCHAR,       max_length=65535),
        FieldSchema(name="vector",       dtype=DataType.FLOAT_VECTOR,  dim=VECTOR_DIM),
    ]
    schema = CollectionSchema(fields, description="South Carolina Legal Opinions - RAG")
    col = Collection(COLLECTION_NAME, schema)

    # HNSW index — fast approximate nearest-neighbor, good for serverless Zilliz
    index_params = {
        "metric_type": "COSINE",
        "index_type": "HNSW",
        "params": {"M": 16, "efConstruction": 200},
    }
    col.create_index("vector", index_params)
    col.load()
    print(f"[Milvus] Collection '{COLLECTION_NAME}' created and indexed.")
    return col
