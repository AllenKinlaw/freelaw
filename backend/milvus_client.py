import os
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

COLLECTION_NAME = "sc_legal_rag"
VECTOR_DIM = 768  # ModernBERT-base output dimension


def connect_milvus() -> None:
    uri = os.environ["MILVUS_URI"]
    token = os.environ["MILVUS_TOKEN"]
    connections.connect(alias="default", uri=uri, token=token)
    print(f"[Milvus] Connected to {uri}")


REQUIRED_FIELDS = {
    "id", "opinion_id", "cluster_id", "case_name", "court",
    "year", "opinion_type", "chunk_index", "text", "citation", "vector",
}


def _build_schema() -> CollectionSchema:
    fields = [
        FieldSchema(name="id",           dtype=DataType.INT64,        is_primary=True, auto_id=True),
        FieldSchema(name="opinion_id",   dtype=DataType.INT64),
        FieldSchema(name="cluster_id",   dtype=DataType.INT64),
        FieldSchema(name="case_name",    dtype=DataType.VARCHAR,      max_length=512),
        FieldSchema(name="court",        dtype=DataType.VARCHAR,      max_length=20),
        FieldSchema(name="year",         dtype=DataType.INT32),
        FieldSchema(name="opinion_type", dtype=DataType.VARCHAR,      max_length=50),
        FieldSchema(name="chunk_index",  dtype=DataType.INT32),
        FieldSchema(name="text",         dtype=DataType.VARCHAR,      max_length=65535),
        FieldSchema(name="citation",     dtype=DataType.VARCHAR,      max_length=512),
        FieldSchema(name="vector",       dtype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM),
    ]
    return CollectionSchema(fields, description="South Carolina Legal Opinions - RAG")


def get_or_create_collection() -> Collection:
    if utility.has_collection(COLLECTION_NAME):
        col        = Collection(COLLECTION_NAME)
        existing   = {f.name for f in col.schema.fields}
        if REQUIRED_FIELDS.issubset(existing):
            print(f"[Milvus] Loading existing collection: {COLLECTION_NAME}")
            col.load()
            return col
        # Schema is outdated (e.g. missing 'citation') — drop and recreate
        print(f"[Milvus] Schema outdated — dropping '{COLLECTION_NAME}' to apply new schema")
        utility.drop_collection(COLLECTION_NAME)

    print(f"[Milvus] Creating collection: {COLLECTION_NAME}")
    col = Collection(COLLECTION_NAME, _build_schema())
    col.create_index("vector", {
        "metric_type": "COSINE",
        "index_type":  "HNSW",
        "params":      {"M": 16, "efConstruction": 200},
    })
    col.load()
    print(f"[Milvus] Collection '{COLLECTION_NAME}' created and indexed.")
    return col
