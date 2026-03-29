import os
from pymilvus import connections, FieldSchema, CollectionSchema, DataType, Collection, utility

COLLECTION_NAME = "sc_laws_rag"
VECTOR_DIM = 768  # ModernBERT-base


def connect_milvus() -> None:
    uri   = os.environ["MILVUS_URI"]
    token = os.environ["MILVUS_TOKEN"]
    connections.connect(alias="default", uri=uri, token=token)
    print(f"[LawsMilvus] Connected to {uri}")


REQUIRED_FIELDS = {
    "id", "section_id", "title_num", "title_name", "chapter_num",
    "short_title", "chunk_index", "chunk_total", "url", "text", "vector",
}


def _build_schema() -> CollectionSchema:
    fields = [
        FieldSchema(name="id",          dtype=DataType.INT64,        is_primary=True, auto_id=True),
        FieldSchema(name="section_id",  dtype=DataType.VARCHAR,      max_length=64),
        FieldSchema(name="title_num",   dtype=DataType.INT32),
        FieldSchema(name="title_name",  dtype=DataType.VARCHAR,      max_length=256),
        FieldSchema(name="chapter_num", dtype=DataType.INT32),
        FieldSchema(name="short_title", dtype=DataType.VARCHAR,      max_length=512),
        FieldSchema(name="chunk_index", dtype=DataType.INT32),
        FieldSchema(name="chunk_total", dtype=DataType.INT32),
        FieldSchema(name="url",         dtype=DataType.VARCHAR,      max_length=512),
        FieldSchema(name="text",        dtype=DataType.VARCHAR,      max_length=65535),
        FieldSchema(name="vector",      dtype=DataType.FLOAT_VECTOR, dim=VECTOR_DIM),
    ]
    return CollectionSchema(fields, description="South Carolina Code of Laws - RAG")


def get_or_create_collection() -> Collection:
    if utility.has_collection(COLLECTION_NAME):
        col      = Collection(COLLECTION_NAME)
        existing = {f.name for f in col.schema.fields}
        if REQUIRED_FIELDS.issubset(existing):
            print(f"[LawsMilvus] Loading existing collection: {COLLECTION_NAME}")
            col.load()
            return col
        print(f"[LawsMilvus] Schema outdated — dropping '{COLLECTION_NAME}' to apply new schema")
        utility.drop_collection(COLLECTION_NAME)

    print(f"[LawsMilvus] Creating collection: {COLLECTION_NAME}")
    col = Collection(COLLECTION_NAME, _build_schema())
    col.create_index("vector", {
        "metric_type": "COSINE",
        "index_type":  "HNSW",
        "params":      {"M": 16, "efConstruction": 200},
    })
    col.load()
    print(f"[LawsMilvus] Collection '{COLLECTION_NAME}' created and indexed.")
    return col
