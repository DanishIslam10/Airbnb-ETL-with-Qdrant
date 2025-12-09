import pandas as pd
import os
from sqlalchemy import create_engine
from dotenv import load_dotenv
from google import genai
from qdrant_client import QdrantClient , models
import json
from pathlib import Path

load_dotenv()

USER = os.getenv("USER")
PASSWORD = os.getenv("PASSWORD")
HOST = os.getenv("HOST")
PORT = os.getenv("PORT")
DBNAME = os.getenv("DBNAME")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
gemini_client = genai.Client(api_key=GEMINI_API_KEY)
EMBED_MODEL = "text-embedding-004"

QDRANT_API_KEY = os.getenv("QDRANT_API_KEY")
QDRANT_CLUSTER_ENDPOINT = os.getenv("QDRANT_CLUSTER_ENDPOINT")

qdrant_client = QdrantClient(
    url=QDRANT_CLUSTER_ENDPOINT,
    api_key= QDRANT_API_KEY,
)

COLLECTION_NAME = "airbnb_embeddings"

DATABASE_URL = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"

STATE_FILE = Path(".progress.json")
DEFAULT_LIMIT = 100


def load_state():
    if STATE_FILE.exists():
        try:
            with STATE_FILE.open("r") as f:
                data = json.load(f)
            return int(data.get("last_seen_id", None))
        except Exception:
            return None
    return None


def save_state(last_seen_id: int):
    """Save the current id to disk."""
    STATE_FILE.write_text(json.dumps({"last_seen_id": last_seen_id}))

def load_clean_data(last_seen_id: int = 0, limit: int = DEFAULT_LIMIT) -> pd.DataFrame:
    engine = create_engine(DATABASE_URL)
    if last_seen_id is None:
        # first batch from airbnb_listings table
        query = f"SELECT * FROM airbnb_listings_clean ORDER BY listing_id LIMIT {limit}"
    else:
        # subsequent batches
        query = f"SELECT * FROM airbnb_listings_clean WHERE listing_id > {last_seen_id} ORDER BY listing_id LIMIT {limit}"
        
    df = pd.read_sql(query,engine)
    print("DataFrame loaded from postgres : ", df.shape, f"(last_seen_id={last_seen_id}, limit={limit})")
    return df

def build_embedding_text(df:pd.DataFrame) -> pd.DataFrame:
    
    numeric_cols = ['accommodates','bedrooms','price','minimum_nights','maximum_nights']
    
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)
    
    def row_to_text(row):
        
        parts = [
            f"Listing name: {row.get('name', '')}",
            f"District: {row.get('district', 'unknown')}",
            f"City: {row.get('city', 'unknown')}",
            f"Property type: {row.get('property_type', '')}",
            f"Room type: {row.get('room_type', '')}",
            f"Price: {row.get('price', '')}",
            f"Accommodates: {row.get('accommodates', '')} guests",
            f"Bedrooms: {row.get('bedrooms', '')}",
            f"Minimum Nights to stay: {row.get('minimum_nights', '')}",
            f"Maximum Nights to stay: {row.get('maximum_nights', '')}",
            f"Reviews: {row.get('text_reviews', '')}",
        ]
        
        return " | ".join(parts)
        
    df['embedding_text'] = df.apply(row_to_text,axis=1)
    print("Sample embedding_text:\n", df["embedding_text"].head())
    
    return df

def embed_batch(texts):
    
    result = gemini_client.models.embed_content(
        model=EMBED_MODEL,
        contents=texts,
    )
    batch_embeddings = [emb.values for emb in result.embeddings]
    return batch_embeddings

def embed_all(df:pd.DataFrame,text_col:str = "embedding_text",batch_size:int = 100):
    
    vectors = []
    n = len(df)
    
    for start in range(0, n, batch_size):
        end = min(start + batch_size, n)
        batch_texts = df[text_col].iloc[start:end].tolist()
        batch_vecs = embed_batch(batch_texts)
        vectors.extend(batch_vecs)
        print(f"Embedded rows {start}–{end} / {n}")

    return vectors

def create_collection(vector_size: int):
    # Check if collection exists
    collections = qdrant_client.get_collections().collections
    existing = {c.name for c in collections}

    if COLLECTION_NAME in existing:
        print(f"Collection '{COLLECTION_NAME}' already exists. Skipping creation.")
        return

    qdrant_client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=models.VectorParams(
            size=vector_size,
            distance=models.Distance.COSINE,
        ),
    )

    print(f"Collection '{COLLECTION_NAME}' created with vector size={vector_size}.")
    
def upload_to_qdrant(df: pd.DataFrame, vectors, batch_size: int = 250):
   
    dimensions = len(vectors[0])
    create_collection(dimensions)

    n = len(df)
    for start in range(0, n, batch_size):
        end = min(start + batch_size, n)
        batch_df = df.iloc[start:end]
        batch_vecs = vectors[start:end]

        points = []
        for row, vec in zip(batch_df.to_dict(orient="records"), batch_vecs):
            point_id = int(row["listing_id"]) 
            payload = row  
            points.append(
                models.PointStruct(
                    id=point_id,
                    vector=vec,
                    payload=payload,
                )
            )

        qdrant_client.upsert(
            collection_name=COLLECTION_NAME,
            points=points,
        )
        print(f"Upserted points {start}–{end} / {n} into Qdrant.")

if __name__ == "__main__":
    last_seen_id = load_state()
    limit = DEFAULT_LIMIT
    
    df = load_clean_data(last_seen_id=last_seen_id,limit=limit)
    
    if df.empty:
        print(f"No rows fetched after listing_id {last_seen_id}. Nothing to do.")
    else:
        df = build_embedding_text(df)
        vectors = embed_all(df, text_col="embedding_text", batch_size=100)
        print("got embeddings for ",len(vectors)," rows")
        upload_to_qdrant(df,vectors,batch_size=250) 
        new_last_seen_id = int(df['listing_id'].max())
        save_state(new_last_seen_id)
        print(f"Saved new last_seen_id = {new_last_seen_id} to {STATE_FILE}")