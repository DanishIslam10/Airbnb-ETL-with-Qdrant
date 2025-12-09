from save_embeddings import embed_batch,qdrant_client,COLLECTION_NAME

def semantic_search(query: str, top_k: int = 3):
    query_vec = embed_batch([query])[0]

    # query_points API for similarity search :contentReference[oaicite:4]{index=4}
    results = qdrant_client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_vec,
        limit=top_k,
        with_payload=True,
        with_vectors=False,
    )

    print(f"\nTop {top_k} hits for: '{query}'")
    for i, point in enumerate(results.points, start=1):
        payload = point.payload
        print(f"\nRank {i} | id={point.id} | score={point.score}")
        print("Name:", payload.get("name"))
        print("City:", payload.get("city"), "| District:", payload.get("district"))
        print("Property type:", payload.get("property_type"), "| Room type:", payload.get("room_type"))
        print("Price:", payload.get("price"))
        print("Sample text:", (payload.get("embedding_text") or "")[:200], "...")
        print("-" * 60)
        
if __name__ == "__main__":
    semantic_search(query="cozy flat in paris")