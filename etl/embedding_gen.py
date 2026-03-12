"""
Embedding Generator for ClinicalTrials Knowledge Graph.

Generates sentence-transformer embeddings for trial descriptions and
condition names, then stores them in Samyama's vector index.
"""

from samyama import SamyamaClient
from sentence_transformers import SentenceTransformer
import numpy as np
import time

# Model config
MODEL_NAME = "all-MiniLM-L6-v2"
EMBEDDING_DIM = 384
BATCH_SIZE = 100

# Vector index settings
TRIAL_INDEX_LABEL = "ClinicalTrial"
TRIAL_INDEX_PROPERTY = "embedding"
CONDITION_INDEX_LABEL = "Condition"
CONDITION_INDEX_PROPERTY = "embedding"
METRIC = "cosine"


def _load_model() -> SentenceTransformer:
    """Load the sentence-transformers model."""
    print(f"Loading model: {MODEL_NAME}...")
    model = SentenceTransformer(MODEL_NAME)
    print(f"Model loaded. Embedding dimension: {EMBEDDING_DIM}")
    return model


def _create_vector_indexes(client: SamyamaClient) -> None:
    """Create vector indexes for ClinicalTrial and Condition nodes."""
    print("Creating vector indexes...")

    client.create_vector_index(
        TRIAL_INDEX_LABEL, TRIAL_INDEX_PROPERTY, EMBEDDING_DIM, METRIC
    )
    print(f"  Created index: {TRIAL_INDEX_LABEL}.{TRIAL_INDEX_PROPERTY} "
          f"({EMBEDDING_DIM}d, {METRIC})")

    client.create_vector_index(
        CONDITION_INDEX_LABEL, CONDITION_INDEX_PROPERTY, EMBEDDING_DIM, METRIC
    )
    print(f"  Created index: {CONDITION_INDEX_LABEL}.{CONDITION_INDEX_PROPERTY} "
          f"({EMBEDDING_DIM}d, {METRIC})")


def _embed_trials(client: SamyamaClient, model: SentenceTransformer) -> int:
    """
    Generate embeddings for ClinicalTrial nodes using brief_summary.

    Returns the number of trials embedded.
    """
    print("\nEmbedding ClinicalTrial nodes (brief_summary)...")

    rows = client.query_readonly(
        "default",
        "MATCH (t:ClinicalTrial) RETURN id(t), t.brief_summary"
    )

    # Filter out trials with no summary
    trials = [(row[0], row[1]) for row in rows if row and row[1]]
    total = len(trials)
    print(f"  Found {total} trials with brief_summary.")

    if total == 0:
        return 0

    embedded_count = 0
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = trials[batch_start:batch_end]

        # Extract texts for this batch
        texts = [summary for _, summary in batch]

        # Generate embeddings
        embeddings = model.encode(texts, show_progress_bar=False)

        # Store each embedding in the vector index
        for (node_id, _), embedding in zip(batch, embeddings):
            vector_list = embedding.tolist()
            client.add_vector(
                TRIAL_INDEX_LABEL, TRIAL_INDEX_PROPERTY, node_id, vector_list
            )

        embedded_count += len(batch)
        print(f"  Progress: {embedded_count}/{total} trials embedded "
              f"({embedded_count * 100 // total}%)")

    return embedded_count


def _embed_conditions(client: SamyamaClient, model: SentenceTransformer) -> int:
    """
    Generate embeddings for Condition nodes using name.

    Returns the number of conditions embedded.
    """
    print("\nEmbedding Condition nodes (name)...")

    rows = client.query_readonly(
        "default",
        "MATCH (c:Condition) RETURN id(c), c.name"
    )

    # Filter out conditions with no name
    conditions = [(row[0], row[1]) for row in rows if row and row[1]]
    total = len(conditions)
    print(f"  Found {total} conditions with name.")

    if total == 0:
        return 0

    embedded_count = 0
    for batch_start in range(0, total, BATCH_SIZE):
        batch_end = min(batch_start + BATCH_SIZE, total)
        batch = conditions[batch_start:batch_end]

        # Extract texts for this batch
        texts = [name for _, name in batch]

        # Generate embeddings
        embeddings = model.encode(texts, show_progress_bar=False)

        # Store each embedding in the vector index
        for (node_id, _), embedding in zip(batch, embeddings):
            vector_list = embedding.tolist()
            client.add_vector(
                CONDITION_INDEX_LABEL, CONDITION_INDEX_PROPERTY, node_id, vector_list
            )

        embedded_count += len(batch)
        print(f"  Progress: {embedded_count}/{total} conditions embedded "
              f"({embedded_count * 100 // total}%)")

    return embedded_count


def generate_embeddings(client: SamyamaClient) -> dict:
    """
    Main entry point: generates and stores embeddings for trials and conditions.

    1. Loads the sentence-transformers model (all-MiniLM-L6-v2, 384d).
    2. Creates vector indexes for ClinicalTrial and Condition nodes.
    3. Encodes ClinicalTrial.brief_summary into vectors.
    4. Encodes Condition.name into vectors.
    5. Stores all vectors in Samyama's HNSW vector index.

    Args:
        client: SamyamaClient instance (embedded or remote).

    Returns:
        dict with stats: trials_embedded, conditions_embedded.
    """
    start_time = time.time()

    model = _load_model()
    _create_vector_indexes(client)

    trials_count = _embed_trials(client, model)
    conditions_count = _embed_conditions(client, model)

    elapsed = time.time() - start_time
    stats = {
        "trials_embedded": trials_count,
        "conditions_embedded": conditions_count,
    }

    print(f"\nDone. Embedded {trials_count} trials and {conditions_count} conditions "
          f"in {elapsed:.1f}s.")
    return stats


if __name__ == "__main__":
    client = SamyamaClient.embedded()
    generate_embeddings(client)
