from datetime import datetime, timedelta

import numpy as np
from feast import Entity, FeatureView, Field, FileSource, ValueType
from feast.data_format import ParquetFormat
from feast.driver.test_data import create_entity_df
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Float64, Int64, String

# Test helper to generate sample feature data matching the Parquet schema
# for local validation and unit testing.

SAMPLE_COLUMNS = [
    "event_id",
    "type",
    "namespace",
    "title",
    "user",
    "bot",
    "wiki",
    "timestamp",
    "comment",
    "edits_last_hour",
    "edits_last_24h",
    "unique_users_last_hour",
    "bot_fraction",
    "avg_edits_per_user",
]


def generate_test_data(num_rows: int = 100) -> str:
    """
    Generate a test Parquet file with sample feature data.

    Args:
        num_rows: Number of synthetic rows to generate.

    Returns:
        Path to the generated Parquet file.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    now = datetime.utcnow()
    np.random.seed(42)

    types = np.random.choice(["edit", "new", "log"], num_rows, p=[0.7, 0.2, 0.1])
    bots = np.random.binomial(1, 0.15, num_rows)
    users = [f"user_{np.random.randint(0, 1000)}" for _ in range(num_rows)]

    table = pa.table({
        "event_id": np.arange(num_rows, dtype=np.int64),
        "type": types,
        "namespace": np.random.randint(0, 30, num_rows),
        "title": [f"Page_{i}" for i in range(num_rows)],
        "user": users,
        "bot": bots,
        "wiki": np.random.choice(["enwiki", "dewiki", "frwiki", "wikidata"], num_rows),
        "timestamp": pa.array([now - timedelta(minutes=np.random.randint(0, 1440))
                                for _ in range(num_rows)]),
        "comment": np.random.choice(["", "/* edit */", "revert", "update"], num_rows),
        "edits_last_hour": np.random.poisson(lam=50, size=num_rows).astype(np.int64),
        "edits_last_24h": np.random.poisson(lam=1200, size=num_rows).astype(np.int64),
        "unique_users_last_hour": np.random.poisson(lam=10, size=num_rows).astype(np.int64),
        "bot_fraction": np.random.beta(1, 10, num_rows).astype(np.float64),
        "avg_edits_per_user": np.random.exponential(scale=3.0, size=num_rows).astype(np.float64),
    })

    import tempfile
    path = f"{tempfile.gettempdir()}/meridian_feast_test.parquet"
    pq.write_table(table, path)
    return path
