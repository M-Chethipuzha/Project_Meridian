from datetime import timedelta

import pandas as pd
from feast import FeatureService, FeatureView, Field, FileSource, PushSource
from feast.data_format import ParquetFormat
from feast.infra.offline_stores.file_source import FileSource
from feast.types import Array, Bytes, Float32, Float64, Int32, Int64, String, UnixTimestamp

# ── Entity ────────────────────────────────────────────────────────────────
# An event is identified by its unique ID and the wiki it belongs to.

event = Entity(
    name="event",
    description="A single Wikimedia ChangeEvent",
    join_keys=["event_id"],
)

# ── Parquet File Source ────────────────────────────────────────────────────
# Points to a directory of time-partitioned Parquet files written by the
# consumer service, e.g.:
#   s3://meridian-events/dt=2026-06-21/hour=12/data.parquet

events_source = FileSource(
    name="change_events_source",
    path="s3://meridian-events/",
    file_format=ParquetFormat(),
    timestamp_field="created_timestamp",
    created_timestamp_column="parsed_timestamp",
)

# ── Feature Views ──────────────────────────────────────────────────────────

# Raw event features
event_fv = FeatureView(
    name="change_event_features",
    entities=[event],
    ttl=timedelta(days=90),
    schema=[
        Field(name="type", dtype=String),
        Field(name="namespace", dtype=Int32),
        Field(name="title", dtype=String),
        Field(name="user", dtype=String),
        Field(name="bot", dtype=Int32),
        Field(name="wiki", dtype=String),
        Field(name="timestamp", dtype=UnixTimestamp),
        Field(name="server_name", dtype=String),
        Field(name="comment", dtype=String),
    ],
    source=events_source,
    online=True,
)

# Aggregated edit features (computed daily)
edit_stats_fv = FeatureView(
    name="edit_statistics",
    entities=[event],
    ttl=timedelta(days=365),
    schema=[
        Field(name="edits_last_hour", dtype=Int64),
        Field(name="edits_last_24h", dtype=Int64),
        Field(name="unique_users_last_hour", dtype=Int64),
        Field(name="bot_fraction", dtype=Float64),
        Field(name="avg_edits_per_user", dtype=Float64),
    ],
    source=events_source,
    online=True,
)

# ── Feature Service ────────────────────────────────────────────────────────
# Bundles features for serving via the Feast API

meridian_features = FeatureService(
    name="meridian_features",
    features=[event_fv, edit_stats_fv],
    description="Meridian Stream event features for ML model inference",
)
