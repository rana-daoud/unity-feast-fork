from datetime import timedelta

from feast import Entity, FeatureView, FileSource
from feast.field import Field
from feast.types import Float32, Int64, String

driver_hourly_stats = FileSource(
    path="test_entity/data/driver_stats_with_string.parquet",
    timestamp_field="event_timestamp",
    created_timestamp_column="created",
)

driver = Entity(name="driver_id", description="driver id")


driver_hourly_stats_view = FeatureView(
    name="driver_stats",
    entities=[driver],
    ttl=timedelta(days=365),
    schema=[
        Field(name="conv_rate", dtype=Float32),
        Field(name="acc_rate", dtype=Float32),
        Field(name="avg_daily_trips", dtype=Int64),
        Field(name="string_feature", dtype=String),
    ],
    online=True,
    source=driver_hourly_stats,
    tags={},
)

