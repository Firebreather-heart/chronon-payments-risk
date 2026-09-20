"""Feature set 2 - spend bucketed by channel.

Bucketing turns one scalar into a map: {"web": 120000, "app": 45000, ...}.
Bucket columns must be strings online, because Chronon uses Avro in the
serving environment.
"""
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit

source = Source(
    events=EventSource(
        table="data.transactions",
        topic="events.transactions",
        query=Query(
            selects=select("user_id", "amount", "channel"),
            wheres=["status = 'SETTLED'"],
            time_column="ts",
        ),
    ))

windows = [Window(length=d, timeUnit=TimeUnit.DAYS) for d in [7, 30]]

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    aggregations=[
        Aggregation(input_column="amount", operation=Operation.SUM,
                    windows=windows, buckets=["channel"]),
        Aggregation(input_column="amount", operation=Operation.COUNT,
                    windows=windows, buckets=["channel"]),
    ],
)
