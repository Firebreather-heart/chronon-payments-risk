"""Feature set 4 - distinct users per device.

Keyed on device_id rather than user_id, which is the interesting move: it is a
different grain from the other GroupBys, and Chronon will happily join features
at several grains into one row.

Many distinct users on one device inside a short window is a classic fraud-ring
signal. approx_unique_count rather than unique_count because the exact version
must retain every distinct value it has seen, so it does not scale; the docs
support it but discourage it.
"""
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit

source = Source(
    events=EventSource(
        table="data.transactions",
        topic="events.transactions",
        query=Query(
            selects=select("device_id", "user_id", "amount"),
            time_column="ts",
        ),
    ))

windows = [
    Window(length=1,  timeUnit=TimeUnit.HOURS),
    Window(length=24, timeUnit=TimeUnit.HOURS),
    Window(length=30, timeUnit=TimeUnit.DAYS),
]

v1 = GroupBy(
    sources=[source],
    keys=["device_id"],
    online=True,
    aggregations=[
        Aggregation(input_column="user_id", operation=Operation.APPROX_UNIQUE_COUNT, windows=windows),
        Aggregation(input_column="amount",  operation=Operation.COUNT,               windows=windows),
    ],
)
