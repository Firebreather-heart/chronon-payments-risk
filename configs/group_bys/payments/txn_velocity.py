"""Feature set 1 - realtime transaction velocity, keyed on user.

This is the fraud-catching feature set: how much and how often has this user
transacted recently. Short windows are the point, so accuracy must be TEMPORAL.
Setting `topic` makes that the default.
"""
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy, Aggregation, Operation, Window, TimeUnit

source = Source(
    events=EventSource(
        table="data.transactions",
        topic="events.transactions",      # presence of a topic => TEMPORAL accuracy
        query=Query(
            selects=select("user_id", "amount"),
            wheres=["status = 'SETTLED'"], # failed and pending attempts are not spend
            time_column="ts",
        ),
    ))

short_windows = [
    Window(length=1,  timeUnit=TimeUnit.HOURS),
    Window(length=24, timeUnit=TimeUnit.HOURS),
    Window(length=7,  timeUnit=TimeUnit.DAYS),
    Window(length=30, timeUnit=TimeUnit.DAYS),
]

v1 = GroupBy(
    sources=[source],
    keys=["user_id"],
    online=True,
    aggregations=[
        Aggregation(input_column="amount", operation=Operation.SUM,     windows=short_windows),
        Aggregation(input_column="amount", operation=Operation.COUNT,   windows=short_windows),
        Aggregation(input_column="amount", operation=Operation.AVERAGE, windows=short_windows),
        Aggregation(input_column="amount", operation=Operation.MAX,     windows=short_windows),
        Aggregation(input_column="amount", operation=Operation.LAST_K(5)),
    ],
)
