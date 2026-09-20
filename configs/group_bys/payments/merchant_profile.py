"""Feature set 3 - merchant attributes, no aggregation.

The source's primary key already matches the GroupBy key, so there is nothing
to aggregate: the selected fields become features directly.

This is also the point-in-time payoff. `data.merchants` holds one full snapshot
per day, so a backfill for an auth in August reads August's snapshot and sees
the merchant's kyc_tier and category AS THEY WERE THEN. Joining the current
merchants table instead would leak a later reclassification backwards.
"""
from ai.chronon.api.ttypes import Source, EntitySource
from ai.chronon.query import Query, select
from ai.chronon.group_by import GroupBy

source = Source(
    entities=EntitySource(
        snapshotTable="data.merchants",
        query=Query(
            selects=select("merchant_id", "category", "kyc_tier", "onboarded_ds"),
        ),
    ))

v1 = GroupBy(
    sources=[source],
    keys=["merchant_id"],
    aggregations=None,
    online=True,
)
