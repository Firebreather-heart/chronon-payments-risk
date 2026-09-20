"""The Join: one wide training row per authorization attempt.

The LEFT side is auth_attempts, because that is the moment the risk model is
called. Every feature on the right is computed AS OF each auth's own timestamp.

Note the three different keys on the right side: user_id, merchant_id and
device_id. One row on the left carries all three, so Chronon can attach
features computed at three different grains.
"""
from ai.chronon.api.ttypes import Source, EventSource
from ai.chronon.query import Query, select
from ai.chronon.join import Join, JoinPart

from group_bys.payments.txn_velocity import v1 as txn_velocity
from group_bys.payments.channel_mix import v1 as channel_mix
from group_bys.payments.merchant_profile import v1 as merchant_profile
from group_bys.payments.device_sharing import v1 as device_sharing

source = Source(
    events=EventSource(
        table="data.auth_attempts",
        query=Query(
            selects=select("user_id", "merchant_id", "device_id", "amount"),
            time_column="ts",
        ),
    ))

v1 = Join(
    left=source,
    right_parts=[
        JoinPart(group_by=txn_velocity),
        JoinPart(group_by=channel_mix),
        JoinPart(group_by=merchant_profile),
        JoinPart(group_by=device_sharing),
    ],
    online = True,
)

