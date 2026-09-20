"""StagingQuery - free-form ETL that the simple source Query cannot express.

Here: join transactions to chargebacks to produce a per-transaction outcome
table. A GroupBy's Query can only select and filter columns of ONE source, so a
join between two tables has to happen here first.

The {{ start_date }} and {{ end_date }} macros are how Chronon expresses
fill-what-is-missing: you do not hardcode dates, the engine substitutes the
range it still needs.
"""
from ai.chronon.api.ttypes import StagingQuery, MetaData

query = """
SELECT
    t.txn_id,
    t.user_id,
    t.merchant_id,
    t.amount,
    t.ts,
    CASE WHEN c.chargeback_id IS NOT NULL THEN 1 ELSE 0 END AS was_charged_back,
    t.ds
FROM data.transactions t
LEFT JOIN data.chargebacks c
  ON c.txn_id = t.txn_id
WHERE t.ds BETWEEN '{{ start_date }}' AND '{{ end_date }}'
  AND t.status = 'SETTLED'
  AND t.ts < (unix_timestamp() * 1000 - 30 * 86400000)
"""

v1 = StagingQuery(
    query=query,
    startPartition="2026-07-22",
    metaData=MetaData(
        name="txn_outcomes",
        outputNamespace="default",
        dependencies=["data.transactions", "data.chargebacks"],
    ),
)
