-- Run inside the container:  spark-sql -f /srv/chronon/register_tables.sql
-- Registers the four raw CSVs as date-partitioned parquet tables in `data`.

CREATE DATABASE IF NOT EXISTS data;

-- ---------- transactions ----------
DROP TABLE IF EXISTS data.transactions_raw;
CREATE TABLE data.transactions_raw
USING csv
OPTIONS (path 'file:///srv/chronon/payments_data/transactions.csv', header 'true', inferSchema 'true');

DROP TABLE IF EXISTS data.transactions;
CREATE TABLE data.transactions
USING parquet
PARTITIONED BY (ds)
AS SELECT
     CAST(txn_id      AS BIGINT) AS txn_id,
     CAST(user_id     AS BIGINT) AS user_id,
     CAST(merchant_id AS BIGINT) AS merchant_id,
     CAST(device_id   AS STRING) AS device_id,
     CAST(channel     AS STRING) AS channel,
     CAST(card_type   AS STRING) AS card_type,
     CAST(status      AS STRING) AS status,
     CAST(amount      AS BIGINT) AS amount,
     CAST(ts          AS BIGINT) AS ts,
     CAST(ds          AS STRING) AS ds
   FROM data.transactions_raw;

-- ---------- chargebacks ----------
DROP TABLE IF EXISTS data.chargebacks_raw;
CREATE TABLE data.chargebacks_raw
USING csv
OPTIONS (path 'file:///srv/chronon/payments_data/chargebacks.csv', header 'true', inferSchema 'true');

DROP TABLE IF EXISTS data.chargebacks;
CREATE TABLE data.chargebacks
USING parquet
PARTITIONED BY (ds)
AS SELECT
     CAST(chargeback_id AS BIGINT) AS chargeback_id,
     CAST(txn_id        AS BIGINT) AS txn_id,
     CAST(user_id       AS BIGINT) AS user_id,
     CAST(amount        AS BIGINT) AS amount,
     CAST(ts            AS BIGINT) AS ts,
     CAST(ds            AS STRING) AS ds
   FROM data.chargebacks_raw;

-- ---------- merchants (daily snapshots) ----------
DROP TABLE IF EXISTS data.merchants_raw;
CREATE TABLE data.merchants_raw
USING csv
OPTIONS (path 'file:///srv/chronon/payments_data/merchants.csv', header 'true', inferSchema 'true');

DROP TABLE IF EXISTS data.merchants;
CREATE TABLE data.merchants
USING parquet
PARTITIONED BY (ds)
AS SELECT
     CAST(merchant_id  AS BIGINT) AS merchant_id,
     CAST(category     AS STRING) AS category,
     CAST(kyc_tier     AS INT)    AS kyc_tier,
     CAST(onboarded_ds AS STRING) AS onboarded_ds,
     CAST(ds           AS STRING) AS ds
   FROM data.merchants_raw;

-- ---------- auth_attempts (the Join's left side) ----------
DROP TABLE IF EXISTS data.auth_attempts_raw;
CREATE TABLE data.auth_attempts_raw
USING csv
OPTIONS (path 'file:///srv/chronon/payments_data/auth_attempts.csv', header 'true', inferSchema 'true');

DROP TABLE IF EXISTS data.auth_attempts;
CREATE TABLE data.auth_attempts
USING parquet
PARTITIONED BY (ds)
AS SELECT
     CAST(auth_id     AS BIGINT) AS auth_id,
     CAST(user_id     AS BIGINT) AS user_id,
     CAST(merchant_id AS BIGINT) AS merchant_id,
     CAST(device_id   AS STRING) AS device_id,
     CAST(amount      AS BIGINT) AS amount,
     CAST(ts          AS BIGINT) AS ts,
     CAST(ds          AS STRING) AS ds
   FROM data.auth_attempts_raw;

-- ---------- verification ----------
SHOW TABLES IN data;
SELECT 'transactions'  AS t, COUNT(*) AS n, MIN(ds) AS first_ds, MAX(ds) AS last_ds FROM data.transactions
UNION ALL SELECT 'chargebacks',   COUNT(*), MIN(ds), MAX(ds) FROM data.chargebacks
UNION ALL SELECT 'merchants',     COUNT(*), MIN(ds), MAX(ds) FROM data.merchants
UNION ALL SELECT 'auth_attempts', COUNT(*), MIN(ds), MAX(ds) FROM data.auth_attempts;
