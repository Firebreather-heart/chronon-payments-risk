# Chronon feature pipeline for payment fraud risk

A point-in-time correct feature pipeline for a card-authorization risk model,
built with [Chronon](https://github.com/airbnb/chronon). Four feature sets
computed at three different entity grains, backfilled into a training set and
served from a KV store through one definition.

Synthetic data, single-container deployment. See
[What this isn't](#what-this-isnt).

---

## The result

A risk model is called at authorization. It needs to know what this user, this
merchant and this device look like *at that exact moment*.

Here is the same fetch, with and without the device key, for a member of a
fraud ring:

**User-level features only**

```json
"payments_txn_velocity_v1_amount_count_30d" : 8,
"payments_txn_velocity_v1_amount_average_30d" : 499270.125,
"payments_txn_velocity_v1_amount_max_30d" : 586066,
"payments_device_sharing_v1_exception" : "Missing keys Vector(device_id) ...",
"payments_merchant_profile_v1_exception" : "Missing keys Vector(merchant_id) ..."
```

**All three keys supplied**

```json
"payments_txn_velocity_v1_amount_count_30d" : 8,
"payments_txn_velocity_v1_amount_average_30d" : 499270.125,
"payments_device_sharing_v1_amount_count_30d" : 48,
"payments_device_sharing_v1_user_id_approx_unique_count_30d" : 6
```

Keying on user_id would have masked the nature of the fraud ring, with a key on device_id
we can see that a single device was used by 6 distinct users in 30 days, which is a questionable
pattern. A user_id key would have given us the result above, which seems like a normal user activity.

![fetch user key only](docs/screenshots/fetch-user-key-only.png)

![fetch user and device key](docs/screenshots/fetch-all-keys.png)

---

## Feature sets

| GroupBy | Key | Source type | Accuracy | What it answers |
|---|---|---|---|---|
| `txn_velocity` | `user_id` | EventSource + topic | TEMPORAL | How much and how often has this user transacted, over 1h / 24h / 7d / 30d |
| `channel_mix` | `user_id` | EventSource + topic | TEMPORAL | Spend split by channel, as a map |
| `merchant_profile` | `merchant_id` | EntitySource (daily snapshots) | SNAPSHOT | Category, KYC tier, onboarding date as of that day |
| `device_sharing` | `device_id` | EventSource + topic | TEMPORAL | Distinct users per device, over 1h / 24h / 30d |

- txn_velocity —> keyed on user_id, because the question is what this customer's spending looks like. Short windows are the point: a 1h and 24h view is what catches a burst, and a burst is what fraud looks like. That forces TEMPORAL accuracy, since a value refreshed at midnight is useless for a one-hour window. Setting topic makes TEMPORAL the default. LAST_K(5) is there because sums and averages throw away shape, and ten small purchases look identical to one large one plus nine tiny ones under a mean.

- channel_mix —> keyed on user_id and bucketed by channel, which turns each aggregation into a map rather than a scalar. Two users spending the same total look identical on the sum; one routing everything through USSD when they've only ever used the app is a behavioural change the total cannot express. Bucket columns must be strings because the serving path encodes with Avro. Deliberately not bucketed on merchant_id: 120 values would give a sparse map where most entries are absent for most users, which is noise rather than signal.

- merchant_profile —> keyed on merchant_id, an EntitySource over daily snapshots, with no aggregations at all. The source's primary key already matches the GroupBy key, so the selected columns become features directly. SNAPSHOT accuracy is correct here and TEMPORAL would be waste: a KYC tier changing at 14:03 rather than at midnight has no bearing on risk, and TEMPORAL would mean running change data capture against the merchants table for no benefit. The snapshot table is what makes this safe, since it lets a backfill read the merchant's state as of each auth's own day rather than today's.

- device_sharing —> keyed on device_id, which is a different grain from everything else in the Join, and that is the point. A ring member's own history is unremarkable, so no user-keyed feature can detect the ring; the signal is a property of the device, not of any user. approx_unique_count rather than the exact version because exact distinct counting must retain every value it has seen and therefore does not scale, while the sketch uses bounded memory and, critically, merges, which is what makes windowed aggregation over tiles possible at all.



The Join's left side is `auth_attempts`, one row per model invocation.
Not `transactions`, for two reasons: a transaction is the *outcome* of an auth,
and transactions only exist for auths that were approved, so training on them
would bake in survivorship bias.

---

## Point-in-time correctness

`data.merchants` holds one full snapshot per day. Several merchants upgraded their
KYC tier on 2026-08-31.
![merchant tier change](docs/screenshots/fetch-merchant-kyc-tier.png)
Joining the current merchants table would have stamped tier 3 on every historical row, including
August's, so the model would have learned from the future, which is a form of data leakage.
Chronon allows the model to learn from the data as if walking through time, with each data point 
being as accurate as it was at the time of the event.

---

## Reproduce

Requires Docker. Tested on an M2 Mac (arm64) with Colima.

```bash
# 1. bring up the Chronon quickstart stack
curl -o docker-compose.yml https://chronon.ai/docker-compose.yml
docker-compose up -d

# 2. generate the synthetic data (on the host)
python data/gen_payments_data.py

# 3. copy data and configs into the container
docker-compose cp payments_data/.              main:/srv/chronon/payments_data
docker-compose cp data/register_tables.sql     main:/srv/chronon/register_tables.sql
docker-compose cp configs/group_bys/payments   main:/srv/chronon/group_bys/payments
docker-compose cp configs/joins/payments       main:/srv/chronon/joins/payments

# 4. inside the container
docker-compose exec main bash
```

```bash
# register the team (Chronon requires it before compiling)
python3 -c "
import json; p='/srv/chronon/teams.json'; d=json.load(open(p))
d['payments']=json.loads(json.dumps(d['quickstart'])); json.dump(d,open(p,'w'),indent=2)"

spark-sql -f /srv/chronon/register_tables.sql

compile.py --conf=joins/payments/risk_model.py
run.py --mode analyze  --conf production/joins/payments/risk_model.v1
run.py                 --conf production/joins/payments/risk_model.v1

# online path
for gb in txn_velocity channel_mix merchant_profile device_sharing; do
  run.py --mode upload --conf production/group_bys/payments/$gb.v1 --ds 2026-09-19
  spark-submit --class ai.chronon.quickstart.online.Spark2MongoLoader --master local[*] \
    /srv/onlineImpl/target/scala-2.12/mongo-online-impl-assembly-0.1.0-SNAPSHOT.jar \
    default.payments_${gb}_v1_upload \
    mongodb://admin:admin@mongodb:27017/?authSource=admin
done

run.py --mode metadata-upload --conf production/joins/payments/risk_model.v1
run.py --mode fetch --type join --name payments/risk_model.v1 \
  -k '{"user_id":"9001","merchant_id":"77","device_id":"devRING"}'

compile.py --conf=staging_queries/payments/txn_outcomes.py
run.py --conf production/staging_queries/payments/txn_outcomes.v1
```

---

## What surprised me

1. The quickstart ships a v1 join for backfills and a v2 for an online serving with a reduced feature set, in real applications, this  re-introduces the training-serving skew which chronon was engineered to solve. This repo uses a single join, marked online, for both training and serving.


2. `analyze` refused the first backfill: the longest window was 30 days but
     the event history started the same day as the first prediction row, so the
     earliest rows would have carried understated aggregates. Raw history has
     to start at least one max-window before the first training row.

3. Missing keys come back as `_exception` ENTRIES in the returned map, not as
     thrown errors. Typical glue code doing features.get(name, 0) over a fixed
     list would swallow them and hand the model zeros.

4. This repo doesn't run the streaming path, so the 1h and 24h windows returned null, while 30d answered, the KV store only held the batch data and there was no streaming job running. The fraud ring burst inside ten minutes: exactly where the null was. Staleness there is not a precision loss, it is the difference between prevention and accounting.

---

## What this isn't

Synthetic data, one container, and no streaming job running, so TEMPORAL
features are served from batch uploads only. Nothing here has been operated at
scale: no tuning of a large backfill, no production incident, no multi-terabyte
anything.
