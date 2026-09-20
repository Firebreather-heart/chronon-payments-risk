"""Generate the raw CSVs for the Nigerian payments fraud scenario.

Run this on your Mac (not in the container). It writes ./payments_data/*.csv,
which you then copy into the container and register as tables.

Every timestamp is epoch milliseconds, UTC. Every amount is kobo (integer).
"""
import csv, os, random
from datetime import datetime, timedelta, timezone

random.seed(7)
OUT = "payments_data"
os.makedirs(OUT, exist_ok=True)

END = datetime(2026, 9, 20, tzinfo=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
DAYS = 90                             # was 60
WARMUP_DAYS = 30                      # new: must be >= longest window
START = END - timedelta(days=DAYS)
AUTH_START = START + timedelta(days=WARMUP_DAYS)   # new

def ms(dt): return int(dt.timestamp() * 1000)
def ds(dt): return dt.strftime("%Y-%m-%d")

N_USERS = 2000
N_MERCHANTS = 120
N_DEVICES = 1500


rows = []
txn_id = 0
for u in range(1, N_USERS + 1):
    n = random.choice([3, 8, 14, 21, 45])
    if u == 1:                      # deliberate hot key
        n = 25_000
    for _ in range(n):
        dt = START + timedelta(seconds=random.randint(0, DAYS * 86400 - 1))
        txn_id += 1
        rows.append({
            "txn_id": txn_id,
            "user_id": u,
            "merchant_id": random.randint(1, N_MERCHANTS),
            "device_id": f"dev{random.randint(1, N_DEVICES)}",
            "channel": random.choice(["web", "app", "ussd"]),
            "card_type": random.choice(["visa", "mastercard", "verve"]),
            "status": random.choices(["SETTLED", "FAILED", "PENDING"], [0.90, 0.07, 0.03])[0],
            "amount": random.randint(50_000, 2_000_000),
            "ts": ms(dt),
            "ds": ds(dt),
        })

# a fraud ring: 6 users sharing one device, bursting inside ten minutes
ring_day = END - timedelta(days=5)
for i, u in enumerate(range(9001, 9007)):
    for k in range(8):
        dt = ring_day + timedelta(hours=14, minutes=i + k, seconds=random.randint(0, 59))
        txn_id += 1
        rows.append({
            "txn_id": txn_id, "user_id": u, "merchant_id": 77,
            "device_id": "devRING", "channel": "web", "card_type": "visa",
            "status": "SETTLED", "amount": random.randint(400_000, 600_000),
            "ts": ms(dt), "ds": ds(dt),
        })

with open(f"{OUT}/transactions.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(rows[0].keys())); w.writeheader(); w.writerows(rows)
print(f"transactions: {len(rows):,}")


cb = []
for i, r in enumerate(random.sample(rows, k=max(1, len(rows) // 400))):
    dt = datetime.fromtimestamp(r["ts"] / 1000, tz=timezone.utc) + timedelta(days=random.randint(1, 10))
    if dt >= END:
        continue
    cb.append({
        "chargeback_id": i + 1, "txn_id": r["txn_id"], "user_id": r["user_id"],
        "amount": r["amount"], "ts": ms(dt), "ds": ds(dt),
    })
with open(f"{OUT}/chargebacks.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(cb[0].keys())); w.writeheader(); w.writerows(cb)
print(f"chargebacks: {len(cb):,}")

# ------------------------------------------------------- merchants (snapshots)
# One full copy of the merchant table per day. kyc_tier changes for a few
# merchants partway through, which is what makes point-in-time correctness
# observable rather than theoretical.
base = {m: {
    "merchant_id": m,
    "category": random.choice(["electronics", "groceries", "travel", "betting"]),
    "kyc_tier": random.choice([1, 2, 3]),
    "onboarded_ds": ds(START - timedelta(days=random.randint(1, 900))),
} for m in range(1, N_MERCHANTS + 1)}

upgrade_day = END - timedelta(days=20)
merch_rows = []
d = START
while d < END:
    for m, rec in base.items():
        tier = rec["kyc_tier"]
        cat = rec["category"]
        if m <= 10 and d >= upgrade_day:
            tier = 3                       # ten merchants upgraded mid-window
        if m == 77 and d >= upgrade_day:
            cat = "betting"                # one merchant reclassified
        merch_rows.append({
            "merchant_id": m, "category": cat, "kyc_tier": tier,
            "onboarded_ds": rec["onboarded_ds"], "ds": ds(d),
        })
    d += timedelta(days=1)
with open(f"{OUT}/merchants.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(merch_rows[0].keys())); w.writeheader(); w.writerows(merch_rows)
print(f"merchant snapshots: {len(merch_rows):,}")

auth_pool = [r for r in rows
             if datetime.fromtimestamp(r["ts"]/1000, tz=timezone.utc) >= AUTH_START]
auth = []
for i, r in enumerate(random.sample(auth_pool, k=len(auth_pool) // 5)):
    auth.append({
        "auth_id": i + 1, "user_id": r["user_id"], "merchant_id": r["merchant_id"],
        "device_id": r["device_id"], "amount": r["amount"],
        "ts": r["ts"] - 1500,            # the auth happens 1.5s before the txn lands
        "ds": r["ds"],
    })
with open(f"{OUT}/auth_attempts.csv", "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(auth[0].keys())); w.writeheader(); w.writerows(auth)
print(f"auth attempts: {len(auth):,}")
print(f"\nwrote to ./{OUT}/")
