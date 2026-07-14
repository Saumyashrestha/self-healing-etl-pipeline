"""
Shared 'world state' for realistic fake data generation.
Both data_generate.py (seed + incremental) and pipeline.py (mutate_postgres)
import from this file so that prices, product popularity, and customer
behavior stay CONSISTENT everywhere data is created.
"""

import random
import numpy as np

# ---------------------------------------------------------------------------
# 1. FIXED CATALOG PRICES + DISCRETE SALE TIERS
#    Each product_id has ONE base price, plus a SMALL FIXED SET of possible
#    sale prices (10% / 20% / 30% off), instead of a continuously
#    regenerated random markdown. Real catalogs have a handful of price
#    points per product (base + a couple of sale prices), not hundreds.
# ---------------------------------------------------------------------------
NUM_PRODUCTS = 50
PRODUCT_CATALOG = {}
for pid in range(1, NUM_PRODUCTS + 1):
    base_price = round(random.uniform(10.0, 150.0), 2)
    base_price = float(int(base_price)) + 0.99  # realistic ".99" ending

    sale_prices = [
        round(base_price * 0.90, 2),  # 10% off
        round(base_price * 0.80, 2),  # 20% off
        round(base_price * 0.70, 2),  # 30% off
    ]

    PRODUCT_CATALOG[pid] = {"base_price": base_price, "sale_prices": sale_prices}


def get_product_price(product_id, allow_markdown=True):
    """Returns the catalog price for a product: the fixed base price 90% of
    the time, or one of a small fixed set of sale prices 10% of the time.
    This keeps distinct_prices per product to at most 4 (base + 3 tiers),
    matching real-world catalog behavior."""
    entry = PRODUCT_CATALOG[product_id]
    if allow_markdown and random.random() < 0.10:
        return random.choice(entry["sale_prices"])
    return entry["base_price"]


# ---------------------------------------------------------------------------
# 2. BESTSELLER SKEW (TIERED, CAPPED)
#    Instead of one continuous power-law curve (which mathematically forces
#    rank #1 to take ~30-38% of ALL mass), products are split into tiers
#    with an explicit CAP on how much combined share each tier can have.
#    No single product can run away with a third of all sales.
# ---------------------------------------------------------------------------
_product_ids = list(range(1, NUM_PRODUCTS + 1))

_product_tiers = [
    {"ids": list(range(1, 6)),   "share": 0.40},   # 5 "bestsellers"   -> 40% combined
    {"ids": list(range(6, 21)),  "share": 0.40},   # 15 "steady sellers" -> 40% combined
    {"ids": list(range(21, 51)), "share": 0.20},   # 30 "long tail"    -> 20% combined
]


def _build_tiered_weights(tiers, exponent=0.6):
    """Within each tier, apply a mild rank-based decay so there's still some
    natural variation — but the TIER's combined share is fixed, so no single
    id can dominate beyond that tier's cap."""
    weight_map = {}
    for tier in tiers:
        ids = tier["ids"]
        n = len(ids)
        raw = np.array([1 / (rank ** exponent) for rank in range(1, n + 1)])
        raw = raw / raw.sum() * tier["share"]
        for _id, w in zip(ids, raw):
            weight_map[_id] = w
    return weight_map


_product_weight_map = _build_tiered_weights(_product_tiers)
_product_weights = np.array([_product_weight_map[pid] for pid in _product_ids])
_product_weights = _product_weights / _product_weights.sum()


def pick_product_id():
    return int(np.random.choice(_product_ids, p=_product_weights))


# ---------------------------------------------------------------------------
# 3. POWER-BUYER SKEW (TIERED, CAPPED)
#    Same fix as products: a "power buyer" TIER (top 5% of customers)
#    collectively drives a large share of orders, but no single customer_id
#    can end up with 38% of all orders like the old continuous curve caused.
# ---------------------------------------------------------------------------
CUSTOMER_POOL_SIZE = 900  # ids 100..999, same range you already use
_customer_ids = list(range(100, 100 + CUSTOMER_POOL_SIZE))

_customer_tiers = [
    {"ids": list(range(100, 145)),  "share": 0.45},  # top 5%  (45 customers)  -> 45% combined
    {"ids": list(range(145, 370)),  "share": 0.35},  # next 25% (225 customers) -> 35% combined
    {"ids": list(range(370, 1000)), "share": 0.20},  # remaining 70% (630 customers) -> 20% combined
]

_customer_weight_map = _build_tiered_weights(_customer_tiers, exponent=0.5)
_customer_weights = np.array([_customer_weight_map[cid] for cid in _customer_ids])
_customer_weights = _customer_weights / _customer_weights.sum()


def pick_customer_id():
    return int(np.random.choice(_customer_ids, p=_customer_weights))


# ---------------------------------------------------------------------------
# 4. MESSIER STATUS LOGIC
#    Replaces the clean if/elif days_ago ladder with realistic exceptions:
#    cancellations, returns, payment failures, stuck pending orders, and
#    instant fulfillment for some recent orders.
# ---------------------------------------------------------------------------
def generate_status(days_ago):
    roll = random.random()

    if days_ago > 5:
        if roll < 0.03:
            return 'Cancelled'
        elif roll < 0.06:
            return 'Returned'
        elif roll < 0.08:
            return 'Pending'          # stuck order: should've shipped by now
        else:
            return 'Completed'

    elif days_ago > 2:
        if roll < 0.02:
            return 'Cancelled'
        elif roll < 0.04:
            return 'Payment Failed'
        else:
            return 'Shipped'

    else:
        if roll < 0.15:
            return 'Completed'        # instant fulfillment (digital/expedited)
        elif roll < 0.18:
            return 'Payment Failed'
        else:
            return 'Pending'