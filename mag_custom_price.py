from __future__ import annotations

import json
import csv
import datetime as dt

# ==============================
# CONFIG – adjust filenames if needed
# ==============================

MAG_JSON  = "mag_price.json"     # MAG/XRP OHLC list
XRP_JSON  = "xrp_price.json"     # XRP/USD {success, data: [{x, y}, ...]}
OUTPUT_CSV = "mag_usd_custom_prices.csv"  # Koinly custom price file

# Time to use in the "Date" column (Koinly sample uses a time component)
TIME_SUFFIX = "12:00:00"  # you can change this if you like

# ==============================
# HELPERS
# ==============================

def load_mag_xrp_daily_map(json_path: str) -> dict[str, float]:
    """
    Load MAG/XRP prices from mag_price.json.

    Expected format:
    [
      {
        "open": 892.3487,
        "close": 902.083,
        "timestamp": "2026-01-01T00:00:00.000Z",
        ...
      },
      ...
    ]

    We compute the daily price as (open + close) / 2.

    Returns: { "YYYY-MM-DD": mag_per_xrp_price }
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, list):
        raise ValueError(f"{json_path} is expected to be a list of OHLC entries")

    price_map: dict[str, float] = {}

    for entry in data:
        ts = entry.get("timestamp")
        o  = entry.get("open")
        c  = entry.get("close")

        if ts is None or o is None or c is None:
            continue

        try:
            ts_str = str(ts).strip()
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1]
            dt_obj = dt.datetime.fromisoformat(ts_str)
            d = dt_obj.date().isoformat()

            avg_price = (float(o) + float(c)) / 2.0  # MAG per XRP
            price_map[d] = avg_price
        except Exception as e:
            print(f"[MAG] WARNING parsing entry {entry}: {e}")
            continue

    print(f"[MAG] Loaded {len(price_map)} MAG/XRP daily prices from {json_path}")
    return price_map


def load_xrp_usd_daily_map(json_path: str) -> dict[str, float]:
    """
    Load XRP/USD prices from xrp_price.json.

    Expected format:
    {
      "success": true,
      "data": [
        { "x": "2013-08-03T21:00:00Z", "y": "0.0059" },
        { "x": "2013-08-04T21:00:00Z", "y": "0.0058" },
        ...
      ]
    }

    Returns: { "YYYY-MM-DD": xrp_usd_price }
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    if not isinstance(data, dict) or "data" not in data:
        raise ValueError(f"{json_path} is expected to be a dict with a 'data' key")

    price_map: dict[str, float] = {}

    for entry in data.get("data", []):
        ts = entry.get("x")
        y  = entry.get("y")
        if ts is None or y is None:
            continue

        try:
            ts_str = str(ts).strip()
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1]
            dt_obj = dt.datetime.fromisoformat(ts_str)
            d = dt_obj.date().isoformat()

            price_map[d] = float(y)  # XRP in USD
        except Exception as e:
            print(f"[XRP] WARNING parsing entry {entry}: {e}")
            continue

    print(f"[XRP] Loaded {len(price_map)} XRP/USD daily prices from {json_path}")
    return price_map


# ==============================
# MAIN: BUILD MAG/USD AND WRITE CSV
# ==============================

mag_xrp_map = load_mag_xrp_daily_map(MAG_JSON)
xrp_usd_map = load_xrp_usd_daily_map(XRP_JSON)

# Intersection of dates where we have both MAG/XRP and XRP/USD
common_dates = sorted(set(mag_xrp_map.keys()) & set(xrp_usd_map.keys()))

print(f"Common dates with both MAG/XRP and XRP/USD: {len(common_dates)}")

with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    # Match your sample: Date,Rate
    writer.writerow(["Date", "Rate"])

    for d in common_dates:
        mag_xrp = mag_xrp_map[d]   # MAG per XRP
        xrp_usd = xrp_usd_map[d]   # XRP per USD

        # MAG/USD = (MAG/XRP) * (XRP/USD)
        mag_usd = mag_xrp * xrp_usd

        # Use "YYYY-MM-DD HH:MM:SS" format like the sample file
        date_str = f"{d} {TIME_SUFFIX}"
        writer.writerow([date_str, f"{mag_usd:.12f}"])

print(f"Done. Wrote MAG/USD custom prices to: {OUTPUT_CSV}")