from __future__ import annotations

import csv
import json
import datetime as dt

# ==============================
# CONFIG
# ==============================

INPUT_CSV  = "transactions-5.csv"
OUTPUT_CSV = "stx_networth_updated.csv"

# Extra config for XRP price source used in MAG/USD calc
XRP_USD_PRICE_JSON = "xrp_price.json"   # XRP/USD JSON (success/data/x/y or list of {x,y})
XRP_X_KEY = "x"
XRP_Y_KEY = "y"

# Map Koinly "To/From Currency" codes to their JSON price files + format
TOKEN_CONFIG = {
    # # APS – old "stats" format: { "stats": [[ts_ms, price], ...] }
    # "APS;16884676": {
    #     "price_file": "aps_price.json",
    #     "format": "stats",
    # },
    # # STX – new "xy" format: { "success": true, "data": [{ "x": iso_ts, "y": price }, ...] }
    # "STX;1770845": {
    #     "price_file": "stx_price.json",
    #     "format": "xy",
    # },
    # # ASC – new "xy" format
    # "ASC;7723464": {
    #     "price_file": "asc_price.json",
    #     "format": "xy",
    # },
    # # ARK – new "xy" format
    # "ARK;8047083": {
    #     "price_file": "ark_price.json",
    #     "format": "xy",
    # },
    # # BEAR – new "xy" format
    # "BEAR;23448216": {
    #     "price_file": "bear_price.json",
    #     "format": "xy",
    # },
    # # XSPECTAR – new "xy" format
    # "XSPECTAR;4794440": {
    #     "price_file": "xspectar_price.json",
    #     "format": "xy",
    # },
    # # HUGETITS – new "xy" format
    # "HUGETITS;5437660": {
    #     "price_file": "hugetits_price.json",
    #     "format": "xy",
    # },
    # MAG – special: MAG/XRP (mag_price.json) + XRP/USD (xrp_price.json) -> MAG/USD
    # BRAD – new "xy" format
    "STX;1770845": {
        "price_file": "stx_price.json",
        "format": "xy",
    },
    # "MAG;8678551": {
    #     "price_file": "mag_price.json",   # MAG/XRP OHLC list
    #     "format": "mag_xrp_usd",
    # },
}

# Stablecoins to treat 1:1 with USD (just copy amount)
STABLE_TOKENS = {
    "USDC;7483231",
    "RLUSD;30660449",
    "USDC;5377860",
}

# All net worth values we set will be in this currency:
NET_WORTH_CCY_VALUE = "USD;10"

DATE_COL             = "Date (UTC)"
TO_CCY_COL           = "To Currency"
TO_AMT_COL           = "To Amount"
FROM_CCY_COL         = "From Currency"
FROM_AMT_COL         = "From Amount"
NET_WORTH_AMT_COL    = "Net Worth Amount"
NET_WORTH_CCY_COL    = "Net Worth Currency"

# ==============================
# LOAD PRICE MAPS
# ==============================

def load_price_map_stats(json_path: str) -> dict[str, float]:
    """
    Format:
    {
      "stats": [
        [1696723200000, 131165.67206219322],
        [1696809600000, 131165.67206219322],
        ...
      ]
    }
    -> { "YYYY-MM-DD": price }
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    price_map: dict[str, float] = {}

    for ts_ms, price in data.get("stats", []):
        ts_sec = int(ts_ms) / 1000.0
        d = dt.datetime.utcfromtimestamp(ts_sec).date()
        price_map[d.isoformat()] = float(price)

    print(f"[stats] Loaded {len(price_map)} prices from {json_path}")
    return price_map


def load_price_map_xy(json_path: str) -> dict[str, float]:
    """
    Format:
    {
      "success": true,
      "data": [
        { "x": "2022-03-20T22:00:00Z", "y": 0.0035164 },
        { "x": "2022-03-21T22:00:00Z", "y": 0.0111199 },
        ...
      ]
    }
    OR:
    [
        { "x": "...", "y": ... },
        ...
    ]

    -> { "YYYY-MM-DD": price }
    """
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Support both: dict with "data" or top-level list
    if isinstance(data, dict):
        entries = data.get("data", [])
    elif isinstance(data, list):
        entries = data
    else:
        raise ValueError(f"Unexpected JSON structure in {json_path}")

    price_map: dict[str, float] = {}

    for entry in entries:
        ts = entry.get("x")
        price = entry.get("y")
        if ts is None or price is None:
            continue

        ts_str = str(ts).strip()
        try:
            # strip trailing 'Z' if present
            if ts_str.endswith("Z"):
                ts_str2 = ts_str[:-1]
                dt_obj = dt.datetime.fromisoformat(ts_str2)
            else:
                dt_obj = dt.datetime.fromisoformat(ts_str)
            d = dt_obj.date()
            price_map[d.isoformat()] = float(price)
        except Exception as e:
            print(f"[xy] WARNING: could not parse timestamp '{ts_str}' in {json_path}: {e}")
            continue

    print(f"[xy] Loaded {len(price_map)} prices from {json_path}")
    return price_map


def load_price_map_mag_xrp_usd(mag_json_path: str) -> dict[str, float]:
    """
    MAG/USD from two sources:

    1) mag_price.json (MAG/XRP), expected format:
       [
         {
           "open": 892.34,
           "close": 902.08,
           "timestamp": "2026-01-01T00:00:00.000Z",
           ...
         },
         ...
       ]
       We compute daily MAG/XRP price = (open + close) / 2

    2) XRP/USD in XRP_USD_PRICE_JSON (xrp_price.json), expected format:
       {
         "success": true,
         "data": [
           { "x": "2013-08-03T21:00:00Z", "y": "0.0059" },
           ...
         ]
       }
       or a list of {x, y}.

    Then:
       MAG/USD = (MAG/XRP) * (XRP/USD)

    Returns:
       { "YYYY-MM-DD": mag_usd_price }
    """

    # --- Load MAG/XRP daily prices from mag_price.json ---
    with open(mag_json_path, "r", encoding="utf-8") as f:
        mag_data = json.load(f)

    if not isinstance(mag_data, list):
        raise ValueError(f"{mag_json_path} is expected to be a list of OHLC entries")

    mag_xrp_map: dict[str, float] = {}
    for entry in mag_data:
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
            mag_xrp_map[d] = avg_price
        except Exception as e:
            print(f"[MAG] WARNING parsing entry {entry}: {e}")
            continue

    print(f"[mag_xrp_usd] Loaded {len(mag_xrp_map)} MAG/XRP prices from {mag_json_path}")

    # --- Load XRP/USD daily prices from xrp_price.json ---
    with open(XRP_USD_PRICE_JSON, "r", encoding="utf-8") as f:
        xrp_data = json.load(f)

    if isinstance(xrp_data, dict):
        xrp_entries = xrp_data.get("data", [])
    elif isinstance(xrp_data, list):
        xrp_entries = xrp_data
    else:
        raise ValueError(f"{XRP_USD_PRICE_JSON} has unexpected structure")

    xrp_usd_map: dict[str, float] = {}
    for entry in xrp_entries:
        ts = entry.get(XRP_X_KEY)
        y  = entry.get(XRP_Y_KEY)
        if ts is None or y is None:
            continue

        try:
            ts_str = str(ts).strip()
            if ts_str.endswith("Z"):
                ts_str = ts_str[:-1]
            dt_obj = dt.datetime.fromisoformat(ts_str)
            d = dt_obj.date().isoformat()
            xrp_usd_map[d] = float(y)
        except Exception as e:
            print(f"[XRP] WARNING parsing entry {entry}: {e}")
            continue

    print(f"[mag_xrp_usd] Loaded {len(xrp_usd_map)} XRP/USD prices from {XRP_USD_PRICE_JSON}")

    # --- Combine into MAG/USD ---
    combined: dict[str, float] = {}
    common_dates = set(mag_xrp_map.keys()) & set(xrp_usd_map.keys())

    for d in common_dates:
        mag_xrp = mag_xrp_map[d]  # MAG per XRP
        xrp_usd = xrp_usd_map[d]  # XRP in USD

        mag_usd = mag_xrp * xrp_usd
        combined[d] = mag_usd

    print(f"[mag_xrp_usd] Built MAG/USD prices for {len(combined)} common dates")
    return combined


def build_all_price_maps(config: dict) -> dict[str, dict[str, float]]:
    """
    For each token code in TOKEN_CONFIG, load its price JSON once.
    Returns: { token_code: { 'YYYY-MM-DD': price } }
    """
    all_maps: dict[str, dict[str, float]] = {}

    for token_code, cfg in config.items():
        price_file = cfg["price_file"]
        fmt        = cfg["format"]

        if fmt == "stats":
            price_map = load_price_map_stats(price_file)
        elif fmt == "xy":
            price_map = load_price_map_xy(price_file)
        elif fmt == "mag_xrp_usd":
            price_map = load_price_map_mag_xrp_usd(price_file)
        else:
            raise ValueError(f"Unknown format '{fmt}' for token {token_code}")

        all_maps[token_code] = price_map

    return all_maps


price_maps_by_token = build_all_price_maps(TOKEN_CONFIG)

# ==============================
# HELPERS
# ==============================

def parse_datetime_utc(s: str) -> dt.datetime | None:
    """
    Parse the Date (UTC) column.
    Expecting 'YYYY-MM-DD HH:MM:SS'
    """
    s = s.strip()
    if not s:
        return None
    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"WARNING: could not parse Date (UTC): '{s}'")
        return None


def safe_float(value, default=0.0) -> float:
    value = "" if value is None else str(value).strip()
    if value == "":
        return default
    try:
        return float(value)
    except ValueError:
        return default

# ==============================
# MAIN PROCESSING
# ==============================

updated_rows = 0
skipped_rows = 0

with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
     open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames

    if fieldnames is None:
        raise RuntimeError("Could not read header row from CSV")

    # Ensure needed columns exist
    required_cols = [
        DATE_COL,
        TO_CCY_COL,
        TO_AMT_COL,
        FROM_CCY_COL,
        FROM_AMT_COL,
        NET_WORTH_AMT_COL,
        NET_WORTH_CCY_COL,
    ]
    for col in required_cols:
        if col not in fieldnames:
            if col == NET_WORTH_CCY_COL:
                fieldnames.append(NET_WORTH_CCY_COL)
            else:
                raise RuntimeError(f"Missing required column '{col}' in CSV")

    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        net_raw  = row.get(NET_WORTH_AMT_COL, "")
        date_raw = row.get(DATE_COL, "")

        # Only fill if Net Worth Amount is currently 0 (or blank)
        current_net = safe_float(net_raw, default=0.0)
        if current_net != 0.0:
            writer.writerow(row)
            continue

        to_currency   = row.get(TO_CCY_COL, "")
        from_currency = row.get(FROM_CCY_COL, "")

        token_code = None
        amt_raw    = None
        mode       = None  # "price" or "stable"

        # Priority 1: tokens with JSON price maps (APS/STX/ASC/ARK/BEAR/XSPECTAR/HUGETITS/MAG)
        if to_currency in TOKEN_CONFIG:
            token_code = to_currency
            amt_raw    = row.get(TO_AMT_COL, "")
            mode       = "price"
        elif from_currency in TOKEN_CONFIG:
            token_code = from_currency
            amt_raw    = row.get(FROM_AMT_COL, "")
            mode       = "price"
        # Priority 2: stablecoins (USDC, RLUSD) -> copy amount directly
        elif to_currency in STABLE_TOKENS:
            token_code = to_currency
            amt_raw    = row.get(TO_AMT_COL, "")
            mode       = "stable"
        elif from_currency in STABLE_TOKENS:
            token_code = from_currency
            amt_raw    = row.get(FROM_AMT_COL, "")
            mode       = "stable"
        else:
            # neither side is one of our known tokens
            writer.writerow(row)
            continue

        amt = safe_float(amt_raw, default=0.0)
        if amt == 0.0:
            skipped_rows += 1
            writer.writerow(row)
            continue

        # --- Stablecoin path: just copy amount ---
        if mode == "stable":
            row[NET_WORTH_AMT_COL] = f"{amt:.8f}"
            row[NET_WORTH_CCY_COL] = NET_WORTH_CCY_VALUE
            updated_rows += 1
            writer.writerow(row)
            continue

        # --- Priced-token path: use date + price map ---
        dt_obj = parse_datetime_utc(date_raw)
        if dt_obj is None:
            skipped_rows += 1
            writer.writerow(row)
            continue

        date_key  = dt_obj.date().isoformat()
        price_map = price_maps_by_token.get(token_code, {})
        price     = price_map.get(date_key)

        if price is None:
            print(f"NO PRICE for token {token_code} on {date_key} — skipping")
            skipped_rows += 1
            writer.writerow(row)
            continue

        new_nv = amt * price
        row[NET_WORTH_AMT_COL] = f"{new_nv:.8f}"
        row[NET_WORTH_CCY_COL] = NET_WORTH_CCY_VALUE

        updated_rows += 1
        writer.writerow(row)

print("Done.")
print(f"Updated rows: {updated_rows}")
print(f"Skipped rows: {skipped_rows}")
print(f"Wrote -> {OUTPUT_CSV}")