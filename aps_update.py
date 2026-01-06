from __future__ import annotations

import csv
import json
import datetime as dt

INPUT_CSV  = "transactions.csv"
OUTPUT_CSV = "koinly_aps_transactions_priced.csv"
PRICE_JSON = "aps_price.json"

APS_TOKEN_CODE = "APS;16884676"


def load_local_price_map(json_path: str) -> dict[str, float]:
    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    price_map = {}

    for ts_ms, price in data.get("stats", []):
        ts = int(ts_ms) / 1000.0
        d = dt.datetime.utcfromtimestamp(ts).date()
        price_map[d.isoformat()] = float(price)

    print(f"Loaded {len(price_map)} APS price points from JSON")
    return price_map


price_map = load_local_price_map(PRICE_JSON)


def parse_datetime_utc(s: str) -> dt.datetime | None:
    s = s.strip()
    if not s:
        return None

    try:
        return dt.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")
    except ValueError:
        print(f"WARNING: could not parse date: {s}")
        return None


updated_rows = 0
skipped_rows = 0

with open(INPUT_CSV, newline="", encoding="utf-8") as infile, \
     open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as outfile:

    reader = csv.DictReader(infile)
    fieldnames = reader.fieldnames

    if "Net Worth Amount" not in fieldnames:
        raise RuntimeError("Expected 'Net Worth Amount' column in CSV")

    writer = csv.DictWriter(outfile, fieldnames=fieldnames)
    writer.writeheader()

    for row in reader:
        to_currency = row.get("To Currency", "")
        net_raw = row.get("Net Worth Amount", "").strip()
        to_amount_raw = row.get("To Amount", "").strip()
        date_raw = row.get("Date (UTC)", "").strip()

        is_aps = (to_currency == APS_TOKEN_CODE)

        try:
            current_net = float(net_raw) if net_raw else 0.0
        except ValueError:
            current_net = 0.0

        if is_aps and current_net == 0.0 and to_amount_raw:
            dt_obj = parse_datetime_utc(date_raw)

            if dt_obj is None:
                skipped_rows += 1
            else:
                date_key = dt_obj.date().isoformat()
                price = price_map.get(date_key)

                if price is None:
                    print(f"NO LOCAL PRICE FOR {date_key} — skipping")
                    skipped_rows += 1
                else:
                    try:
                        amt = float(to_amount_raw)
                        new_value = amt * price

                        row["Net Worth Amount"] = f"{new_value:.8f}"
                        row["Net Worth Currency"] = "USD;10"

                        updated_rows += 1
                    except ValueError:
                        skipped_rows += 1

        writer.writerow(row)

print("Done.")
print(f"Updated rows: {updated_rows}")
print(f"Skipped rows: {skipped_rows}")
print(f"Wrote -> {OUTPUT_CSV}")