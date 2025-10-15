#!/usr/bin/env python3
import json
import re
import argparse
import csv

SUPPRESS_CODES = {"-555555555", "-555555555.0", "-888888888", "-888888888.0",
                  "-666666666", "-666666666.0", "-222222222", "-222222222.0"}

def clean_num(x):
    """Return float (or int) from string, or None for suppression/NA."""
    if x is None:
        return None
    s = str(x).strip()
    if s in SUPPRESS_CODES:
        return None
    # Also ignore empty strings or annotation placeholders like '(X)', '*****', '-', '**'
    if s in {"", "(X)", "*****", "-", "**"}:
        return None
    try:
        v = float(s)
        # return int if it looks integral
        return int(v) if abs(v - int(v)) < 1e-9 else v
    except:
        return None

def parse_headers(headers):
    """
    Find all S1501 fields and index them by (group_code, line_no, measure_kind)
    group_code: 'C01'..'C06'
    line_no: '001'.. etc
    measure_kind: 'E' (estimate) or 'M' (margin of error)
    """
    fieldmap = {}  # key -> column index
    pat = re.compile(r'^(S1501)_C(\d{2})_(\d{3})([EM])$')
    for idx, h in enumerate(headers):
        m = pat.match(h or "")
        if m:
            _, group, line_no, kind = m.groups()
            fieldmap[(group, line_no, kind)] = idx
    return fieldmap

def row_to_records(headers, row):
    """
    Turn a single data row into list of records (one per line_no).
    Returns [ {geo_id, name, line_no, total, total_pct, male, male_pct, female, female_pct, ...}, ... ]
    """
    # Core IDs
    try:
        geo_id = row[headers.index("GEO_ID")]
    except ValueError:
        geo_id = None
    try:
        name = row[headers.index("NAME")]
    except ValueError:
        name = None

    fieldmap = parse_headers(headers)

    # Collect all line numbers seen
    line_nos = sorted({line for (_, line, _) in fieldmap.keys()})

    records = []
    for line in line_nos:
        # helper to get a value if present
        def getv(group, kind):
            col = fieldmap.get((group, line, kind))
            return clean_num(row[col]) if col is not None else None

        rec = {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": line,
            # counts
            "total":  getv("01", "E"),
            "male":   getv("03", "E"),
            "female": getv("05", "E"),
            # percents
            "total_pct":  getv("02", "E"),
            "male_pct":   getv("04", "E"),
            "female_pct": getv("06", "E"),
            # MOEs (counts only; percent MOEs are usually present too but less commonly used)
            "total_moe":  getv("01", "M"),
            "male_moe":   getv("03", "M"),
            "female_moe": getv("05", "M"),
        }
        # Skip completely empty lines (no data in any field)
        if any(rec[k] is not None for k in ("total","male","female","total_pct","male_pct","female_pct")):
            records.append(rec)

    return records

def main():
    ap = argparse.ArgumentParser(description="Parse ACS S1501 Educational Attainment (list-of-lists JSON) to a tidy CSV")
    ap.add_argument("input_txt", help="Path to the text file containing [[headers],[row1],...[rowN]] JSON")
    ap.add_argument("output_csv", help="Path for the output CSV")
    args = ap.parse_args()

    with open(args.input_txt, "r", encoding="utf-8") as f:
        data = json.loads(f.read())

    headers = data[0]
    rows = data[1:]

    all_records = []
    for r in rows:
        all_records.extend(row_to_records(headers, r))

    # Sort nicely
    all_records.sort(key=lambda d: (d.get("GEO_ID") or "", d.get("line_no") or ""))

    # Write CSV
    fieldnames = [
        "GEO_ID","NAME","line_no",
        "total","total_moe","total_pct",
        "male","male_moe","male_pct",
        "female","female_moe","female_pct",
    ]
    with open(args.output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for rec in all_records:
            w.writerow(rec)

    print(f"✅ Wrote {len(all_records)} records to {args.output_csv}")

if __name__ == "__main__":
    main()
