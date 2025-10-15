#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sys
import time
from typing import Dict, List, Tuple, Any, Optional

import requests

API_BASE = "https://api.census.gov/data"

SUPPRESS_CODES = {
    "-555555555", "-555555555.0",
    "-888888888", "-888888888.0",
    "-666666666", "-666666666.0",
    "-222222222", "-222222222.0",
}
ANNOTATION_TOKENS = {"", "(X)", "*****", "-", "**"}

def clean_num(x: Any) -> Optional[float]:
    """Return float/int from string, or None for suppression/NA/annotations."""
    if x is None:
        return None
    s = str(x).strip()
    if s in SUPPRESS_CODES or s in ANNOTATION_TOKENS:
        return None
    try:
        v = float(s)
        return int(v) if abs(v - int(v)) < 1e-9 else v
    except Exception:
        return None

def parse_headers_for_table(table: str, headers: List[str]) -> Dict[Tuple[str, str, str], int]:
    """
    Map (group '01'.., line '001'.., kind 'E'/'M') -> column index
    Works for any Subject table id like 'S1501', 'S0101' by regexing its variable names.
    """
    pat = re.compile(rf'^({re.escape(table)})_C(\d{{2}})_(\d{{3}})([EM])$')
    fieldmap: Dict[Tuple[str, str, str], int] = {}
    for idx, h in enumerate(headers):
        m = pat.match(h or "")
        if m:
            _, group, line_no, kind = m.groups()
            fieldmap[(group, line_no, kind)] = idx
    return fieldmap

def row_to_records(table: str, headers: List[str], row: List[str]) -> List[Dict[str, Any]]:
    """
    Turn a single api row into tidy records (one per line_no), using the
    standard Subject-table convention:
      C01=Total, C02=Total %, C03=Male, C04=Male %, C05=Female, C06=Female %
    """
    # Core IDs
    def safe_idx(colname: str) -> Optional[int]:
        try:
            return headers.index(colname)
        except ValueError:
            return None

    geo_idx = safe_idx("GEO_ID")
    name_idx = safe_idx("NAME")
    geo_id = row[geo_idx] if geo_idx is not None else None
    name = row[name_idx] if name_idx is not None else None

    fieldmap = parse_headers_for_table(table, headers)
    line_nos = sorted({ln for (_, ln, _) in fieldmap.keys()})

    def getv(group: str, kind: str, ln: str) -> Optional[float]:
        col = fieldmap.get((group, ln, kind))
        return clean_num(row[col]) if col is not None else None

    out: List[Dict[str, Any]] = []
    for ln in line_nos:
        rec = {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            # counts
            "total":  getv("01", "E", ln),
            "male":   getv("03", "E", ln),
            "female": getv("05", "E", ln),
            # percentages
            "total_pct":  getv("02", "E", ln),
            "male_pct":   getv("04", "E", ln),
            "female_pct": getv("06", "E", ln),
            # MOEs (counts only; add percent MOEs if needed)
            "total_moe":  getv("01", "M", ln),
            "male_moe":   getv("03", "M", ln),
            "female_moe": getv("05", "M", ln),
        }
        rec = {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            "total":  getv("01", "E", ln),
            "below_pl":   getv("02", "E", ln),
            "below_pl_pct": getv("03", "E", ln),
            "total_moe":  getv("01", "M", ln),
            "below_pl_moe":   getv("02", "M", ln),
            "below_pl_pct_moe": getv("03", "M", ln),
        }
        if any(rec[k] is not None for k in ("total","male","female","total_pct","male_pct","female_pct")):
            out.append(rec)

    return out

def load_label_map_for_table(table: str) -> Dict[str, str]:
    """
    Load labels from a CSV named '<TABLE>.csv' (e.g., S0101.csv) in the
    current directory with headers: line_no,label
    Uses utf-8-sig as requested.
    """
    mapping: Dict[str, str] = {}
    path = f"labels/{table}.csv"
    try:
        with open(path, newline="", encoding="utf-8-sig") as f:
            r = csv.DictReader(f)
            for row in r:
                ln = (row.get("line_no") or "").strip()
                if not ln:
                    continue
                ln = ln.zfill(3)
                label = (row.get("label") or "").strip()
                mapping[ln] = label
    except FileNotFoundError:
        # No labels file; labels will be blank.
        pass
    return mapping

def fetch_group_subject(year: str, product: str, table: str, tract: str, session: requests.Session, retries: int = 3, backoff: float = 0.8) -> List[List[str]]:
    """
    Call the Census API Subject dataset group endpoint:
      /data/{year}/acs/{product}/subject?get=group({table})&ucgid=1400000US{tract}
    Returns list-of-lists (rows), where first row is headers.
    """
    params = {
        "get": f"group({table})",
        "ucgid": f"1400000US{tract}",
    }
    url = f"{API_BASE}/{year}/acs/{product}/subject"
    last_err = None
    for attempt in range(retries):
        try:
            resp = session.get(url, params=params, timeout=60)
            resp.raise_for_status()
            data = resp.json()
            if not isinstance(data, list) or not data or not isinstance(data[0], list):
                raise ValueError("Unexpected API response format.")
            return data
        except Exception as e:
            last_err = e
            # basic backoff for throttling / transient issues
            time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {table} for tract {tract}: {last_err}")

def write_tidy_csv(path: str, records: List[Dict[str, Any]]) -> None:
    fieldnames = [
        "GEO_ID","NAME","line_no","label",
        "total","total_moe","total_pct",
        "male","male_moe","male_pct",
        "female","female_moe","female_pct",
    ]
    fieldnames = [
        "GEO_ID","NAME","line_no","label",
        "total","total_moe",
        "below_pl","below_pl_moe",
        "below_pl_pct","below_pl_moe",
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for rec in records:
            w.writerow({k: rec.get(k) for k in fieldnames})

def main():
    ap = argparse.ArgumentParser(
        description="Fetch ACS Subject tables from Census API for multiple tracts and tables, merge labels, and write tidy CSVs."
    )
    ap.add_argument("--year", default="2019", help="ACS year (e.g., 2019, 2023). Default: 2019")
    ap.add_argument("--product", default="acs5", help="ACS product (acs1, acs5). Default: acs5")
    ap.add_argument("--tables", nargs="+", required=True, help="Subject table IDs (e.g., S0101 S1501 S1701)")
    ap.add_argument("--tracts", nargs="+", required=True, help="Census tract GEOID 11-digit (no prefix), e.g., 48021950801")
    ap.add_argument("--api-key", default=None, help="Optional Census API key (recommended for higher rate limits)")
    args = ap.parse_args()

    # Use a session for connection reuse
    sess = requests.Session()
    if args.api_key:
        sess.headers.update({"User-Agent": "ACSFetcher/1.0 (+api key)"})

    # Process every (table, tract)
    for table in args.tables:
        label_map = load_label_map_for_table(table)
        for tract in args.tracts:
            # fetch
            data = fetch_group_subject(args.year, args.product, table, tract, sess)
            headers = data[0]
            rows = data[1:]

            # Build tidy records from each row & add labels
            all_records: List[Dict[str, Any]] = []
            for r in rows:
                recs = row_to_records(table, headers, r)
                for rec in recs:
                    ln = (rec.get("line_no") or "").zfill(3)
                    rec["label"] = label_map.get(ln, "")
                all_records.extend(recs)

            # sort & write
            all_records.sort(key=lambda d: (d.get("GEO_ID") or "", d.get("line_no") or ""))
            out_path = f"output/{tract}_{table}.csv"
            write_tidy_csv(out_path, all_records)
            print(f"✅ {table} for tract {tract} -> {out_path} ({len(all_records)} rows)")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
