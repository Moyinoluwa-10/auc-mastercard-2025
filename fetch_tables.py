#!/usr/bin/env python3
import argparse
import csv
import json
import re
import sys
import time
from typing import Dict, List, Tuple, Any, Optional, Callable
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
    Works for any Subject table ID like 'S1501', 'S0101' by regexing its variable names.
    """
    pat = re.compile(rf'^({re.escape(table)})_C(\d{{2}})_(\d{{3}})([EM])$')
    fieldmap: Dict[Tuple[str, str, str], int] = {}
    for idx, h in enumerate(headers):
        m = pat.match(h or "")
        if m:
            _, group, line_no, kind = m.groups()
            fieldmap[(group, line_no, kind)] = idx
    return fieldmap

def row_ids(headers: List[str], row: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """Extract GEO_ID and NAME if present."""
    def safe_idx(colname: str) -> Optional[int]:
        try:
            return headers.index(colname)
        except ValueError:
            return None
    gi = safe_idx("GEO_ID")
    ni = safe_idx("NAME")
    return (row[gi] if gi is not None else None,
            row[ni] if ni is not None else None)

def load_label_map_for_table(table: str) -> Dict[str, str]:
    """
    Load labels from a CSV named '<TABLE>.csv' (e.g., S0101.csv) in the
    current directory with headers: line_no,label
    Uses utf-8-sig per request.
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
        pass  # labels optional
    return mapping

def fetch_group_subject(year: str, product: str, table: str, tract: str,
                        session: requests.Session, retries: int = 3, backoff: float = 0.8) -> List[List[str]]:
    """
    Call the Census API Subject dataset group endpoint:
      /data/{year}/acs/{product}/subject?get=group({table})&ucgid=1400000US{tract}
    Returns list-of-lists (first row is headers).
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
            time.sleep(backoff * (attempt + 1))
    raise RuntimeError(f"Failed to fetch {table} for tract {tract}: {last_err}")

# -------------------------
# Default schema (most S-tables)
# -------------------------

DEFAULT_FIELDNAMES = [
    "GEO_ID","NAME","line_no","label",
    "total","total_moe","total_pct","total_pct_moe",
    "male","male_moe","male_pct","male_pct_moe",
    "female","female_moe","female_pct","female_pct_moe",
]

def default_builder(geo_id: Optional[str], name: Optional[str],
                    ln: str,
                    getv: Callable[[str, str, str], Optional[float]]) -> Dict[str, Any]:
    """
    Standard convention:
      C01=Total (count), C02=Total %, C03=Male (count), C04=Male %,
      C05=Female (count), C06=Female %
    """
    return {
        "GEO_ID": geo_id,
        "NAME": name,
        "line_no": ln,
        # C01: total (count)
        "total":  getv("01", "E", ln),
        "total_moe":  getv("01", "M", ln),
        # C02: total (%)
        "total_pct":  getv("02", "E", ln),
        "total_pct_moe":  getv("02", "M", ln),
        # C03: male (count)
        "male":   getv("03", "E", ln),
        "male_moe":   getv("03", "M", ln),
        # C04: male (%)
        "male_pct":   getv("04", "E", ln),
        "male_pct_moe":   getv("04", "M", ln),
        # C05: female (count)
        "female": getv("05", "E", ln),
        "female_moe": getv("05", "M", ln),
        # C06: female (%)
        "female_pct": getv("06", "E", ln),
        "female_pct_moe": getv("06", "M", ln),
    }

# -------------------------
# Custom schemas registry
# -------------------------
# Add new tables here by copying the S1701 example and editing.
# Each entry supplies:
#   - "fieldnames": list[str]
#   - "builder":    callable(geo_id, name, ln, getv) -> dict
# Where:
#   getv(group, kind, ln) returns numeric or None for:
#     group = '01'.., kind = 'E' or 'M', ln = '001'.. (line number)
# -------------------------

TABLE_SCHEMAS: Dict[str, Dict[str, Any]] = {
    # Example: S1701 Poverty Status — per your spec
    "S1701": {
        "fieldnames": [
            "GEO_ID","NAME","line_no","label",
            "total","total_moe",
            "below_pl","below_pl_moe",
            "below_pl_pct","below_pl_pct_moe",
        ],
        "builder": lambda geo_id, name, ln, getv: {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            # C01: total (count)
            "total":  getv("01", "E", ln),
            "total_moe": getv("01", "M", ln),
            # C02: below poverty level (count)
            "below_pl": getv("02", "E", ln),
            "below_pl_moe": getv("02", "M", ln),
            # C03: below poverty level (%)
            "below_pl_pct": getv("03", "E", ln),
            "below_pl_pct_moe": getv("03", "M", ln),
        }
    },
    "S1901": {
        "fieldnames": [
            "GEO_ID","NAME","line_no","label",
            "households","households_moe",
            "families","families_moe",
            "mc_families","mc_families_moe",
            "nf_households","nf_households_moe",
        ],
        "builder": lambda geo_id, name, ln, getv: {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            # C01: households
            "households":  getv("01", "E", ln),
            "households_moe": getv("01", "M", ln),
            # C02: families 
            "families": getv("02", "E", ln),
            "families_moe": getv("02", "M", ln),
            # C03: married-couple families
            "mc_families": getv("03", "E", ln),
            "mc_families_moe": getv("03", "M", ln),
            # C04: non-family households
            "nf_households": getv("04", "E", ln),
            "nf_households_moe": getv("04", "M", ln),
        }
    },
    "S2301": {
        "fieldnames": [
            "GEO_ID","NAME","line_no","label",
            "total","total_moe",
            "lfp_rate","lfp_rate_moe",
            "ep_ratio","ep_ratio_moe",
            "unemployment_rate","unemployment_rate_moe",
        ],
        "builder": lambda geo_id, name, ln, getv: {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            # C01: total (count)
            "total":  getv("01", "E", ln),
            "total_moe": getv("01", "M", ln),
            # C02: labor force participation rate (%)
            "lfp_rate": getv("02", "E", ln),
            "lfp_rate_moe": getv("02", "M", ln),
            # C03: employment/population ratio (%)
            "ep_ratio": getv("03", "E", ln),
            "ep_ratio_moe": getv("03", "M", ln),
            # C04: unemployment rate (%)
            "unemployment_rate": getv("04", "E", ln),
            "unemployment_rate_moe": getv("04", "M", ln),
        }
    },
    "S2502": {
        "fieldnames": [
            "GEO_ID","NAME","line_no","label",
            "oc_units","oc_units_moe",
            "oc_units_pct","oc_units_pct_moe",
            
            "ooh_units","ooh_units_moe",
            "ooh_units_pct","ooh_units_pct_moe",
            "roh_units","roh_units_moe",
            "roh_units_pct","roh_units_pct_moe",
        ],
        "builder": lambda geo_id, name, ln, getv: {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            # C01: occupied units (count)
            "oc_units":  getv("01", "E", ln),
            "oc_units_moe": getv("01", "M", ln),
            # C02: occupied units (%)
            "oc_units_pct": getv("02", "E", ln),
            "oc_units_pct_moe": getv("02", "M", ln),
            # C03: owner-occupied housing units (count)
            "ooh_units": getv("03", "E", ln),
            "ooh_units_moe": getv("03", "M", ln),
            # C04: owner-occupied housing units (%)
            "ooh_units_pct": getv("04", "E", ln),
            "ooh_units_pct_moe": getv("04", "M", ln),
            # C05: renter-occupied housing units (count)
            "roh_units": getv("05", "E", ln),
            "roh_units_moe": getv("05", "M", ln),
            # C06: renter-occupied housing units (%)
            "roh_units_pct": getv("06", "E", ln),
            "roh_units_pct_moe": getv("06", "M", ln),
        }
    },
    "S2701": {
        "fieldnames": [
            "GEO_ID","NAME","line_no","label","total","total_moe",
            "insured","insured_moe","insured_pct","insured_pct_moe",
            "uninsured","uninsured_moe","uninsured_pct","uninsured_pct_moe",
        ],
        "builder": lambda geo_id, name, ln, getv: {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            # C01: total (count)
            "total":  getv("01", "E", ln),
            "total_moe": getv("01", "M", ln),
            # C02: insured (count)
            "insured": getv("02","E",ln),            # <- adjust groups per the shell
            "insured_moe": getv("02","M",ln),
            # C03: insured (%)
            "insured_pct": getv("03","E",ln),
            "insured_pct_moe": getv("03","M",ln),
            # C04: uninsured (count)
            "uninsured": getv("04","E",ln),
            "uninsured_moe": getv("04","M",ln),
            # C05: uninsured (%)
            "uninsured_pct": getv("05","E",ln),
            "uninsured_pct_moe": getv("05","M",ln),
        }
    },
     "S2801": {
        "fieldnames": [
            "GEO_ID","NAME","line_no","label",
            "total","total_moe",
            "total_pct","total_pct_moe",
        ],
        "builder": lambda geo_id, name, ln, getv: {
            "GEO_ID": geo_id,
            "NAME": name,
            "line_no": ln,
            # C01: total (count)
            "total":  getv("01", "E", ln),
            "total_moe": getv("01", "M", ln),
            # C02: total (%)
            "total_pct": getv("02", "E", ln),
            "total_pct_moe": getv("02", "M", ln),
        }
    },
    # You can add more custom tables here, e.g.:
    # "S2701": { "fieldnames": [...], "builder": lambda geo_id, name, ln, getv: {...} },
}

# -------------------------
# Record assembly
# -------------------------

def build_records_for_row(table: str, headers: List[str], row: List[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """
    Build tidy records for one API row using either a custom table schema
    or the default S-table convention. Returns (records, fieldnames).
    """
    geo_id, name = row_ids(headers, row)
    fieldmap = parse_headers_for_table(table, headers)

    # discover present line_nos
    line_nos = sorted({ln for (_, ln, _) in fieldmap.keys()})

    def getv(group: str, kind: str, ln: str) -> Optional[float]:
        col = fieldmap.get((group, ln, kind))
        return clean_num(row[col]) if col is not None else None

    # pick schema
    schema = TABLE_SCHEMAS.get(table)
    if schema:
        fieldnames = schema["fieldnames"]
        builder = schema["builder"]
    else:
        fieldnames = DEFAULT_FIELDNAMES
        builder = default_builder

    records: List[Dict[str, Any]] = []
    for ln in line_nos:
        rec = builder(geo_id, name, ln, getv)
        # keep rows with at least one meaningful value
        keep = any(
            (k in rec) and (rec[k] is not None)
            for k in rec.keys()
            if k not in ("GEO_ID", "NAME", "line_no", "label")
        )
        if keep:
            records.append(rec)

    return records, fieldnames

def write_tidy_csv(path: str, records: List[Dict[str, Any]], fieldnames: List[str]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for rec in records:
            w.writerow({k: rec.get(k) for k in fieldnames})

# -------------------------
# Main
# -------------------------

def main():
    ap = argparse.ArgumentParser(
        description="Fetch ACS Subject tables from Census API for multiple tracts and tables, with pluggable per-table schemas; merge labels; write tidy CSVs."
    )
    ap.add_argument("--year", default="2019", help="ACS year (e.g., 2019, 2023). Default: 2019")
    ap.add_argument("--product", default="acs5", help="ACS product (acs1, acs5). Default: acs5")
    ap.add_argument("--tables", nargs="+", required=True, help="Subject table IDs (e.g., S0101 S1501 S1701)")
    ap.add_argument("--tracts", nargs="+", required=True, help="11-digit tract geoid(s), e.g., 48021950801")
    ap.add_argument("--api-key", default=None, help="Optional Census API key")
    args = ap.parse_args()

    sess = requests.Session()
    if args.api_key:
        # (You can also pass the key as a URL param; this header is fine for ID)
        sess.headers.update({"User-Agent": "ACSFetcher/1.1 (+API key provided)"})

    for table in args.tables:
        label_map = load_label_map_for_table(table)

        for tract in args.tracts:
            data = fetch_group_subject(args.year, args.product, table, tract, sess)
            headers = data[0]
            rows = data[1:]

            all_records: List[Dict[str, Any]] = []
            out_fieldnames: Optional[List[str]] = None

            for r in rows:
                recs, fieldnames = build_records_for_row(table, headers, r)
                # attach labels
                for rec in recs:
                    ln = (rec.get("line_no") or "").zfill(3)
                    rec["label"] = label_map.get(ln, "")
                all_records.extend(recs)
                if out_fieldnames is None:
                    out_fieldnames = fieldnames

            # Fallback if no rows
            if out_fieldnames is None:
                out_fieldnames = TABLE_SCHEMAS.get(table, {}).get("fieldnames", DEFAULT_FIELDNAMES)

            all_records.sort(key=lambda d: (d.get("GEO_ID") or "", d.get("line_no") or ""))

            out_path = f"output/{tract}_{table}.csv"
            write_tidy_csv(out_path, all_records, out_fieldnames)
            print(f"✅ {table} for tract {tract} -> {out_path} ({len(all_records)} rows)")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
