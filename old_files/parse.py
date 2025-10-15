import json
import pandas as pd
import argparse

def to_int(x):
    """Convert to int if possible, else None."""
    try:
        if x is None:
            return None
        s = str(x)
        if s in {"-555555555", "-888888888"}:
            return None
        xf = float(s)
        return int(xf)
    except:
        return None


def load_data(path):
    """Load the JSON-like data from a text file."""
    with open(path, "r", encoding="utf-8") as f:
        text = f.read().strip()

    # Fix possible formatting issues if double brackets not parsed
    if not text.startswith("[["):
        raise ValueError("Expected a list-of-lists JSON array (starts with [[ ... ]] )")

    data = json.loads(text)
    headers = data[0]
    row = data[1]
    return dict(zip(headers, row))


def build_tables(data_map):
    """Build readable DataFrames for CSV export."""
    name = data_map.get("NAME")
    total_pop = to_int(data_map.get("S0101_C01_001E"))
    male_pop = to_int(data_map.get("S0101_C03_001E"))
    female_pop = to_int(data_map.get("S0101_C05_001E"))

    # 5-year age bins (ACS standard)
    age_labels = [
        "Under 5", "5 to 9", "10 to 14", "15 to 19",
        "20 to 24", "25 to 29", "30 to 34", "35 to 39",
        "40 to 44", "45 to 49", "50 to 54", "55 to 59",
        "60 to 64", "65 to 69", "70 to 74", "75 to 79",
        "80 to 84", "85 and over"
    ]

    total_counts, male_counts, female_counts = [], [], []

    for i in range(2, 20):
        total_counts.append(to_int(data_map.get(f"S0101_C01_{i:03d}E")))
        male_counts.append(to_int(data_map.get(f"S0101_C03_{i:03d}E")))
        female_counts.append(to_int(data_map.get(f"S0101_C05_{i:03d}E")))

    df = pd.DataFrame({
        "Age group": age_labels,
        "Total": total_counts,
        "Male": male_counts,
        "Female": female_counts,
    })

    # Add % of total
    if total_pop and total_pop > 0:
        df["% of total"] = [
            round(v / total_pop * 100, 2) if v is not None else None
            for v in df["Total"]
        ]

    # Summary info
    sex_ratio = ((male_pop / female_pop) * 100) if (male_pop and female_pop) else None
    summary = {
        "Geography": name,
        "Total population": total_pop,
        "Male population": male_pop,
        "Female population": female_pop,
        "Sex ratio (males per 100 females)": round(sex_ratio, 1) if sex_ratio else None,
    }

    return df, summary


def main(input_path, output_csv):
    data_map = load_data(input_path)
    age_df, summary = build_tables(data_map)

    # Append summary rows at the bottom
    summary_rows = pd.DataFrame(list(summary.items()), columns=["Metric", "Value"])
    summary_path = output_csv.replace(".csv", "_summary.csv")

    age_df.to_csv(output_csv, index=False)
    summary_rows.to_csv(summary_path, index=False)

    print(f"✅ Data saved to:\n - {output_csv}\n - {summary_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Parse ACS S0101 JSON text file into readable CSV")
    parser.add_argument("input", help="Path to input text file")
    parser.add_argument("output", help="Path to output CSV file")
    args = parser.parse_args()

    main(args.input, args.output)
