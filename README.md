# AUC Mastercard 2025

## Setup

### Clone Repository
```bash
git clone <repository-url>
```

### Create a virtual environment
```
python -m venv venv
source venv/bin/activate # On macOS/Linux
venv\Scripts\activate  # On Windows (cmd/powershell). Use `source venv\Scripts\activate` if using bash terminal on Windows
```

### Install Python dependencies
```
pip install -r requirements.txt
```

### NB: If any additional packages are installed, update requirements.txt using
```
pip freeze > requirements.txt
```

### Run code
Options for `fetch_tables.py`:
- `--year`: Year of the ACS data (e.g., 2019, 2020, 2021, 2022) Default is `2019`
- `--product`: ACS product type (e.g., acs1, acs3, acs5). Default is `acs5`
- `--tables`: List of ACS table IDs to fetch (e.g., S0101, S1501)
- `--tracts`: List of census tract IDs to fetch data for (e.g., 48021950801, 28047003206)
  
```
python fetch_tables.py --year 2019 --product acs5 --tables S0101 S1501 S1701 S1901 S2301 S2502 S2701 S2801 --tracts 48021950801 28047003206 28047003301
```

NB: You have to add additional table labels to the label folder if you are fetching new tables. The file format is CSV and the headers should be `line_no,label`. Also, you have to update the `TABLE_SCHEMAS` dictionary in `fetch_tables.py`.