# ETL\_Bank\_Project

Lightweight ETL pipeline that extracts the archived Wikipedia **List of largest banks** table, converts USD market-cap values into GBP, EUR, and INR using an exchange-rates CSV, and saves the output to both a CSV file and an SQLite database. Logging and an HTML table backup are included for traceability.

---

## Key Features

* Extracts bank market-cap data from an archived Wikipedia page.
* Transforms USD values into GBP, EUR, and INR using a provided exchange-rate CSV.
* Saves results to `Largest_banks_data.csv` and an SQLite database (`Banks.db`).
* Writes an HTML backup of the source table (`table_html.txt`) and a timestamped log (`code_log.txt`).
* Includes sample SQL queries to validate the loaded data.

---

## Quick facts

* **Language:** Python
* **Tested with:** Python 3.12+
* **Main dependencies:** `pandas`, `requests`, `beautifulsoup4`
* **Outputs:** `Largest_banks_data.csv`, `Banks.db`, `table_html.txt`, `code_log.txt`

---

## Files

* `etl_bank_script.py` — main ETL script (the code you provided).
* `Largest_banks_data.csv` — generated CSV of transformed data.
* `Banks.db` — SQLite database containing table `Largest_banks`.
* `table_html.txt` — prettified HTML of the source wikitable for backup/reference.
* `code_log.txt` — textual log of ETL steps with timestamps.

> Tip: add the generated files to `.gitignore` (see the suggested `.gitignore` section below).

---

## Requirements

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
# On Windows
.\.venv\Scripts\activate
# On macOS / Linux
source .venv/bin/activate

pip install pandas requests beautifulsoup4
```

Optionally create a `requirements.txt`:

```bash
pip freeze > requirements.txt
```

---

## Configuration

The script exposes a few constants at the top of the file that you can change:

* `URL` — the source Wikipedia page (currently an archived URL). Keep or replace with another snapshot.
* `EXCHANGE_RATE_URL` — location of the exchange rate CSV. It can be a remote URL or a local file path (e.g. `"./exchange_rate.csv"`).
* `DB_NAME`, `TABLE_NAME`, `CSV_PATH`, `LOG_FILE` — output file names.

Example: to use a local `exchange_rate.csv`, change `EXCHANGE_RATE_URL` to `"exchange_rate.csv"`.

---

## Usage

Run the ETL script from the project root:

```bash
python etl_bank_script.py
```

What the script will do (in order):

1. Log start of process to `code_log.txt`.
2. Extract bank names and USD market-cap (from the archived Wikipedia table).
3. Save an HTML copy of the first `wikitable` to `table_html.txt`.
4. Transform the data by adding `MC_GBP_Billion`, `MC_EUR_Billion`, and `MC_INR_Billion` using the exchange CSV.
5. Save transformed data to `Largest_banks_data.csv`.
6. Load the data to `Banks.db` into table `Largest_banks`.
7. Run sample SQL queries and print results to console.
8. Close the DB connection and log completion.

---

## Sample SQL queries (included in script)

```sql
SELECT * FROM Largest_banks;
SELECT AVG(MC_GBP_Billion) FROM Largest_banks;
SELECT Name FROM Largest_banks LIMIT 5;
```

You can inspect the database with the `sqlite3` CLI or GUI tools (DB Browser for SQLite, DBeaver, etc.).

---

## Output / Schema

The saved table `Largest_banks` contains these columns:

* `Name` (string)
* `MC_USD_Billion` (float)
* `MC_GBP_Billion` (float)
* `MC_EUR_Billion` (float)
* `MC_INR_Billion` (float)

---

## Logging

Progress is appended to `code_log.txt` with timestamps. Use this file to trace ETL steps and detect where a failure occurred.

---

## Common troubleshooting

* **Requests failing / URL unreachable:** the script uses an archived Wikipedia URL. If the page or snapshot changes, update `URL` to a working snapshot or the current page and adjust parsing logic if the table layout changed.
* **Exchange rates CSV not found:** if `EXCHANGE_RATE_URL` points to a remote CSV and it fails to download, switch to a local CSV path and ensure the CSV columns match the expected format (currency name in first column, rate in second column).
* **Database locks / concurrency:** if the DB file is open in another program, close it before re-running.

---

---

## Contributing

Contributions and improvements are welcome. Ideas:

* Add unit tests for parsing and transformation functions.
* Add CLI flags to control `URL`, `EXCHANGE_RATE_URL`, output file names, and verbosity.
* Add caching for exchange rates and retry/backoff on network calls.

---

## License

This project is provided under the MIT License. See `LICENSE` for details.

---

