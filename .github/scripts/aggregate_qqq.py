"""
Aggregates hedge-fund-tracker CSV data into a JSON summary for the 11 QQQ tickers.

Run after fetcher.py in the GitHub Actions workflow:
    pipenv run python .github/scripts/aggregate_qqq.py

Produces database/qqq-summary.json
"""

import csv
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

QQQ_TICKERS = {"NVDA", "AAPL", "MSFT", "META", "AMZN", "GOOGL", "AVGO", "TSLA", "AMD", "NFLX", "CRM"}

DATABASE_DIR = Path(__file__).resolve().parent.parent.parent / "database"


def find_latest_quarter(database_dir: Path) -> str | None:
    """Find the most recent quarter folder (e.g. '2025Q4')."""
    quarter_pattern = re.compile(r"^\d{4}Q[1-4]$")
    quarters = sorted(
        [d.name for d in database_dir.iterdir() if d.is_dir() and quarter_pattern.match(d.name)],
        reverse=True,
    )
    return quarters[0] if quarters else None


def load_fund_managers(database_dir: Path) -> dict[str, str]:
    """Load fund -> manager mapping from hedge_funds.csv."""
    managers = {}
    path = database_dir / "hedge_funds.csv"
    if not path.exists():
        return managers
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            fund = row.get("Fund", "").strip()
            manager = row.get("Manager", "").strip()
            if fund:
                managers[fund] = manager
    return managers


def parse_fund_csv(filepath: Path) -> list[dict]:
    """Parse a single fund CSV and return rows as dicts."""
    rows = []
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows


def classify_delta(delta_str: str) -> str:
    """Classify a delta string into a category."""
    d = delta_str.strip().upper()
    if d == "NEW":
        return "new"
    if d == "CLOSE":
        return "closed"
    if d.startswith("+"):
        return "increased"
    if d.startswith("-"):
        return "decreased"
    return "unchanged"


def aggregate_quarter(database_dir: Path, quarter: str, managers: dict[str, str]) -> dict:
    """Aggregate all fund CSVs for a quarter, filtered to QQQ tickers."""
    quarter_dir = database_dir / quarter
    holdings: dict[str, dict] = {t: {"positions": [], "summary": {"increased": 0, "decreased": 0, "new": 0, "closed": 0}} for t in QQQ_TICKERS}

    for csv_file in sorted(quarter_dir.glob("*.csv")):
        fund_name = csv_file.stem
        rows = parse_fund_csv(csv_file)

        for row in rows:
            ticker = row.get("Ticker", "").strip()
            if ticker not in QQQ_TICKERS:
                continue

            delta_str = row.get("Delta", "").strip()
            category = classify_delta(delta_str)

            # Parse numeric fields safely
            shares_str = row.get("Shares", "0").strip().replace(",", "")
            delta_shares_str = row.get("Delta_Shares", "0").strip().replace(",", "")

            try:
                shares = int(shares_str)
            except (ValueError, TypeError):
                shares = 0

            try:
                delta_shares = int(delta_shares_str)
            except (ValueError, TypeError):
                delta_shares = 0

            position = {
                "fund": fund_name,
                "manager": managers.get(fund_name, ""),
                "shares": shares,
                "value": row.get("Value", "N/A").strip(),
                "portfolioPct": row.get("Portfolio%", "N/A").strip(),
                "delta": delta_str,
                "deltaShares": delta_shares,
            }

            holdings[ticker]["positions"].append(position)
            if category in holdings[ticker]["summary"]:
                holdings[ticker]["summary"][category] += 1

    # Sort positions by value descending (parse M/B suffixes)
    for ticker in holdings:
        holdings[ticker]["positions"].sort(key=lambda p: _parse_value(p["value"]), reverse=True)
        holdings[ticker]["totalFunds"] = len(holdings[ticker]["positions"])

    return holdings


def _parse_value(value_str: str) -> float:
    """Parse value strings like '71.3M' or '1.2B' into floats for sorting."""
    s = value_str.strip().replace("$", "").replace(",", "")
    try:
        if s.upper().endswith("B"):
            return float(s[:-1]) * 1_000_000_000
        if s.upper().endswith("M"):
            return float(s[:-1]) * 1_000_000
        if s.upper().endswith("K"):
            return float(s[:-1]) * 1_000
        return float(s)
    except (ValueError, TypeError):
        return 0.0


def aggregate_non_quarterly(database_dir: Path) -> dict:
    """Parse non_quarterly.csv and filter to QQQ tickers."""
    nq_path = database_dir / "non_quarterly.csv"
    result: dict[str, list] = {t: [] for t in QQQ_TICKERS}

    if not nq_path.exists():
        return result

    with open(nq_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            ticker = row.get("Ticker", "").strip()
            if ticker not in QQQ_TICKERS:
                continue

            shares_str = row.get("Shares", "0").strip().replace(",", "")
            try:
                shares = int(shares_str)
            except (ValueError, TypeError):
                shares = 0

            avg_price_str = row.get("Avg_Price", "0").strip()
            try:
                avg_price = float(avg_price_str)
            except (ValueError, TypeError):
                avg_price = 0.0

            entry = {
                "fund": row.get("Fund", "").strip(),
                "shares": shares,
                "value": row.get("Value", "N/A").strip(),
                "avgPrice": avg_price,
                "date": row.get("Date", "").strip(),
                "filingDate": row.get("Filing_Date", "").strip(),
            }
            result[ticker].append(entry)

    # Sort by filing date descending
    for ticker in result:
        result[ticker].sort(key=lambda x: x["filingDate"], reverse=True)

    return result


def main():
    quarter = find_latest_quarter(DATABASE_DIR)
    if not quarter:
        print("No quarterly data found.")
        return

    print(f"Aggregating data for quarter: {quarter}")
    managers = load_fund_managers(DATABASE_DIR)
    print(f"Loaded {len(managers)} fund managers")

    holdings = aggregate_quarter(DATABASE_DIR, quarter, managers)
    non_quarterly = aggregate_non_quarterly(DATABASE_DIR)

    summary = {
        "quarter": quarter,
        "generatedAt": datetime.now(timezone.utc).isoformat(),
        "holdings": holdings,
        "nonQuarterly": non_quarterly,
    }

    output_path = DATABASE_DIR / "qqq-summary.json"
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    # Print stats
    for ticker in sorted(QQQ_TICKERS):
        h = holdings[ticker]
        nq = non_quarterly[ticker]
        print(f"  {ticker}: {h['totalFunds']} funds, {len(nq)} non-quarterly filings")

    print(f"\nOutput written to {output_path}")


if __name__ == "__main__":
    main()
