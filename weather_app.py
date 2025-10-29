"""Simple weather CLI leveraging the Django-powered services."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from weather import services


def run_cli() -> None:
    parser = argparse.ArgumentParser(description="Fetch and store weather data using pandas.")
    parser.add_argument("city", help="City name, e.g. 'London' or 'New York'.")
    parser.add_argument("--history", default="weather_history.csv", help="History CSV path.")
    parser.add_argument(
        "--tail",
        type=int,
        default=5,
        help="Show the most recent N history rows after updating.",
    )
    parser.add_argument("--skip-fetch", action="store_true", help="Skip fetching and just show saved history.")
    args = parser.parse_args()

    history_path = Path(args.history)
    history_df = services.load_history(history_path)

    if not args.skip_fetch:
        payload = services.fetch_weather_payload(args.city)
        fresh_df = services.normalize_weather(args.city, payload)
        report = services.build_report(fresh_df)
        history_df = services.append_history(history_df, fresh_df)
        services.persist_history(history_path, history_df)

        for line in services.format_report_lines(report):
            print(line)
    else:
        if history_df.empty:
            raise SystemExit("No history found; run without --skip-fetch first.")

    if args.tail:
        print("\nSaved history tail:")
        with pd.option_context("display.max_columns", None):
            print(history_df.tail(args.tail))


if __name__ == "__main__":
    run_cli()
