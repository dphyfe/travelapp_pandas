"""Shared weather utilities using pandas."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

import pandas as pd
import requests
from django.conf import settings

WTTR_URL_TEMPLATE = "https://wttr.in/{city}?format=j1"
HISTORY_COLUMNS: Sequence[str] = (
    "city",
    "timestamp_utc",
    "source",
    "description",
    "temperature_C",
    "feels_like_C",
    "humidity_pct",
    "wind_kmph",
)


@dataclass
class ForecastSnapshot:
    timestamp_utc: datetime
    description: str
    temperature_c: float
    humidity_pct: float


@dataclass
class WeatherReport:
    city: str
    observed: datetime
    description: str
    temperature_c: float
    feels_like_c: float
    humidity_pct: float
    wind_kmph: float
    forecasts: list[ForecastSnapshot]


def _history_path() -> Path:
    return Path(getattr(settings, "WEATHER_HISTORY_PATH", Path(settings.BASE_DIR) / "weather_history.csv"))


def fetch_weather_payload(city: str) -> dict:
    response = requests.get(WTTR_URL_TEMPLATE.format(city=city), timeout=15)
    response.raise_for_status()
    return response.json()


def normalize_weather(city: str, payload: dict) -> pd.DataFrame:
    current = payload["current_condition"][0]
    now = datetime.now(timezone.utc)
    rows: list[dict[str, object]] = [
        {
            "city": city,
            "timestamp_utc": now,
            "source": "current",
            "description": current["weatherDesc"][0]["value"],
            "temperature_C": float(current["temp_C"]),
            "feels_like_C": float(current["FeelsLikeC"]),
            "humidity_pct": float(current["humidity"]),
            "wind_kmph": float(current["windspeedKmph"]),
        }
    ]

    for day in payload.get("weather", []):
        date_str = day["date"]
        midday_block = day["hourly"][4] if len(day["hourly"]) >= 5 else day["hourly"][0]
        when = datetime.fromisoformat(f"{date_str}T12:00:00+00:00")
        rows.append(
            {
                "city": city,
                "timestamp_utc": when,
                "source": "forecast",
                "description": midday_block["weatherDesc"][0]["value"],
                "temperature_C": float(midday_block["tempC"]),
                "feels_like_C": float(midday_block["FeelsLikeC"]),
                "humidity_pct": float(midday_block["humidity"]),
                "wind_kmph": float(midday_block["windspeedKmph"]),
            }
        )

    df = pd.DataFrame(rows, columns=HISTORY_COLUMNS)
    df = df.astype({"city": "string", "source": "category"})
    return df


def load_history(history_path: Path | None = None) -> pd.DataFrame:
    target = history_path or _history_path()
    if target.exists():
        return pd.read_csv(target, parse_dates=["timestamp_utc"], dtype={"city": "string"})
    return pd.DataFrame(columns=HISTORY_COLUMNS)


def append_history(history_df: pd.DataFrame, new_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        combined = new_df.copy()
    else:
        combined = pd.concat([history_df, new_df], ignore_index=True)
    combined = combined.drop_duplicates(subset=["city", "timestamp_utc", "source"], keep="last")
    combined = combined.sort_values("timestamp_utc")
    return combined.reset_index(drop=True)


def persist_history(history_path: Path | None, df: pd.DataFrame) -> None:
    target = history_path or _history_path()
    df.to_csv(target, index=False, date_format="%Y-%m-%dT%H:%M:%SZ")


def build_report(df: pd.DataFrame) -> WeatherReport:
    current_row = df[df["source"] == "current"].iloc[-1]
    forecast_rows = df[df["source"] == "forecast"].nlargest(3, "timestamp_utc")
    forecasts: list[ForecastSnapshot] = [
        ForecastSnapshot(
            timestamp_utc=row.timestamp_utc,
            description=row.description,
            temperature_c=float(row.temperature_C),
            humidity_pct=float(row.humidity_pct),
        )
        for row in forecast_rows.itertuples(index=False)
    ]
    return WeatherReport(
        city=str(current_row.city),
        observed=current_row.timestamp_utc,
        description=str(current_row.description),
        temperature_c=float(current_row.temperature_C),
        feels_like_c=float(current_row.feels_like_C),
        humidity_pct=float(current_row.humidity_pct),
        wind_kmph=float(current_row.wind_kmph),
        forecasts=sorted(forecasts, key=lambda snap: snap.timestamp_utc),
    )


def format_report_lines(report: WeatherReport) -> Iterable[str]:
    yield f"City: {report.city}"
    yield f"Observed (UTC): {report.observed:%Y-%m-%d %H:%M}"
    yield f"Conditions: {report.description}"
    yield (f"Temp / Feels Like (°C): {report.temperature_c:.1f} / {report.feels_like_c:.1f}")
    yield f"Humidity (%): {report.humidity_pct:.0f}"
    yield f"Wind (km/h): {report.wind_kmph:.0f}"
    yield "Forecast snapshots:"
    for forecast in report.forecasts:
        yield (f"  {forecast.timestamp_utc:%Y-%m-%d} — {forecast.description} — Temp {forecast.temperature_c:.1f}°C, Humidity {forecast.humidity_pct:.0f}%")


def dataframe_tail_html(df: pd.DataFrame, limit: int | None = None) -> str | None:
    if df.empty:
        return None
    tail = df.tail(limit or getattr(settings, "WEATHER_HISTORY_TAIL", 10))
    formatted = tail.copy()
    formatted["timestamp_utc"] = pd.to_datetime(formatted["timestamp_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
    formatted = formatted.rename(
        columns={
            "city": "City",
            "timestamp_utc": "Timestamp (UTC)",
            "source": "Source",
            "description": "Description",
            "temperature_C": "Temp °C",
            "feels_like_C": "Feels Like °C",
            "humidity_pct": "Humidity %",
            "wind_kmph": "Wind km/h",
        }
    )
    return formatted.to_html(classes=["table", "table-striped"], index=False, border=0, justify="center")
