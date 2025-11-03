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
HOME_PRICE_BASE_COLUMNS: Sequence[str] = ("city", "avg_home_price")
HOME_PRICE_OPTIONAL_BEACH_COLUMNS: Sequence[str] = ("has_beach", "Beaches", "beach", "has_beaches")
HOME_PRICE_OPTIONAL_MOUNTAIN_COLUMNS: Sequence[str] = ("has_mountain", "Mountains", "mountains", "has_mountains")
HOME_PRICE_OPTIONAL_CONTINENT_COLUMNS: Sequence[str] = ("continent", "Continent", "region", "Region")
HOME_PRICE_KEY_COLUMN = "city_key"
HOME_PRICE_NORMALIZED_BEACH_COLUMN = "has_beach"
HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN = "has_mountain"
HOME_PRICE_NORMALIZED_CONTINENT_COLUMN = "continent"
WINTER_MONTH_NAMES: Sequence[str] = ("December", "January", "February")


@dataclass
class ForecastSnapshot:
    timestamp_utc: datetime
    description: str
    temperature_c: float
    temperature_f: float
    humidity_pct: float


@dataclass
class WeatherReport:
    city: str
    observed: datetime
    description: str
    temperature_c: float
    temperature_f: float
    feels_like_c: float
    feels_like_f: float
    humidity_pct: float
    wind_kmph: float
    forecasts: list[ForecastSnapshot]


def _history_path() -> Path:
    return Path(getattr(settings, "WEATHER_HISTORY_PATH", Path(settings.BASE_DIR) / "weather_history.csv"))


def _home_price_path() -> Path:
    return Path(getattr(settings, "HOME_PRICES_PATH", Path(settings.BASE_DIR) / "home_prices.csv"))


def _empty_home_price_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            *HOME_PRICE_BASE_COLUMNS,
            HOME_PRICE_NORMALIZED_BEACH_COLUMN,
            HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN,
            HOME_PRICE_NORMALIZED_CONTINENT_COLUMN,
            HOME_PRICE_KEY_COLUMN,
        ]
    )


def _c_to_f(value: float) -> float:
    if pd.isna(value):
        return value
    return (float(value) * 9 / 5) + 32


def _coerce_bool(value: object) -> bool:
    if pd.isna(value):
        return False
    if isinstance(value, str):
        return value.strip().lower() in {"true", "1", "yes", "y", "t", "on"}
    return bool(value)


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
        df = pd.read_csv(target, dtype={"city": "string"})

        if "timestamp_utc" not in df.columns:
            return pd.DataFrame(columns=HISTORY_COLUMNS)

        missing_columns = [column for column in HISTORY_COLUMNS if column not in df.columns]
        for column in missing_columns:
            df[column] = pd.NA

        df = df.reindex(columns=HISTORY_COLUMNS)
        df["timestamp_utc"] = pd.to_datetime(df["timestamp_utc"], utc=True, errors="coerce")
        df["city"] = df["city"].astype("string")
        df["source"] = df["source"].astype("category")
        return df
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


def load_home_prices(home_price_path: Path | None = None) -> pd.DataFrame:
    target = home_price_path or _home_price_path()
    if not target.exists():
        return _empty_home_price_df()

    df = pd.read_csv(target)
    if df.empty:
        return _empty_home_price_df()

    missing_columns = [column for column in HOME_PRICE_BASE_COLUMNS if column not in df.columns]
    if missing_columns:
        return _empty_home_price_df()

    cleaned = df[list(HOME_PRICE_BASE_COLUMNS)].copy()
    cleaned["city"] = cleaned["city"].astype("string").str.strip()
    cleaned["avg_home_price"] = pd.to_numeric(cleaned["avg_home_price"], errors="coerce")

    beach_column = next((column for column in HOME_PRICE_OPTIONAL_BEACH_COLUMNS if column in df.columns), None)
    if beach_column is not None:
        cleaned[HOME_PRICE_NORMALIZED_BEACH_COLUMN] = df[beach_column].apply(_coerce_bool)
    else:
        cleaned[HOME_PRICE_NORMALIZED_BEACH_COLUMN] = False

    mountain_column = next((column for column in HOME_PRICE_OPTIONAL_MOUNTAIN_COLUMNS if column in df.columns), None)
    if mountain_column is not None:
        cleaned[HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN] = df[mountain_column].apply(_coerce_bool)
    else:
        cleaned[HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN] = False

    continent_column = next((column for column in HOME_PRICE_OPTIONAL_CONTINENT_COLUMNS if column in df.columns), None)
    if continent_column is not None:
        cleaned[HOME_PRICE_NORMALIZED_CONTINENT_COLUMN] = df[continent_column].astype("string").str.strip().replace({"": pd.NA})
    else:
        cleaned[HOME_PRICE_NORMALIZED_CONTINENT_COLUMN] = pd.NA

    cleaned = cleaned.dropna(subset=["city", "avg_home_price"])
    if cleaned.empty:
        return _empty_home_price_df()

    cleaned["avg_home_price"] = cleaned["avg_home_price"].astype(float)
    cleaned[HOME_PRICE_NORMALIZED_BEACH_COLUMN] = cleaned[HOME_PRICE_NORMALIZED_BEACH_COLUMN].astype(bool)
    cleaned[HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN] = cleaned[HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN].astype(bool)
    cleaned[HOME_PRICE_NORMALIZED_CONTINENT_COLUMN] = cleaned[HOME_PRICE_NORMALIZED_CONTINENT_COLUMN].astype("string")
    cleaned[HOME_PRICE_KEY_COLUMN] = cleaned["city"].str.casefold()
    cleaned = cleaned.drop_duplicates(subset=HOME_PRICE_KEY_COLUMN, keep="last")
    cleaned = cleaned.reindex(
        columns=[
            *HOME_PRICE_BASE_COLUMNS,
            HOME_PRICE_NORMALIZED_BEACH_COLUMN,
            HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN,
            HOME_PRICE_NORMALIZED_CONTINENT_COLUMN,
            HOME_PRICE_KEY_COLUMN,
        ]
    )
    return cleaned.reset_index(drop=True)


def filter_cities_by_home_price(
    home_prices: pd.DataFrame,
    max_price: float | int | None,
    require_beach: bool = False,
    require_mountain: bool = False,
    continents: Sequence[str] | None = None,
) -> pd.DataFrame:
    if home_prices.empty:
        return home_prices

    try:
        threshold = float(max_price) if max_price is not None else float(home_prices["avg_home_price"].max())
    except (TypeError, ValueError):
        threshold = float(home_prices["avg_home_price"].max())

    filtered = home_prices[home_prices["avg_home_price"] <= threshold].copy()
    if require_beach and HOME_PRICE_NORMALIZED_BEACH_COLUMN in filtered.columns:
        filtered = filtered[filtered[HOME_PRICE_NORMALIZED_BEACH_COLUMN]]
    if require_mountain and HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN in filtered.columns:
        filtered = filtered[filtered[HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN]]
    if continents and HOME_PRICE_NORMALIZED_CONTINENT_COLUMN in filtered.columns:
        normalized = {str(value).strip().casefold() for value in continents if str(value).strip()}
        filtered = filtered[filtered[HOME_PRICE_NORMALIZED_CONTINENT_COLUMN].astype("string").str.strip().str.casefold().isin(normalized)]
    return filtered.sort_values("avg_home_price").reset_index(drop=True)


def filter_history_by_cities(history_df: pd.DataFrame, cities_df: pd.DataFrame) -> pd.DataFrame:
    if history_df.empty:
        return history_df
    if cities_df.empty:
        return history_df.iloc[0:0]

    if HOME_PRICE_KEY_COLUMN not in cities_df.columns:
        raise KeyError(f"Expected '{HOME_PRICE_KEY_COLUMN}' column in cities_df")

    city_keys = set(cities_df[HOME_PRICE_KEY_COLUMN])
    filtered = history_df.assign(_city_key=history_df["city"].astype("string").str.strip().str.casefold())
    filtered = filtered[filtered["_city_key"].isin(city_keys)]
    return filtered.drop(columns=["_city_key"]).reset_index(drop=True)


def filter_history_for_winter_snow(history_df: pd.DataFrame, require_winter_snow: bool) -> pd.DataFrame:
    if not require_winter_snow:
        return history_df
    if history_df.empty:
        return history_df

    enriched = history_df.copy()
    enriched["_month_name"] = pd.to_datetime(enriched["timestamp_utc"], utc=True, errors="coerce").dt.month_name()
    enriched["_has_snow"] = enriched["description"].astype("string").str.contains("snow", case=False, na=False)

    mask = enriched["_month_name"].isin(WINTER_MONTH_NAMES) & enriched["_has_snow"]
    filtered = enriched[mask].drop(columns=["_month_name", "_has_snow"], errors="ignore")
    return filtered.reset_index(drop=True)


def home_prices_html(home_prices: pd.DataFrame) -> str | None:
    if home_prices.empty:
        return None

    formatted = home_prices.copy()
    if HOME_PRICE_KEY_COLUMN in formatted.columns:
        formatted = formatted.drop(columns=[HOME_PRICE_KEY_COLUMN])
    formatted = formatted.rename(
        columns={
            "city": "City",
            "avg_home_price": "Avg Home Price (USD)",
            HOME_PRICE_NORMALIZED_BEACH_COLUMN: "Beaches",
            HOME_PRICE_NORMALIZED_MOUNTAIN_COLUMN: "Mountains",
            HOME_PRICE_NORMALIZED_CONTINENT_COLUMN: "Continent",
        }
    )
    if "Beaches" in formatted.columns:
        formatted["Beaches"] = formatted["Beaches"].map(lambda value: "Yes" if bool(value) else "No")
    if "Mountains" in formatted.columns:
        formatted["Mountains"] = formatted["Mountains"].map(lambda value: "Yes" if bool(value) else "No")
    if "Continent" in formatted.columns:
        formatted["Continent"] = formatted["Continent"].fillna("—")
    formatted["Avg Home Price (USD)"] = formatted["Avg Home Price (USD)"].map(lambda value: f"${value:,.0f}")
    column_order = ["City", "Avg Home Price (USD)"]
    if "Beaches" in formatted.columns:
        column_order.append("Beaches")
    if "Mountains" in formatted.columns:
        column_order.append("Mountains")
    if "Continent" in formatted.columns:
        column_order.append("Continent")
    formatted = formatted[column_order]
    return formatted.to_html(classes=["table", "table-compact"], index=False, border=0, justify="center")


def build_report(df: pd.DataFrame) -> WeatherReport:
    current_row = df[df["source"] == "current"].iloc[-1]
    forecast_rows = df[df["source"] == "forecast"].nlargest(3, "timestamp_utc")
    forecasts: list[ForecastSnapshot] = [
        ForecastSnapshot(
            timestamp_utc=row.timestamp_utc,
            description=row.description,
            temperature_c=float(row.temperature_C),
            temperature_f=_c_to_f(float(row.temperature_C)),
            humidity_pct=float(row.humidity_pct),
        )
        for row in forecast_rows.itertuples(index=False)
    ]
    return WeatherReport(
        city=str(current_row.city),
        observed=current_row.timestamp_utc,
        description=str(current_row.description),
        temperature_c=float(current_row.temperature_C),
        temperature_f=_c_to_f(float(current_row.temperature_C)),
        feels_like_c=float(current_row.feels_like_C),
        feels_like_f=_c_to_f(float(current_row.feels_like_C)),
        humidity_pct=float(current_row.humidity_pct),
        wind_kmph=float(current_row.wind_kmph),
        forecasts=sorted(forecasts, key=lambda snap: snap.timestamp_utc),
    )


def format_report_lines(report: WeatherReport) -> Iterable[str]:
    yield f"City: {report.city}"
    yield f"Observed (UTC): {report.observed:%Y-%m-%d %H:%M}"
    yield f"Conditions: {report.description}"
    yield (f"Temp / Feels Like: {report.temperature_c:.1f}°C ({report.temperature_f:.1f}°F) / {report.feels_like_c:.1f}°C ({report.feels_like_f:.1f}°F)")
    yield f"Humidity (%): {report.humidity_pct:.0f}"
    yield f"Wind (km/h): {report.wind_kmph:.0f}"
    yield "Forecast snapshots:"
    for forecast in report.forecasts:
        yield (f"  {forecast.timestamp_utc:%Y-%m-%d} — {forecast.description} — Temp {forecast.temperature_c:.1f}°C ({forecast.temperature_f:.1f}°F), Humidity {forecast.humidity_pct:.0f}%")


def dataframe_tail_html(df: pd.DataFrame, limit: int | None = None) -> str | None:
    if df.empty:
        return None
    filtered = df[df["source"] == "current"]
    if filtered.empty:
        return None
    tail = filtered.tail(limit or getattr(settings, "WEATHER_HISTORY_TAIL", 10))
    formatted = tail.copy()
    formatted["timestamp_utc"] = pd.to_datetime(formatted["timestamp_utc"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d %H:%M")
    formatted["temperature_F"] = formatted["temperature_C"].apply(_c_to_f)
    formatted["feels_like_F"] = formatted["feels_like_C"].apply(_c_to_f)
    formatted = formatted.rename(
        columns={
            "city": "City",
            "timestamp_utc": "Timestamp (UTC)",
            "source": "Source",
            "description": "Description",
            "temperature_C": "Temp °C",
            "temperature_F": "Temp °F",
            "feels_like_C": "Feels Like °C",
            "feels_like_F": "Feels Like °F",
            "humidity_pct": "Humidity %",
            "wind_kmph": "Wind km/h",
        }
    )
    return formatted.to_html(classes=["table", "table-striped"], index=False, border=0, justify="center")
