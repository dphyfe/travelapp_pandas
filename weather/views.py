from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.contrib import messages
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from .forms import CityForm, HomeFilterForm
from . import services
from .services import HOME_PRICE_NORMALIZED_CONTINENT_COLUMN


CONTINENT_CODE_LOOKUP: dict[str, str] = {
    "africa": "AF",
    "antarctica": "AN",
    "asia": "AS",
    "australia": "AU",
    "australia and oceania": "AU",
    "central america": "CA",
    "europe": "EU",
    "middle east": "ME",
    "north america": "NA",
    "oceania": "OC",
    "south america": "SA",
}


def _normalize_continent_key(value: str | None) -> str:
    if value is None:
        return ""
    sanitized = str(value).strip()
    if not sanitized:
        return ""
    normalized = sanitized.replace("_", " ").replace("-", " ").replace("/", " ").replace("&", " and ")
    normalized = " ".join(normalized.split())
    return normalized.casefold()


def _derive_continent_code(continent_name: str) -> str:
    normalized_key = _normalize_continent_key(continent_name)
    if normalized_key in CONTINENT_CODE_LOOKUP:
        return CONTINENT_CODE_LOOKUP[normalized_key]

    tokens = [token for token in normalized_key.split() if token and token not in {"and"}]
    if not tokens:
        fallback = "".join(ch for ch in continent_name.upper() if ch.isalpha())
        return fallback[:2] or "XX"

    if len(tokens) == 1:
        token = tokens[0]
        if len(token) >= 2:
            return token[:2].upper()
        return (token * 2).upper()[:2]

    code = "".join(token[0] for token in tokens if token)
    if len(code) < 2:
        code = "".join(token[:2] for token in tokens)
    code = code[:3]
    if len(code) < 2:
        fallback = "".join(ch for ch in continent_name.upper() if ch.isalpha())
        code = fallback[:2]
    return code.upper()


def home(request: HttpRequest) -> HttpResponse:
    history_path = Path(getattr(settings, "WEATHER_HISTORY_PATH", settings.BASE_DIR / "weather_history.csv"))
    history_df = services.load_history(history_path)
    home_price_path = Path(getattr(settings, "HOME_PRICES_PATH", settings.BASE_DIR / "home_prices.csv"))
    home_price_df = services.load_home_prices(home_price_path)

    supports_beach_filter = not home_price_df.empty and "has_beach" in home_price_df.columns
    supports_mountain_filter = not home_price_df.empty and "has_mountain" in home_price_df.columns
    supports_continent_filter = not home_price_df.empty and HOME_PRICE_NORMALIZED_CONTINENT_COLUMN in home_price_df.columns
    supports_winter_snow_filter = not history_df.empty and {"timestamp_utc", "description"}.issubset(history_df.columns)

    if supports_continent_filter:
        available_continent_map: dict[str, str] = {}
        continent_code_by_canonical: dict[str, str] = {}
        for raw_value in home_price_df[HOME_PRICE_NORMALIZED_CONTINENT_COLUMN].dropna().astype("string"):
            canonical_candidate = str(raw_value).strip()
            if not canonical_candidate:
                continue
            canonical_key = _normalize_continent_key(canonical_candidate)
            canonical_value = available_continent_map.setdefault(canonical_key, canonical_candidate)
            available_continent_map.setdefault(canonical_value.casefold(), canonical_value)

            code = continent_code_by_canonical.get(canonical_value)
            if not code:
                code = _derive_continent_code(canonical_value)
            continent_code_by_canonical[canonical_value] = code
            available_continent_map.setdefault(code.casefold(), canonical_value)

        continent_choices = [(code, f"{code} — {canonical}") for canonical, code in sorted(continent_code_by_canonical.items(), key=lambda item: item[0].casefold())]

        for alias, code in CONTINENT_CODE_LOOKUP.items():
            canonical = available_continent_map.get(alias)
            if canonical:
                available_continent_map.setdefault(code.casefold(), canonical)
    else:
        available_continent_map = {}
        continent_code_by_canonical = {}
        continent_choices = []

    price_min = int(home_price_df["avg_home_price"].min()) if not home_price_df.empty else 0
    price_max = int(home_price_df["avg_home_price"].max()) if not home_price_df.empty else 0
    slider_range = price_max - price_min
    if slider_range > 0:
        slider_step = max(5000, slider_range // 12)
    else:
        slider_step = max(5000, price_max // 10 if price_max else 5000)
    slider_step = max(5000, (slider_step // 5000) * 5000 or 5000)

    raw_max = request.GET.get("max_price")
    try:
        initial_max = int(raw_max) if raw_max is not None else (price_max or 0)
    except (TypeError, ValueError):
        initial_max = price_max or 0
    initial_max = max(price_min, min(initial_max, price_max)) if price_max else max(price_min, initial_max)

    def parse_bool_flag(raw_value: str | None) -> bool:
        if raw_value is None:
            return False
        return raw_value.strip().lower() not in {"", "0", "false", "no", "off"}

    initial_has_beach = parse_bool_flag(request.GET.get("has_beach")) if supports_beach_filter else False
    initial_has_mountain = parse_bool_flag(request.GET.get("has_mountain")) if supports_mountain_filter else False
    initial_has_winter_snow = parse_bool_flag(request.GET.get("has_winter_snow")) if supports_winter_snow_filter else False
    requested_continents = request.GET.getlist("continents") if supports_continent_filter else []
    initial_continents: list[str] = []
    initial_continent_codes: list[str] = []
    seen_continents: set[str] = set()
    for continent in requested_continents:
        lookup_key = _normalize_continent_key(continent)
        canonical_value = available_continent_map.get(lookup_key)
        if not canonical_value or canonical_value in seen_continents:
            continue
        seen_continents.add(canonical_value)
        initial_continents.append(canonical_value)
        mapped_code = continent_code_by_canonical.get(canonical_value)
        if mapped_code:
            initial_continent_codes.append(mapped_code)

    bound_get = request.GET or None
    home_filter_initial = {
        "max_price": initial_max,
        "has_beach": initial_has_beach,
        "has_mountain": initial_has_mountain,
        "has_winter_snow": initial_has_winter_snow,
        "continents": initial_continent_codes,
    }

    home_filter_form = HomeFilterForm(
        price_min,
        price_max,
        data=bound_get,
        initial=home_filter_initial,
        step=slider_step,
        beach_available=supports_beach_filter,
        mountain_available=supports_mountain_filter,
        winter_snow_available=supports_winter_snow_filter,
        continent_choices=continent_choices,
    )

    selected_max_price = initial_max
    selected_has_beach = initial_has_beach
    selected_has_mountain = initial_has_mountain
    selected_has_winter_snow = initial_has_winter_snow
    selected_continent_codes = list(initial_continent_codes)
    selected_continents = list(initial_continents)

    if home_filter_form.is_bound:
        if home_filter_form.is_valid():
            selected_max_price = home_filter_form.cleaned_data["max_price"]
            selected_has_beach = home_filter_form.cleaned_data["has_beach"]
            selected_has_mountain = home_filter_form.cleaned_data["has_mountain"]
            selected_has_winter_snow = home_filter_form.cleaned_data["has_winter_snow"]
            selected_continent_codes = list(home_filter_form.cleaned_data.get("continents", []))
            selected_continents = []
            seen_selected: set[str] = set()
            for code in selected_continent_codes:
                canonical_value = available_continent_map.get(_normalize_continent_key(code))
                if canonical_value and canonical_value not in seen_selected:
                    seen_selected.add(canonical_value)
                    selected_continents.append(canonical_value)
        else:
            selected_max_price = initial_max
            selected_has_beach = initial_has_beach
            selected_has_mountain = initial_has_mountain
            selected_has_winter_snow = initial_has_winter_snow
            selected_continent_codes = list(initial_continent_codes)
            selected_continents = list(initial_continents)
            home_filter_form = HomeFilterForm(
                price_min,
                price_max,
                initial={
                    "max_price": selected_max_price,
                    "has_beach": selected_has_beach,
                    "has_mountain": selected_has_mountain,
                    "has_winter_snow": selected_has_winter_snow,
                    "continents": selected_continent_codes,
                },
                step=slider_step,
                beach_available=supports_beach_filter,
                mountain_available=supports_mountain_filter,
                winter_snow_available=supports_winter_snow_filter,
                continent_choices=continent_choices,
            )

    if not supports_beach_filter:
        selected_has_beach = False
    if not supports_mountain_filter:
        selected_has_mountain = False
    if not supports_winter_snow_filter:
        selected_has_winter_snow = False
    if not supports_continent_filter:
        selected_continents = []
        selected_continent_codes = []

    filtered_price_df = services.filter_cities_by_home_price(
        home_price_df,
        selected_max_price,
        require_beach=selected_has_beach,
        require_mountain=selected_has_mountain,
        continents=selected_continents,
    )

    def human_join(parts: list[str]) -> str:
        if not parts:
            return ""
        if len(parts) == 1:
            return parts[0]
        if len(parts) == 2:
            return f"{parts[0]} and {parts[1]}"
        return ", ".join(parts[:-1]) + f", and {parts[-1]}"

    filter_parts: list[str] = [
        "Showing cities with an average home price at or below the selected value",
    ]
    feature_clauses: list[str] = []
    if selected_has_beach:
        feature_clauses.append("beaches")
    if selected_has_mountain:
        feature_clauses.append("mountains")
    if feature_clauses:
        feature_text = human_join(feature_clauses)
        filter_parts.append(f" and with {feature_text}")
    if selected_continents:
        continents_text = human_join(selected_continents)
        filter_parts.append(f" located in {continents_text}")
    if selected_has_winter_snow:
        filter_parts.append(" while highlighting winter snow observations")
    home_filter_description = "".join(filter_parts) + "."

    # Compose the DataFrame shown in the UI after applying temperature, home price, and winter filters.
    def build_display_frame(source_df):
        display = source_df[source_df["temperature_C"] > 15].sort_values("temperature_C", ascending=False).reset_index(drop=True)
        if home_price_df.empty:
            filtered_display = display
        else:
            filtered_display = services.filter_history_by_cities(display, filtered_price_df)
        return services.filter_history_for_winter_snow(filtered_display, selected_has_winter_snow)

    display_history_df = build_display_frame(history_df)

    context: dict[str, object] = {
        "form": CityForm(initial={"city": request.GET.get("city", "")}),
        "history_html": services.dataframe_tail_html(
            display_history_df,
            getattr(settings, "WEATHER_HISTORY_TAIL", 10),
        ),
        "home_filter_form": home_filter_form,
        "home_price_display_value": f"${selected_max_price:,.0f}" if selected_max_price else "$0",
        "home_price_summary_html": services.home_prices_html(filtered_price_df),
        "filtered_cities": filtered_price_df["city"].tolist(),
        "display_history_count": len(display_history_df),
        "home_prices_available": not home_price_df.empty,
        "home_filter_description": home_filter_description,
        "home_filter_has_beach": selected_has_beach,
        "home_filter_supports_beach": supports_beach_filter,
        "home_filter_has_mountain": selected_has_mountain,
        "home_filter_supports_mountain": supports_mountain_filter,
        "home_filter_has_winter_snow": selected_has_winter_snow,
        "home_filter_supports_winter_snow": supports_winter_snow_filter,
        "home_filter_supports_continent": supports_continent_filter,
        "home_filter_selected_continents": selected_continents,
        "home_filter_selected_continent_codes": selected_continent_codes,
        "home_filter_continent_choices": continent_choices,
    }

    if request.method == "POST":
        form = CityForm(request.POST)
        if form.is_valid():
            city = form.cleaned_data["city"].strip()
            try:
                payload = services.fetch_weather_payload(city)
                fresh_df = services.normalize_weather(city, payload)
                report = services.build_report(fresh_df)
                history_df = services.append_history(history_df, fresh_df)
                services.persist_history(history_path, history_df)
                display_history_df = build_display_frame(history_df)
                context["report"] = report
                context["history_html"] = services.dataframe_tail_html(display_history_df)
                context["display_history_count"] = len(display_history_df)
                context["form"] = CityForm(initial={"city": report.city})
            except Exception as exc:  # noqa: BLE001 - show error to user
                messages.error(request, f"Could not fetch weather for {city}: {exc}")
                context["form"] = form
        else:
            context["form"] = form

    return render(request, "weather/home.html", context)
