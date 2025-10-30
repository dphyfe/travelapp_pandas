from __future__ import annotations

from pathlib import Path

from django.conf import settings
from django.contrib import messages
from django.http import HttpRequest, HttpResponse
from django.shortcuts import render

from .forms import CityForm
from . import services


def home(request: HttpRequest) -> HttpResponse:
    history_path = Path(getattr(settings, "WEATHER_HISTORY_PATH", settings.BASE_DIR / "weather_history.csv"))
    history_df = services.load_history(history_path)

    display_history_df = history_df[history_df["temperature_C"] > 15].sort_values("temperature_C", ascending=False).reset_index(drop=True)

    context: dict[str, object] = {
        "form": CityForm(initial={"city": request.GET.get("city", "")}),
        "history_html": services.dataframe_tail_html(
            display_history_df,
            getattr(settings, "WEATHER_HISTORY_TAIL", 10),
        ),
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
                display_history_df = history_df[history_df["temperature_C"] > 15].sort_values("temperature_C", ascending=False).reset_index(drop=True)
                context["report"] = report
                context["history_html"] = services.dataframe_tail_html(display_history_df)
                context["form"] = CityForm(initial={"city": report.city})
            except Exception as exc:  # noqa: BLE001 - show error to user
                messages.error(request, f"Could not fetch weather for {city}: {exc}")
                context["form"] = form
        else:
            context["form"] = form

    return render(request, "weather/home.html", context)
