from django import forms


class CityForm(forms.Form):
    city = forms.CharField(
        label="City",
        max_length=80,
        widget=forms.TextInput(attrs={"placeholder": "e.g. London", "class": "w-full"}),
    )


class HomeFilterForm(forms.Form):
    max_price = forms.IntegerField(label="Max Avg Home Price (USD)", min_value=0)
    has_beach = forms.BooleanField(label="City has beaches", required=False)
    has_mountain = forms.BooleanField(label="City has mountains", required=False)
    has_winter_snow = forms.BooleanField(label="City has winter snow", required=False)
    continents = forms.MultipleChoiceField(
        label="Continents",
        required=False,
        choices=(),
        widget=forms.CheckboxSelectMultiple,
    )

    def __init__(self, price_min: int, price_max: int, *args, **kwargs) -> None:
        step = int(kwargs.pop("step", 10000))
        beach_available = bool(kwargs.pop("beach_available", True))
        mountain_available = bool(kwargs.pop("mountain_available", True))
        winter_snow_available = bool(kwargs.pop("winter_snow_available", True))
        continent_choices = list(kwargs.pop("continent_choices", ()))

        coerced_min = max(0, int(price_min)) if price_min is not None else 0
        coerced_max = max(coerced_min, int(price_max)) if price_max is not None else coerced_min
        slider_step = max(1, step)

        initial = kwargs.setdefault("initial", {})
        initial.setdefault("max_price", coerced_max)
        initial.setdefault("has_beach", False)
        initial.setdefault("has_mountain", False)
        initial.setdefault("has_winter_snow", False)
        initial.setdefault("continents", [])

        super().__init__(*args, **kwargs)

        price_field = self.fields["max_price"]
        price_field.min_value = coerced_min
        price_field.max_value = coerced_max
        price_field.widget = forms.NumberInput(
            attrs={
                "type": "range",
                "min": coerced_min,
                "max": coerced_max,
                "step": slider_step,
                "class": "slider-input",
                "onchange": "this.form.submit()",
                "aria-label": "Filter cities by average home price",
            }
        )

        for field_name, available, title in (
            ("has_beach", beach_available, "Beach data unavailable for these cities"),
            ("has_mountain", mountain_available, "Mountain data unavailable for these cities"),
            ("has_winter_snow", winter_snow_available, "Winter snow data unavailable for these cities"),
        ):
            field = self.fields[field_name]
            field.widget = forms.CheckboxInput(
                attrs={
                    "class": "toggle-input",
                    "onchange": "this.form.submit()",
                }
            )
            if not available:
                field.widget.attrs["disabled"] = "disabled"
                field.widget.attrs["title"] = title

        continent_field = self.fields["continents"]
        normalized_choices = []
        seen_values = set()
        for choice in continent_choices:
            if isinstance(choice, (list, tuple)) and choice:
                if len(choice) == 1:
                    raw_value = choice[0]
                    raw_label = choice[0]
                else:
                    raw_value, raw_label = choice[0], choice[1]
            else:
                raw_value = choice
                raw_label = choice

            value = str(raw_value).strip() if raw_value is not None else ""
            label = str(raw_label).strip() if raw_label is not None else ""
            if not value:
                continue
            if not label:
                label = value
            if value in seen_values:
                continue
            normalized_choices.append((value, label))
            seen_values.add(value)
        continent_field.choices = tuple(normalized_choices)
        continent_field.widget = forms.CheckboxSelectMultiple(
            attrs={
                "class": "checkbox-grid",
            }
        )
        if not normalized_choices:
            continent_field.disabled = True
            continent_field.widget.attrs["title"] = "Continent data unavailable for these cities"
