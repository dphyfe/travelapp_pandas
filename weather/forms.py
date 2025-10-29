from django import forms


class CityForm(forms.Form):
    city = forms.CharField(
        label="City",
        max_length=80,
        widget=forms.TextInput(attrs={"placeholder": "e.g. London", "class": "w-full"}),
    )
