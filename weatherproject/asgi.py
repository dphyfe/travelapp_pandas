"""ASGI config for weatherproject."""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "weatherproject.settings")

application = get_asgi_application()
