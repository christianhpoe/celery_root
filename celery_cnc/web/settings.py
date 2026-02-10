"""Django settings for the Celery CnC web app."""

from __future__ import annotations

import os
from pathlib import Path

from celery_cnc.config import get_settings

BASE_DIR = Path(__file__).resolve().parent
CONFIG = get_settings()

SECRET_KEY = CONFIG.secret_key
DEBUG = CONFIG.web_debug
ALLOWED_HOSTS = ["*"]

INSTALLED_APPS = [
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.staticfiles",
]

MIDDLEWARE = [
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "celery_cnc.web.auth.AuthMiddleware",
]

ROOT_URLCONF = "celery_cnc.web.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [str(BASE_DIR / "templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
            ],
        },
    },
]

WSGI_APPLICATION = "celery_cnc.web.wsgi.application"
ASGI_APPLICATION = "celery_cnc.web.asgi.application"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": str(BASE_DIR / "sqlite3.db"),
    },
}

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATICFILES_DIRS = [str(BASE_DIR / "static")]

SESSION_ENGINE = "django.contrib.sessions.backends.signed_cookies"

CELERY_CNC_DB_PATH = Path(CONFIG.db_path)
CELERY_CNC_LOG_DIR = Path(CONFIG.log_dir)
CELERY_CNC_RETENTION_DAYS = int(CONFIG.retention_days)


def _parse_worker_paths(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [item.strip() for item in raw.split(",") if item.strip()]


CELERY_CNC_WORKERS = _parse_worker_paths(os.getenv("CELERY_CNC_WORKERS"))

CELERY_CNC_BASIC_AUTH = CONFIG.basic_auth
CELERY_CNC_AUTH_PROVIDER = CONFIG.auth_provider
CELERY_CNC_AUTH = CONFIG.auth
CELERY_CNC_OAUTH2_KEY = CONFIG.oauth2_key
CELERY_CNC_OAUTH2_SECRET = CONFIG.oauth2_secret
CELERY_CNC_OAUTH2_REDIRECT_URI = CONFIG.oauth2_redirect_uri
CELERY_CNC_OAUTH2_OKTA_BASE_URL = CONFIG.oauth2_okta_base_url
CELERY_CNC_GITLAB_AUTH_ALLOWED_GROUPS = CONFIG.gitlab_allowed_groups
CELERY_CNC_GITLAB_MIN_ACCESS_LEVEL = CONFIG.gitlab_min_access_level
CELERY_CNC_GITLAB_OAUTH_DOMAIN = CONFIG.gitlab_oauth_domain

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
