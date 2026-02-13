# SPDX-FileCopyrightText: 2026 Christian-Hauke Poensgen
# SPDX-FileCopyrightText: 2026 Maximilian Dolling
# SPDX-FileContributor: AUTHORS.md
#
# SPDX-License-Identifier: BSD-3-Clause

"""Django settings for the Celery Root web app."""

from __future__ import annotations

from pathlib import Path

from celery_root.config import FrontendConfig, McpConfig, get_settings

BASE_DIR = Path(__file__).resolve().parent
CONFIG = get_settings()
FRONTEND = CONFIG.frontend or FrontendConfig()
MCP = CONFIG.mcp or McpConfig()

SECRET_KEY = FRONTEND.secret_key
DEBUG = FRONTEND.debug
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
    "celery_root.components.web.auth.AuthMiddleware",
]

ROOT_URLCONF = "celery_root.components.web.urls"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [str(BASE_DIR / "templates")],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.debug",
                "django.template.context_processors.request",
                "celery_root.components.web.context_processors.component_context",
            ],
        },
    },
]

WSGI_APPLICATION = "celery_root.components.web.wsgi.application"
ASGI_APPLICATION = "celery_root.components.web.asgi.application"

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

CELERY_ROOT_DB_PATH = Path(CONFIG.database.db_path)
CELERY_ROOT_LOG_DIR = Path(CONFIG.logging.log_dir)
CELERY_ROOT_RETENTION_DAYS = int(CONFIG.database.retention_days)


CELERY_ROOT_WORKERS = list(CONFIG.worker_import_paths)

CELERY_ROOT_BASIC_AUTH = FRONTEND.basic_auth
CELERY_ROOT_AUTH_PROVIDER = FRONTEND.auth_provider
CELERY_ROOT_AUTH = FRONTEND.auth
CELERY_ROOT_OAUTH2_KEY = FRONTEND.oauth2_key
CELERY_ROOT_OAUTH2_SECRET = FRONTEND.oauth2_secret
CELERY_ROOT_OAUTH2_REDIRECT_URI = FRONTEND.oauth2_redirect_uri
CELERY_ROOT_OAUTH2_OKTA_BASE_URL = FRONTEND.oauth2_okta_base_url
CELERY_ROOT_GITLAB_AUTH_ALLOWED_GROUPS = FRONTEND.gitlab_allowed_groups
CELERY_ROOT_GITLAB_MIN_ACCESS_LEVEL = FRONTEND.gitlab_min_access_level
CELERY_ROOT_GITLAB_OAUTH_DOMAIN = FRONTEND.gitlab_oauth_domain

CELERY_ROOT_MCP_ENABLED = CONFIG.mcp is not None
CELERY_ROOT_MCP_HOST = MCP.host
CELERY_ROOT_MCP_PORT = MCP.port
CELERY_ROOT_MCP_PATH = MCP.path
CELERY_ROOT_MCP_AUTH_KEY_SET = bool(MCP.auth_key)

DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"
