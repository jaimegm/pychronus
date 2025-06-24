import os

from flask_appbuilder.security.manager import AUTH_OAUTH

WTF_CSRF_ENABLED = True
AUTH_TYPE = AUTH_OAUTH
AUTH_USER_REGISTRATION = (
    True  # allow users who are not already in the FAB DB to register
)
AUTH_USER_REGISTRATION_ROLE = "Admin"  # Default New user Role when created
OAUTH_PROVIDERS = [
    {
        "name": "google",
        "token_key": "access_token",
        "icon": "fa-google",
        "whitelist": ["company.com"],
        "remote_app": {
            "api_base_url": "https://www.googleapis.com/oauth2/v2/",
            "client_kwargs": {"scope": "email profile"},
            "access_token_url": "https://accounts.google.com/o/oauth2/token",
            "authorize_url": "https://accounts.google.com/o/oauth2/auth",
            "request_token_url": None,
            "client_id": os.environ.get("AIRFLOW__GOOGLE__CLIENT_ID"),
            "client_secret": os.environ.get("AIRFLOW__GOOGLE__CLIENT_SECRET"),
        },
    }
]
