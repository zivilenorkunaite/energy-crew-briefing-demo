"""Configuration and authentication for EE Work Order Document Generator."""

import os
from typing import Optional

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))


def get_oauth_token() -> Optional[str]:
    """Get OAuth token - works both locally and in Databricks Apps."""
    if IS_DATABRICKS_APP:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        if w.config.token:
            return w.config.token
        auth_headers = w.config.authenticate()
        if auth_headers and "Authorization" in auth_headers:
            return auth_headers["Authorization"].replace("Bearer ", "")
        return None
    else:
        try:
            from databricks.sdk import WorkspaceClient
            profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
            w = WorkspaceClient(profile=profile)
            if w.config.token:
                return w.config.token
            auth_headers = w.config.authenticate()
            if auth_headers and "Authorization" in auth_headers:
                return auth_headers["Authorization"].replace("Bearer ", "")
        except Exception as e:
            print(f"Auth failed: {e}")
        return None


def get_workspace_host() -> str:
    """Get workspace host URL with https:// prefix."""
    if IS_DATABRICKS_APP:
        host = os.environ.get("DATABRICKS_HOST", "")
        if host and not host.startswith("http"):
            host = f"https://{host}"
        return host
    else:
        try:
            from databricks.sdk import WorkspaceClient
            profile = os.environ.get("DATABRICKS_PROFILE", "DEFAULT")
            w = WorkspaceClient(profile=profile)
            return w.config.host
        except Exception:
            return os.environ.get("DATABRICKS_HOST", "")
