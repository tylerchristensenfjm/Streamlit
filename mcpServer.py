import os
from fastmcp import FastMCP
import json
from typing import Optional
import requests as r
import pandas as pd
import numpy as np
from functools import lru_cache
from starlette.responses import PlainTextResponse
from starlette.routing import Route

# Initialize FastMCP server
mcp = FastMCP("HighBond-Connector")

# HighBond Configuration (Set these as Env Vars in Horizon later)
HIGHBOND_TOKEN = os.getenv("HIGHBOND_TOKEN")
HIGHBOND_ORG_ID = os.getenv("HIGHBOND_ORG_ID")
HIGHBOND_BASE_URL = os.getenv("HIGHBOND_BASE_URL", "https://apis-us.highbond.com/v1")
OPENAI_APPS_CHALLENGE_TOKEN = os.getenv("OPENAI_APPS_CHALLENGE_TOKEN")

HIGHBOND_API_ROOT = "https://apis-us.highbond.com"

if not HIGHBOND_TOKEN:
    raise ValueError("HIGHBOND_TOKEN environment variable is required")
HEADERS = {
    "Authorization": f"Bearer {HIGHBOND_TOKEN}",
    "Content-Type": "application/json",
}

ISSUE_FIELDS = (
    "fields[issues]="
    "created_at,updated_at,published,project,entities,title,description,"
    "owner,executive_owner,project_owner,deficiency_type,severity,"
    "remediation_status,remediation_plan,remediation_date,"
    "actual_remediation_date,closed,recommendation"
)


def normalize_next_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if url.startswith("http"):
        return url
    return f"{HIGHBOND_API_ROOT}{url}"


def fetch_all_pages(url: str) -> list[dict]:
    rows = []

    while url:
        response = r.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        payload = response.json()

        rows.extend(payload.get("data", []))
        url = normalize_next_url(payload.get("links", {}).get("next"))

    return rows


def fetch_issues_df() -> pd.DataFrame:
    issue_url = f"{HIGHBOND_BASE_URL}/orgs/{HIGHBOND_ORG_ID}/issues/?{ISSUE_FIELDS}"
    raw = fetch_all_pages(issue_url)

    rows = []
    for item in raw:
        relationships = item.get("relationships", {})
        project = relationships.get("project", {})
        entities = relationships.get("entities", {})

        project_id = project.get("data", {}).get("id")

        entities_data = entities.get("data", [])
        entity_ids = []
        if isinstance(entities_data, list):
            entity_ids = [x.get("id") for x in entities_data if x.get("id")]

        attr = item.get("attributes", {})

        rows.append(
            {
                "issue_id": item.get("id"),
                "issue_title": attr.get("title"),
                "issue_description": attr.get("description"),
                "issue_owner": attr.get("owner"),
                "issue_executive_owner": attr.get("executive_owner"),
                "issue_project_owner": attr.get("project_owner"),
                "issue_type": attr.get("deficiency_type"),
                "severity": attr.get("severity"),
                "project_id": project_id,
                "entities_id": entity_ids,
                "created_at": attr.get("created_at"),
                "updated_at": attr.get("updated_at"),
                "published": attr.get("published"),
                "issue_closed": attr.get("closed"),
                "remediation_status": attr.get("remediation_status"),
                "remediation_date": attr.get("remediation_date"),
                "actual_remediation_date": attr.get("actual_remediation_date"),
                "remediation_plan": attr.get("remediation_plan"),
                "recommendation": attr.get("recommendation"),
                "issue_url": (
                    f"activity-centers-api.highbond.com/redirect"
                    f"?target=issue&subdomain=fjmgt&project_id={project_id}&issue_id={item.get('id')}"
                ),
            }
        )

    return pd.DataFrame(rows)


def fetch_projects_df() -> pd.DataFrame:
    project_url = f"{HIGHBOND_BASE_URL}/orgs/{HIGHBOND_ORG_ID}/projects"
    raw = fetch_all_pages(project_url)

    rows = []
    for item in raw:
        attr = item.get("attributes", {})

        rows.append(
            {
                "project_id": item.get("id"),
                "project_name": attr.get("name"),
                "project_url": item.get("links", {}).get("ui"),
                "project_state": attr.get("state"),
                "project_status": attr.get("status"),
                "project_progress": attr.get("progress"),
                "project_budget": attr.get("budget"),
                "project_time_spent": attr.get("time_spent"),
            }
        )

    return pd.DataFrame(rows)


def build_merged_df() -> pd.DataFrame:
    issues_df = fetch_issues_df()
    projects_df = fetch_projects_df()

    df = pd.merge(issues_df, projects_df, on="project_id", how="inner")

    # Convert date fields
    for col in ["created_at", "updated_at", "published", "remediation_date", "actual_remediation_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df["rem_has_date"] = df["remediation_date"].notna().astype(int)

    # Business filters
    df = df[df["project_id"] != "288433"]
    df = df[~df["project_name"].fillna("").str.lower().str.startswith("tab", na=False)]

    # Normalize issue status
    df["issue_closed"] = np.where(df["issue_closed"] == True, "Closed", "Open")

    return df


@lru_cache(maxsize=1)
def get_cached_df() -> pd.DataFrame:
    return build_merged_df()


def get_df() -> pd.DataFrame:
    return get_cached_df().copy()


def sort_df(
    df: pd.DataFrame,
    sort_by: str = "created_at",
    sort_desc: bool = True,
) -> pd.DataFrame:
    allowed_sort_cols = {
        "created_at",
        "updated_at",
        "published",
        "remediation_date",
        "actual_remediation_date",
        "issue_title",
        "project_name",
        "severity",
        "issue_status",
        "remediation_status",
    }

    normalized_sort_by = "issue_closed" if sort_by == "issue_status" else sort_by

    if normalized_sort_by not in df.columns or sort_by not in allowed_sort_cols:
        normalized_sort_by = "created_at"

    return df.sort_values(
        by=normalized_sort_by,
        ascending=not sort_desc,
        na_position="last",
        kind="stable",
    )


def serialize_df(df: pd.DataFrame, max_rows: int = 50) -> dict:
    out = df.copy()

    # Convert timestamps to strings for cleaner MCP responses
    datetime_cols = out.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns
    for col in datetime_cols:
        out[col] = out[col].astype(str)

    out = out.where(pd.notna(out), None)

    return {
        "row_count": int(len(out)),
        "columns": list(out.columns),
        "data": out.head(max_rows).to_dict(orient="records"),
    }


@mcp.tool
def refresh_data() -> dict:
    """
    Refresh the HighBond issues dataframe from the API.
    """
    get_cached_df.cache_clear()
    df = get_df()
    return {
        "status": "ok",
        "rows": int(len(df)),
        "columns": list(df.columns),
    }


@mcp.tool
def search_issues(
    title_contains: Optional[str] = None,
    project_name: Optional[str] = None,
    issue_status: Optional[str] = None,
    issue_owner: Optional[str] = None,
    issue_description: Optional[str] = None,
    issue_executive_owner: Optional[str] = None,
    issue_project_owner: Optional[str] = None,
    remediation_status: Optional[str] = None,
    severity: Optional[str] = None,
    issue_type: Optional[str] = None,
    has_remediation_date: Optional[bool] = None,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    max_rows: int = 500,
) -> dict:
    """
    Search issues using business-friendly filters.

    sort_by options:
    - created_at
    - updated_at
    - published
    - remediation_date
    - actual_remediation_date
    - issue_title
    - project_name
    - severity
    - issue_status
    - remediation_status
    """
    df = get_df()

    if title_contains:
        df = df[df["issue_title"].fillna("").str.contains(title_contains, case=False, na=False)]

    if issue_owner:
        df = df[df["issue_owner"].fillna("").str.contains(issue_owner, case=False, na=False)]

    if issue_description:
        df = df[df["issue_description"].fillna("").str.contains(issue_description, case=False, na=False)]

    if issue_executive_owner:
        df = df[
            df["issue_executive_owner"].fillna("").str.contains(
                issue_executive_owner, case=False, na=False
            )
        ]

    if issue_project_owner:
        df = df[
            df["issue_project_owner"].fillna("").str.contains(
                issue_project_owner, case=False, na=False
            )
        ]

    if project_name:
        df = df[df["project_name"].fillna("").str.contains(project_name, case=False, na=False)]

    if issue_status:
        df = df[df["issue_closed"].fillna("").str.lower() == issue_status.lower()]

    if remediation_status:
        df = df[
            df["remediation_status"].fillna("").str.contains(remediation_status, case=False, na=False)
        ]

    if severity:
        df = df[df["severity"].fillna("").str.contains(severity, case=False, na=False)]

    if issue_type:
        df = df[df["issue_type"].fillna("").str.contains(issue_type, case=False, na=False)]

    if has_remediation_date is not None:
        if has_remediation_date:
            df = df[df["remediation_date"].notna()]
        else:
            df = df[df["remediation_date"].isna()]

    df = sort_df(df, sort_by=sort_by, sort_desc=sort_desc)

    return serialize_df(df, max_rows=max_rows)


@mcp.tool
def get_recent_issues(max_rows: int = 5) -> dict:
    """
    Return the most recently created issues.
    """
    df = get_df()
    df = sort_df(df, sort_by="created_at", sort_desc=True)
    return serialize_df(df, max_rows=max_rows)


@mcp.tool
def get_recently_updated_issues(max_rows: int = 5) -> dict:
    """
    Return the most recently updated issues.
    """
    df = get_df()
    df = sort_df(df, sort_by="updated_at", sort_desc=True)
    return serialize_df(df, max_rows=max_rows)


@mcp.tool
def get_issue_summary() -> dict:
    """
    Return overall counts for issues.
    """
    df = get_df()
    today = pd.Timestamp.today().normalize()
    rem_dates = pd.to_datetime(df["remediation_date"], errors="coerce")

    overdue_mask = (
        (df["issue_closed"] == "Open")
        & rem_dates.notna()
        & (rem_dates < today)
    )

    return {
        "total_issues": int(len(df)),
        "open_issues": int((df["issue_closed"] == "Open").sum()),
        "closed_issues": int((df["issue_closed"] == "Closed").sum()),
        "high_severity_issues": int(df["severity"].fillna("").str.contains("high", case=False).sum()),
        "issues_missing_remediation_date": int(df["remediation_date"].isna().sum()),
        "overdue_open_issues": int(overdue_mask.sum()),
    }


@mcp.tool
def get_overdue_issues(
    project_name: Optional[str] = None,
    severity: Optional[str] = None,
    sort_by: str = "remediation_date",
    sort_desc: bool = False,
    max_rows: int = 50,
) -> dict:
    """
    Return open issues with remediation dates earlier than today.
    """
    df = get_df()

    rem_dates = pd.to_datetime(df["remediation_date"], errors="coerce")
    today = pd.Timestamp.today().normalize()

    mask = (
        (df["issue_closed"] == "Open")
        & rem_dates.notna()
        & (rem_dates < today)
    )
    df = df[mask].copy()

    if project_name:
        df = df[df["project_name"].fillna("").str.contains(project_name, case=False, na=False)]

    if severity:
        df = df[df["severity"].fillna("").str.contains(severity, case=False, na=False)]

    df = sort_df(df, sort_by=sort_by, sort_desc=sort_desc)

    return serialize_df(df, max_rows=max_rows)


@mcp.tool
def get_issues_missing_remediation_date(
    project_name: Optional[str] = None,
    issue_status: Optional[str] = None,
    sort_by: str = "created_at",
    sort_desc: bool = True,
    max_rows: int = 50,
) -> dict:
    """
    Return issues where remediation date is missing.
    """
    df = get_df()
    df = df[df["remediation_date"].isna()].copy()

    if project_name:
        df = df[df["project_name"].fillna("").str.contains(project_name, case=False, na=False)]

    if issue_status:
        df = df[df["issue_closed"].fillna("").str.lower() == issue_status.lower()]

    df = sort_df(df, sort_by=sort_by, sort_desc=sort_desc)

    return serialize_df(df, max_rows=max_rows)


@mcp.tool
def get_project_summary(
    project_name: Optional[str] = None,
    max_rows: int = 100,
) -> dict:
    """
    Return issue counts grouped by project name.
    """
    df = get_df()

    if project_name:
        df = df[df["project_name"].fillna("").str.contains(project_name, case=False, na=False)]

    summary = (
        df.groupby("project_name", dropna=False)
        .agg(
            total_issues=("issue_title", "count"),
            open_issues=("issue_closed", lambda s: int((s == "Open").sum())),
            closed_issues=("issue_closed", lambda s: int((s == "Closed").sum())),
            missing_remediation_date=("remediation_date", lambda s: int(s.isna().sum())),
            latest_created_at=("created_at", "max"),
            latest_updated_at=("updated_at", "max"),
        )
        .reset_index()
        .sort_values(["open_issues", "total_issues"], ascending=[False, False])
    )

    return serialize_df(summary, max_rows=max_rows)


@mcp.tool
def list_filter_values() -> dict:
    """
    Return common values the assistant can use for filtering.
    """
    df = get_df()

    def clean_unique(series: pd.Series) -> list[str]:
        vals = series.dropna().astype(str).str.strip()
        vals = vals[vals != ""]
        return sorted(vals.unique().tolist())

    return {
        "issue_status_values": clean_unique(df["issue_closed"]),
        "severity_values": clean_unique(df["severity"]),
        "issue_type_values": clean_unique(df["issue_type"]),
        "remediation_status_values": clean_unique(df["remediation_status"]),
        "issue_owner_values": clean_unique(df["issue_owner"])[:100],
        "issue_executive_owner_values": clean_unique(df["issue_executive_owner"])[:100],
        "issue_project_owner_values": clean_unique(df["issue_project_owner"])[:100],
        "sample_project_names": clean_unique(df["project_name"])[:100],
    }

async def openai_apps_challenge(request):
    if not OPENAI_APPS_CHALLENGE_TOKEN:
        return PlainTextResponse("Challenge token not configured", status_code=500)
    return PlainTextResponse(OPENAI_APPS_CHALLENGE_TOKEN)

app = mcp.http_app(
    path="/mcp/",
    routes=[
        Route("/.well-known/openai-apps-challenge", openai_apps_challenge),
    ],
)

if __name__ == "__main__":
    mcp.run(transport="http")


