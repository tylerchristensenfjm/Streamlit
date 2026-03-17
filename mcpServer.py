import os
from fastmcp import FastMCP
import json
from typing import Optional
import requests as r
import pandas as pd
import numpy as np

# Initialize FastMCP server
mcp = FastMCP("HighBond-Connector")

# HighBond Configuration (Set these as Env Vars in Horizon later)
HIGHBOND_TOKEN = os.getenv("HIGHBOND_TOKEN")
HIGHBOND_ORG_ID = os.getenv("HIGHBOND_ORG_ID")
HIGHBOND_BASE_URL = os.getenv("HIGHBOND_BASE_URL", "https://apis-us.highbond.com/v1")

HIGHBOND_API_ROOT = "https://apis-us.highbond.com"


if not HIGHBOND_TOKEN:
    raise ValueError("Missing HIGHBOND_TOKEN environment variable")

HEADERS = {
    "Authorization": f"Bearer {HIGHBOND_TOKEN}",
    "Content-Type": "application/json",
}

ISSUE_FIELDS = (
    "fields[issues]="
    "published,project,entities,title,deficiency_type,severity,"
    "remediation_status,remediation_plan,remediation_date,"
    "actual_remediation_date,closed"
)


def normalize_next_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    if url.startswith("http"):
        return url
    return f"{HIGHBOND_API_ROOT}{url}"


def fetch_all_pages(url: str) -> list[dict]:
    all_rows = []

    while url:
        response = r.get(url, headers=HEADERS, timeout=60)
        response.raise_for_status()
        payload = response.json()

        all_rows.extend(payload.get("data", []))
        url = normalize_next_url(payload.get("links", {}).get("next"))

    return all_rows


def fetch_issues() -> pd.DataFrame:
    url = f"{HIGHBOND_BASE_URL}/orgs/{HIGHBOND_ORG_ID}/issues/?{ISSUE_FIELDS}"
    raw_issues = fetch_all_pages(url)

    rows = []
    for item in raw_issues:
        issue_id = item.get("id")

        relationships = item.get("relationships", {})
        project = relationships.get("project", {})
        entities = relationships.get("entities", {})

        project_id = project.get("data", {}).get("id")

        entities_data = entities.get("data", [])
        entity_ids = []
        if isinstance(entities_data, list):
            entity_ids = [e.get("id") for e in entities_data if e.get("id")]

        attributes = item.get("attributes", {})

        rows.append(
            {
                "issue_id": issue_id,
                "issue_title": attributes.get("title"),
                "issue_type": attributes.get("deficiency_type"),
                "severity": attributes.get("severity"),
                "project_id": project_id,
                "entities_id": entity_ids,
                "published": attributes.get("published"),
                "issue_closed": attributes.get("closed"),
                "remediation_status": attributes.get("remediation_status"),
                "remediation_date": attributes.get("remediation_date"),
                "actual_remediation_date": attributes.get("actual_remediation_date"),
                "remediation_plan": attributes.get("remediation_plan"),
                "issue_url": (
                    f"activity-centers-api.highbond.com/redirect"
                    f"?target=issue&subdomain=fjmgt&project_id={project_id}&issue_id={issue_id}"
                ),
            }
        )

    return pd.DataFrame(rows)


def fetch_projects() -> pd.DataFrame:
    url = f"{HIGHBOND_BASE_URL}/orgs/{HIGHBOND_ORG_ID}/projects"
    raw_projects = fetch_all_pages(url)

    rows = []
    for item in raw_projects:
        attributes = item.get("attributes", {})

        rows.append(
            {
                "project_id": item.get("id"),
                "project_name": attributes.get("name"),
                "project_url": item.get("links", {}).get("ui"),
                "project_state": attributes.get("state"),
                "project_status": attributes.get("status"),
                "project_progress": attributes.get("progress"),
                "project_budget": attributes.get("budget"),
                "project_time_spent": attributes.get("time_spent"),
            }
        )

    return pd.DataFrame(rows)


def build_merged_df() -> pd.DataFrame:
    issues_df = fetch_issues()
    projects_df = fetch_projects()

    merged_df = pd.merge(issues_df, projects_df, on="project_id", how="inner")

    merged_df["rem_has_date"] = merged_df["remediation_date"].notna().astype(int)
    merged_df = merged_df[merged_df["project_id"] != "288433"]
    merged_df = merged_df[
        ~merged_df["project_name"].str.lower().str.startswith("tab", na=False)
    ]
    merged_df["issue_closed"] = np.where(
        merged_df["issue_closed"] == True, "Closed", "Open"
    )

    return merged_df


def df_for_ai(df: pd.DataFrame, max_rows: int = 100) -> dict:
    out = df.copy()

    # convert lists/dates/nan to model-friendly values
    for col in out.columns:
        if pd.api.types.is_datetime64_any_dtype(out[col]):
            out[col] = out[col].astype(str)

    out = out.where(pd.notna(out), None)

    return {
        "row_count": len(out),
        "columns": list(out.columns),
        "data": out.head(max_rows).to_dict(orient="records"),
    }


@mcp.tool
def get_issue_summary() -> dict:
    """
    Return a compact summary of issues and projects.
    """
    df = build_merged_df()

    return {
        "total_rows": int(len(df)),
        "open_issues": int((df["issue_closed"] == "Open").sum()),
        "closed_issues": int((df["issue_closed"] == "Closed").sum()),
        "projects": int(df["project_id"].nunique()),
        "remediation_status_counts": df["remediation_status"]
        .fillna("Missing")
        .value_counts()
        .to_dict(),
    }


@mcp.tool
def query_issues(
    project_name: Optional[str] = None,
    issue_status: Optional[str] = None,
    remediation_status: Optional[str] = None,
    max_rows: int = 50,
) -> dict:
    """
    Return filtered issue rows in AI-friendly format.
    """
    df = build_merged_df()

    if project_name:
        df = df[df["project_name"].str.contains(project_name, case=False, na=False)]

    if issue_status:
        df = df[df["issue_closed"].str.lower() == issue_status.lower()]

    if remediation_status:
        df = df[
            df["remediation_status"].fillna("").str.lower()
            == remediation_status.lower()
        ]

    return df_for_ai(df, max_rows=max_rows)


@mcp.tool
def get_project_issues(project_id: str, max_rows: int = 100) -> dict:
    """
    Return issues for a single project.
    """
    df = build_merged_df()
    df = df[df["project_id"] == project_id]

    return df_for_ai(df, max_rows=max_rows)


@mcp.tool
def get_dataframe_schema() -> dict:
    """
    Return columns and dtypes so the model knows what it can query.
    """
    df = build_merged_df()

    return {
        "columns": list(df.columns),
        "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
        "sample_rows": df_for_ai(df, max_rows=5)["data"],
    }


if __name__ == "__main__":
    mcp.run()
