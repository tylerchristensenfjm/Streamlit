from fastmcp import FastMCP
import requests
import os

# Initialize FastMCP server
mcp = FastMCP("HighBond-Connector")

# HighBond Configuration (Set these as Env Vars in Horizon later)
HIGHBOND_TOKEN = os.getenv("HIGHBOND_TOKEN")
HIGHBOND_ORG_ID = os.getenv("HIGHBOND_ORG_ID")
HIGHBOND_BASE_URL = os.getenv("HIGHBOND_BASE_URL", "https://apis-us.highbond.com")

@mcp.tool()
def get_highbond_projects() -> str:
    """
    Fetches a list of projects from HighBond Diligent One.
    """
    if not HIGHBOND_TOKEN or not HIGHBOND_ORG_ID:
        return "Error: HighBond credentials not configured."

    url = f"{HIGHBOND_BASE_URL}/v1/orgs/{HIGHBOND_ORG_ID}/projects"
    headers = {
        "Authorization": f"Bearer {HIGHBOND_TOKEN}",
        "Content-Type": "application/vnd.api+json"
    }

    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        # Simple formatting for the AI to read
        projects = [p['attributes']['name'] for p in data.get('data', [])]
        return f"Found {len(projects)} projects: " + ", ".join(projects)
    
    except Exception as e:
        return f"Failed to reach HighBond: {str(e)}"

if __name__ == "__main__":
    mcp.run()