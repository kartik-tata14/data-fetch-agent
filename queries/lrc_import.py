"""
LoadRunner Cloud integration – extract Job GUIDs from transaction summary.

API workflow:
1. Authenticate with Client ID + Client Secret → get session token
2. Fetch transaction summary for a specific Run ID
3. Filter transactions matching "GDC Details Debug: Captured JOBGUID_..."
4. Extract Job GUIDs from matching transaction names
"""

import re
import requests

_JOBGUID_PATTERN = re.compile(r"JOBGUID_([0-9a-fA-F\-]{36})")


def authenticate(base_url: str, client_id: str, client_secret: str, tenant_id: str) -> str:
    """Authenticate to LRC and return session token."""
    url = f"{base_url.rstrip('/')}/v1/auth-client"
    params = {"TENANTID": str(tenant_id)} if tenant_id else {}
    payload = {"client_id": client_id, "client_secret": client_secret}
    headers = {"Content-Type": "application/json"}
    resp = requests.post(url, json=payload, headers=headers, params=params, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("token")
    if not token:
        raise ValueError("Authentication succeeded but no token returned.")
    return token


def fetch_transaction_summary(
    base_url: str,
    token: str,
    tenant_id: str,
    project_id: str,
    run_id: str,
) -> list[dict]:
    """Fetch transaction summary for a given run."""
    url = f"{base_url.rstrip('/')}/v1/test-runs/{run_id}/transactions"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    params = {"TENANTID": str(tenant_id)}
    resp = requests.get(url, headers=headers, params=params, timeout=60)
    resp.raise_for_status()
    return resp.json()


def extract_job_guids(
    transactions: list[dict],
    filter_text: str = "GDC Details Debug: Captured JOBGUID_",
) -> list[str]:
    """
    Filter transactions by name containing filter_text,
    then extract Job GUIDs from the transaction name.

    Sample name:
      "GDC Details Debug: Captured JOBGUID_b614ff02-d2c9-4056-9acd-8bcbbf0474d2,
       Client_6437a817-06f5-4f95-ab7d-964cf90c3f4c"
    """
    guids = []
    for txn in transactions:
        name = txn.get("name", "") or txn.get("transactionName", "")
        if filter_text in name:
            match = _JOBGUID_PATTERN.search(name)
            if match:
                guid = match.group(1)
                if guid not in guids:
                    guids.append(guid)
    return guids


def import_guids_from_lrc(
    base_url: str,
    client_id: str,
    client_secret: str,
    tenant_id: str,
    project_id: str,
    run_id: str,
    filter_text: str = "GDC Details Debug: Captured JOBGUID_",
) -> tuple[list[str], list[dict]]:
    """
    End-to-end: authenticate → fetch transactions → extract GUIDs.
    Returns (list_of_guids, raw_transactions).
    """
    token = authenticate(base_url, client_id, client_secret, tenant_id)
    transactions = fetch_transaction_summary(
        base_url, token, tenant_id, project_id, run_id
    )
    guids = extract_job_guids(transactions, filter_text)
    return guids, transactions
