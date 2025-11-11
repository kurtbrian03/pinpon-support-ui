# datapipe_core.py
from __future__ import annotations
import io, json, math, requests
import pandas as pd

# ------------------------------
# Normalización de columnas
# ------------------------------
_CANON = {
    "precio_venta": ["precio_venta","precio","venta","pv","precio final","monto","importe"],
    "costo_proveedor": ["costo_proveedor","costo","cp","costo unitario","compra"],
    "iva": ["iva","impuesto","vat"],
    "total": ["total","importe_total","monto_total"]
}
def _norm_name(s: str) -> str:
    s = (s or "").strip().lower()
    for k, aliases in _CANON.items():
        if s == k or s in aliases:
            return k
    return s

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    df = df.copy()
    # Renombrar por similitud
    rename_map = {c: _norm_name(str(c)) for c in df.columns}
    df.rename(columns=rename_map, inplace=True)

    # Tipos numéricos seguros
    for col in ["precio_venta","costo_proveedor","iva","total"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Completar faltantes básicos
    if "total" not in df.columns and {"precio_venta","iva"}.issubset(df.columns):
        df["total"] = df["precio_venta"] + df["iva"]
    if "iva" not in df.columns and {"precio_venta","total"}.issubset(df.columns):
        df["iva"] = df["total"] - df["precio_venta"]

    return df

# ------------------------------
# Excel local (upload)
# ------------------------------
def load_from_excel(file) -> pd.DataFrame:
    # file puede ser csv o xlsx
    name = getattr(file, "name", "").lower()
    if name.endswith(".csv"):
        df = pd.read_csv(file)
    else:
        df = pd.read_excel(file)
    return normalize_columns(df)

# ------------------------------
# Google Sheets (Service Account)
# ------------------------------
def load_from_gsheets(sheet_url: str, sa_info: dict) -> pd.DataFrame:
    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = Credentials.from_service_account_info(sa_info, scopes=scopes)
    client = gspread.authorize(creds)

    sh = client.open_by_url(sheet_url)
    ws = sh.sheet1  # primera hoja
    values = ws.get_all_values()
    if not values:
        return pd.DataFrame()
    header, rows = values[0], values[1:]
    df = pd.DataFrame(rows, columns=header)
    return normalize_columns(df)

# ------------------------------
# Notion (Database)
# ------------------------------
def _prop_to_value(p):
    t = p.get("type")
    v = p.get(t)
    if t in ("title","rich_text"):
        return " ".join([x.get("plain_text","") for x in v]) if isinstance(v, list) else ""
    if t == "number":
        return v
    if t == "select":
        return (v or {}).get("name")
    if t == "multi_select":
        return ",".join([x.get("name","") for x in v or []])
    if t == "date":
        return (v or {}).get("start")
    if t == "checkbox":
        return bool(v)
    if t == "url":
        return v
    return v

def load_from_notion(database_id: str, api_key: str) -> pd.DataFrame:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json",
    }
    results = []
    payload = {}
    while True:
        r = requests.post(url, headers=headers, json=payload, timeout=30)
        r.raise_for_status()
        data = r.json()
        for page in data.get("results", []):
            props = page.get("properties", {})
            row = {k: _prop_to_value(v) for k, v in props.items()}
            results.append(row)
        next_cursor = data.get("next_cursor")
        if not next_cursor:
            break
        payload["start_cursor"] = next_cursor
    df = pd.DataFrame(results)
    return normalize_columns(df)

# ------------------------------
# KPIs
# ------------------------------
def kpis(df: pd.DataFrame) -> dict:
    if df is None or df.empty:
        return {}
    pv = df.get("precio_venta")
    cp = df.get("costo_proveedor")
    iva = df.get("iva")
    total = df.get("total")
    margen = None
    if pv is not None and cp is not None:
        margen = (pv - cp)
    return {
        "ticket_promedio": float(total.mean()) if total is not None else None,
        "margen_promedio": float(margen.mean()) if margen is not None else None,
        "iva_pct_prom": float(((iva / pv).replace([pd.NA, pd.NaT], 0).mean())*100) if (iva is not None and pv is not None) else None,
    }
