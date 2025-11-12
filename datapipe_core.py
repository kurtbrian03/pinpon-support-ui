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

# ==== Google Sheets Bridge (Facturación Cloud) ====
from typing import List, Dict, Tuple
import os
import pandas as pd

REQUIRED_COLS = ["ID","FECHA","PACIENTE","HOSPITAL","PROVEEDOR","CATEGORIA","CONCEPTO"]

def _get_gs_config():
    """Lee credenciales y nombres de hojas desde st.secrets (Streamlit) o variables de entorno."""
    try:
        import streamlit as st  # opcional
        secrets = getattr(st, "secrets", {})
        SA_INFO = dict(secrets["gcp_service_account"])
        SHEET_ID = secrets.get("SHEET_ID", "")
        FACT_SHEET = secrets.get("FACT_SHEET", "FACTURAS")
        CONT_SHEET = secrets.get("CONT_SHEET", "FACTURAS_PARA_CONTADOR")
    except Exception:
        import json
        SA_INFO = json.loads(os.environ["GCP_SERVICE_ACCOUNT_JSON"])
        SHEET_ID = os.environ["SHEET_ID"]
        FACT_SHEET = os.environ.get("FACT_SHEET", "FACTURAS")
        CONT_SHEET = os.environ.get("CONT_SHEET", "FACTURAS_PARA_CONTADOR")
    return SA_INFO, SHEET_ID, FACT_SHEET, CONT_SHEET

def _gs_client():
    import gspread
    from google.oauth2.service_account import Credentials
    SA_INFO, *_ = _get_gs_config()
    scope = ["https://www.googleapis.com/auth/spreadsheets",
             "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(SA_INFO, scopes=scope)
    return gspread.authorize(creds)

def _open_ws(sheet_name: str):
    gc = _gs_client()
    _, SHEET_ID, _, _ = _get_gs_config()
    sh = gc.open_by_key(SHEET_ID)
    try:
        ws = sh.worksheet(sheet_name)
    except Exception:
        ws = sh.add_worksheet(title=sheet_name, rows=2000, cols=26)
    return sh, ws

def gs_read(sheet_name: str) -> pd.DataFrame:
    """Lee una hoja de Google Sheets como DataFrame."""
    _, ws = _open_ws(sheet_name)
    rows = ws.get_all_records()
    return pd.DataFrame(rows)

def gs_write(sheet_name: str, df: pd.DataFrame):
    """Escribe un DataFrame completo (sobrescribe contenido)."""
    sh, ws = _open_ws(sheet_name)
    if df.empty:
        ws.clear(); ws.update('A1', [[]]); return
    ws.resize(rows=len(df)+1, cols=len(df.columns))
    ws.update('A1', [list(df.columns)] + df.fillna("").values.tolist())

def validate_facturas(df: pd.DataFrame) -> Tuple[bool, pd.DataFrame]:
    """Si CONCEPTO no está vacío, no pueden faltar columnas requeridas."""
    if df.empty: return True, df
    missing_cols = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Faltan columnas requeridas: {missing_cols}")
    mask_conc = df["CONCEPTO"].astype(str).str.strip().ne("")
    invalid = df[mask_conc & df[REQUIRED_COLS].isna().any(axis=1)]
    return invalid.empty, invalid

def facturas_read() -> pd.DataFrame:
    """Lee FACTURAS y asegura columnas base."""
    _, _, FACT_SHEET, _ = _get_gs_config()
    df = gs_read(FACT_SHEET)
    base_cols = ["ID","FECHA","PACIENTE","HOSPITAL","PROVEEDOR","CATEGORIA","CONCEPTO",
                 "COSTO_MXN","PRECIO_MXN","IVA_16","TOTAL_MXN","ESTATUS","FOLIO"]
    for c in base_cols:
        if c not in df.columns:
            df[c] = 0 if c in ["COSTO_MXN","PRECIO_MXN","IVA_16","TOTAL_MXN"] else ""
    return df[base_cols]

def facturas_upsert(rows: List[Dict]) -> pd.DataFrame:
    """Upsert por 'ID': si existe actualiza; si no, inserta."""
    df = facturas_read()
    if "ID" not in df.columns: df["ID"] = ""
    idx = {str(r["ID"]): i for i, r in df.iterrows() if pd.notna(r.get("ID")) and str(r["ID"]).strip()}
    for r in rows:
        rid = str(r.get("ID","")).strip()
        if not rid: continue
        if rid in idx:
            i = idx[rid]
            for k, v in r.items():
                if k in df.columns:
                    df.at[i, k] = v
        else:
            new = {c: r.get(c, "") for c in df.columns}
            df.loc[len(df)] = new
            idx[rid] = len(df) - 1
    ok, invalid = validate_facturas(df)
    if not ok:
        raise ValueError(f"Filas inválidas, corrige antes de guardar:\n{invalid}")
    _, _, FACT_SHEET, _ = _get_gs_config()
    gs_write(FACT_SHEET, df)
    return df

def export_por_enviar_to_contador() -> pd.DataFrame:
    """Pasa ESTATUS='Por enviar' → hoja del contador con FOLIO vacío."""
    df = facturas_read()
    out = df[df["ESTATUS"].fillna("").eq("Por enviar")].copy()
    if out.empty: return out
    out["FOLIO"] = out.get("FOLIO","")
    _, _, _, CONT_SHEET = _get_gs_config()
    gs_write(CONT_SHEET, out)
    return out

def sync_folios_from_contador() -> pd.DataFrame:
    """Trae FOLIO/ESTATUS desde la hoja del contador y actualiza FACTURAS por ID."""
    _, _, FACT_SHEET, CONT_SHEET = _get_gs_config()
    base = gs_read(FACT_SHEET)
    cont = gs_read(CONT_SHEET)
    if base.empty or cont.empty: return base
    required = {"ID","FOLIO","ESTATUS"}
    if not required.issubset(cont.columns):
        raise ValueError("La hoja del contador debe tener: ID, FOLIO, ESTATUS")
    cont_map = cont.set_index("ID")[["FOLIO","ESTATUS"]]
    for i, row in base.iterrows():
        idv = row.get("ID")
        if pd.notna(idv) and str(idv) in cont_map.index:
            folio, est = cont_map.loc[str(idv), ["FOLIO","ESTATUS"]]
            if pd.notna(folio) and str(folio).strip(): base.at[i, "FOLIO"] = folio
            if pd.notna(est) and str(est).strip():     base.at[i, "ESTATUS"] = est
    gs_write(FACT_SHEET, base)
    return base

