import os, json, time
import streamlit as st
import pandas as pd

from datapipe_core import (
    load_from_excel, load_from_gsheets, load_from_notion, kpis
)

st.set_page_config(page_title="Pinpon · DataPipe", layout="wide")
st.title("🧠 Pinpon · DataPipe (ExcelLink + Notion + Sheets)")

# ------------------------------
# Seguridad: Token simple
# ------------------------------
TOKEN_CONF = st.secrets.get("PIN_TOKEN", None)
with st.sidebar:
    st.header("🔐 Acceso")
    token = st.text_input("Token", type="password")
    ok = (TOKEN_CONF is None) or (token == TOKEN_CONF)
    st.caption("Configura PIN_TOKEN en Secrets para activar el control.")

if not ok:
    st.error("Token inválido. Solicita acceso o configura PIN_TOKEN en Secrets.")
    st.stop()

# ------------------------------
# Selector de fuente
# ------------------------------
src = st.radio(
    "Elige la fuente de datos:",
    ["Subir Excel/CSV", "Google Sheets (URL)", "Notion (Database)"],
    horizontal=True
)

df = None

if src == "Subir Excel/CSV":
    up = st.file_uploader("Archivo", type=["csv","xlsx"])
    if up:
        df = load_from_excel(up)

elif src == "Google Sheets (URL)":
    st.info("Pega la URL de tu Google Sheet y comparte la hoja con el correo del Service Account.")
    url = st.text_input("URL de Google Sheet", placeholder="https://docs.google.com/spreadsheets/...")
    sa_json = st.secrets.get("GOOGLE_SERVICE_ACCOUNT_JSON")
    if st.button("Cargar Sheet"):
        if not url:
            st.warning("Pega la URL del Google Sheet.")
        elif not sa_json:
            st.error("Falta GOOGLE_SERVICE_ACCOUNT_JSON en Secrets.")
        else:
            df = load_from_gsheets(url, json.loads(sa_json))

elif src == "Notion (Database)":
    st.info("Usa NOTION_API_KEY y NOTION_DB_ID en Secrets.")
    if st.button("Cargar Notion"):
        api_key = st.secrets.get("NOTION_API_KEY")
        db_id   = st.secrets.get("NOTION_DB_ID")
        if not api_key or not db_id:
            st.error("Faltan NOTION_API_KEY o NOTION_DB_ID en Secrets.")
        else:
            df = load_from_notion(db_id, api_key)

# ------------------------------
# Resultado + KPIs
# ------------------------------
if df is not None and not df.empty:
    st.success(f"Registros cargados: {len(df):,}")
    st.dataframe(df.head(100), use_container_width=True)

    # KPIs
    metrics = kpis(df)
    c1,c2,c3 = st.columns(3)
    c1.metric("Ticket promedio", f"${(metrics.get('ticket_promedio') or 0):,.2f}")
    c2.metric("Margen promedio", f"${(metrics.get('margen_promedio') or 0):,.2f}")
    c3.metric("IVA Promedio (%)", f"{(metrics.get('iva_pct_prom') or 0):,.1f}%")

    # Export
    st.download_button(
        "⬇️ Descargar CSV normalizado",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="pinpon_normalizado.csv",
        mime="text/csv",
    )
else:
    st.caption("Sube un archivo o carga desde Sheets/Notion para ver datos y KPIs.")

