import streamlit as st
import pandas as pd
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials

st.set_page_config(page_title="Pinpon · Facturación Cloud", layout="wide")
st.title("🧾 Pinpon · Facturación — Cloud (Google Sheets)")

SCOPE = ["https://www.googleapis.com/auth/spreadsheets",
         "https://www.googleapis.com/auth/drive"]

# --- Configuración desde secrets ---
SHEET_ID = st.secrets.get("SHEET_ID", "")
FACT_SHEET = st.secrets.get("FACT_SHEET", "FACTURAS")
CONT_SHEET = st.secrets.get("CONT_SHEET", "FACTURAS_PARA_CONTADOR")
SA_INFO = dict(st.secrets["gcp_service_account"])

def gs_client():
    creds = Credentials.from_service_account_info(SA_INFO, scopes=SCOPE)
    return gspread.authorize(creds)

@st.cache_data(show_spinner=False)
def load_sheet(_sheet_id, sheet_name):
    gc = gs_client()
    sh = gc.open_by_key(_sheet_id)
    try:
        ws = sh.worksheet(sheet_name)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=sheet_name, rows=2000, cols=26)
        ws.update('A1', [["ID","FECHA","PACIENTE","HOSPITAL","PROVEEDOR","CATEGORIA","CONCEPTO",
                          "COSTO_MXN","PRECIO_MXN","IVA_16","TOTAL_MXN","ESTATUS","FOLIO"]])
    df = pd.DataFrame(ws.get_all_records())
    return sh, ws, df

def save_df_to_ws(ws, df: pd.DataFrame):
    if df.empty:
        ws.update('A1', [[]]); return
    ws.resize(rows=len(df)+1, cols=len(df.columns))
    ws.update('A1', [list(df.columns)] + df.fillna("").values.tolist())

if not SHEET_ID:
    st.error("Falta SHEET_ID en secrets.")
    st.stop()

sh, ws_fact, df = load_sheet(SHEET_ID, FACT_SHEET)
st.subheader("📄 FACTURAS")
st.dataframe(df, use_container_width=True, height=360)

col1, col2, col3 = st.columns([1,1,2])

with col1:
    if st.button("📤 Exportar 'Por enviar' → hoja contador"):
        required = ["ID","FECHA","PACIENTE","HOSPITAL","PROVEEDOR","CATEGORIA","CONCEPTO"]
        missing = set(required) - set(df.columns)
        if missing:
            st.error(f"Faltan columnas: {sorted(list(missing))}")
        else:
            mask_conc = df["CONCEPTO"].astype(str).str.strip().ne("")
            invalid = df[mask_conc & df[required].isna().any(axis=1)]
            if not invalid.empty:
                st.error("Hay filas con CONCEPTO pero sin datos mínimos (ID/FECHA/PACIENTE/HOSPITAL/PROVEEDOR/CATEGORIA).")
                st.dataframe(invalid, use_container_width=True, height=200)
            else:
                out = df[df["ESTATUS"].fillna("").eq("Por enviar")].copy()
                if out.empty:
                    st.warning("No hay filas con ESTATUS = 'Por enviar'.")
                else:
                    out["FOLIO"] = ""
                    try:
                        ws_cont = sh.worksheet(CONT_SHEET)
                    except gspread.exceptions.WorksheetNotFound:
                        ws_cont = sh.add_worksheet(title=CONT_SHEET, rows=2000, cols=26)
                    save_df_to_ws(ws_cont, out)
                    st.success(f"Exportadas {len(out)} filas a '{CONT_SHEET}'.")
                    bio = BytesIO(); out.to_excel(bio, index=False); bio.seek(0)
                    st.download_button("Descargar copia para contador (.xlsx)", bio, file_name="FACTURAS_PARA_CONTADOR.xlsx")

with col2:
    if st.button("📥 Sincronizar folios desde hoja contador"):
        try:
            ws_cont = sh.worksheet(CONT_SHEET)
        except gspread.exceptions.WorksheetNotFound:
            st.error("No existe la hoja del contador aún.")
            ws_cont = None
        if ws_cont is not None:
            df_cont = pd.DataFrame(ws_cont.get_all_records())
            needed = {"ID","FOLIO","ESTATUS"}
            if not needed.issubset(set(df_cont.columns)):
                st.error(f"La hoja del contador debe tener: {sorted(list(needed))}")
            else:
                updated = 0
                base = df.copy()
                cont_map = df_cont.set_index("ID")[["FOLIO","ESTATUS"]]
                for i, row in base.iterrows():
                    idv = row.get("ID")
                    if pd.notna(idv) and idv in cont_map.index:
                        folio = cont_map.loc[idv, "FOLIO"]
                        est = cont_map.loc[idv, "ESTATUS"]
                        if pd.notna(folio) and str(folio).strip() != "": base.at[i, "FOLIO"] = folio
                        if pd.notna(est) and str(est).strip() != "":    base.at[i, "ESTATUS"] = est
                        updated += 1
                save_df_to_ws(ws_fact, base)
                st.success(f"Sincronizado. Filas afectadas: {updated}")
                st.dataframe(base, use_container_width=True, height=280)

with col3:
    st.markdown("### KPIs rápidos")
    tot_precio = df["PRECIO_MXN"].sum() if "PRECIO_MXN" in df.columns else 0
    tot_iva    = df["IVA_16"].sum()     if "IVA_16" in df.columns else 0
    tot_total  = df["TOTAL_MXN"].sum()  if "TOTAL_MXN" in df.columns else 0
    por_enviar = int((df["ESTATUS"].fillna("")=="Por enviar").sum()) if "ESTATUS" in df.columns else 0
    timbrada   = int((df["ESTATUS"].fillna("")=="Timbrada").sum())   if "ESTATUS" in df.columns else 0
    pagada     = int((df["ESTATUS"].fillna("")=="Pagada").sum())     if "ESTATUS" in df.columns else 0
    st.metric("Ingreso (PRECIO)", f"$ {tot_precio:,.0f}")
    st.metric("IVA", f"$ {tot_iva:,.0f}")
    st.metric("Total", f"$ {tot_total:,.0f}")
    st.metric("Por enviar", por_enviar); st.metric("Timbradas", timbrada); st.metric("Pagadas", pagada)

st.divider()
st.caption("Colabora con tu contador en el mismo Sheet. Exporta 'Por enviar' y sincroniza FOLIO/ESTATUS.")
