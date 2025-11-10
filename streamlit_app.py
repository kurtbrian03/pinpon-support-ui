import streamlit as st
import pandas as pd
import os

st.set_page_config(page_title="Pinpon · Soporte (MVP)", layout="wide")
st.title("✅ Pinpon · Soporte (MVP sin Docker)")

st.write("Sube un CSV o Excel con estos encabezados para KPIs:")
st.code("Precio_Venta, Costo_Proveedor, IVA, Total", language="text")

up = st.file_uploader("Archivo", type=["csv","xlsx"])

df = None
if up:
    if up.name.lower().endswith(".csv"):
        df = pd.read_csv(up)
    else:
        df = pd.read_excel(up)
    st.subheader("Vista previa")
    st.dataframe(df.head(100), use_container_width=True)
    os.makedirs("data", exist_ok=True)
    df.to_csv("data/echo.csv", index=False)
    st.toast("Guardado como data/echo.csv")

if df is not None:
    faltantes = {"Precio_Venta","Costo_Proveedor","IVA","Total"} - set(df.columns)
    if faltantes:
        st.warning(f"Faltan columnas: {', '.join(faltantes)}")
    else:
        df["Margen"] = df["Precio_Venta"] - df["Costo_Proveedor"]
        c1,c2,c3 = st.columns(3)
        c1.metric("Ticket Promedio", f"${df['Total'].mean():,.2f}")
        c2.metric("Margen Promedio", f"${df['Margen'].mean():,.2f}")
        c3.metric("IVA Promedio (%)", f"{(df['IVA']/df['Precio_Venta']).mean()*100:,.1f}%")

st.caption("MVP listo. Luego añadimos ExcelLink/DataPipe completos y seguridad con token.")
