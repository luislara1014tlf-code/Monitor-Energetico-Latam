import pandas as pd
import streamlit as st
from textblob import TextBlob

# --- CONFIGURACIÓN DE PÁGINA (Nivel Ejecutivo) ---
st.set_page_config(
    page_title="Monitor Estratégico Energético Latam",
    page_icon="⚡",
    layout="wide"
)

# --- 1. EXTRACCIÓN (Simulación NoSQL/API) ---
def extraer_datos():
    return [
        {"usuario": "User1", "comentario": "Excelente servicio, la energía es constante.", "pais": "Brasil"},
        {"usuario": "User2", "comentario": "Pésimo suministro, muchos cortes de luz.", "pais": "Venezuela"},
        {"usuario": "User3", "comentario": "El costo es aceptable pero podría mejorar.", "pais": "Brasil"},
        {"usuario": "User4", "comentario": "Increíble avance en paneles solares!", "pais": "Brasil"},
        {"usuario": "User5", "comentario": "Falta mantenimiento en las redes eléctricas.", "pais": "Venezuela"},
        {"usuario": "User6", "comentario": "Situación crítica con el racionamiento.", "pais": "Venezuela"}
    ]

# --- 2. INTELIGENCIA (Minería de Datos / Sentimientos) ---
def procesar_sentimientos(lista_datos):
    positivas = ["excelente", "bueno", "mejor", "avance", "constante", "increíble", "potencial"]
    negativas = ["pésimo", "cortes", "mantenimiento", "crítica", "racionamiento", "mal", "fallas"]

    for item in lista_datos:
        texto = item["comentario"].lower()
        score = 0.0
        for p in positivas:
            if p in texto: score += 0.4
        for n in negativas:
            if n in texto: score -= 0.4
        item["score"] = score
        item["sentimiento"] = "Positivo" if score > 0 else ("Negativo" if score < 0 else "Neutral")
    return lista_datos

# --- 3. BIG DATA (Carga Flexible de Parquet) ---
@st.cache_data
def cargar_historico_parquet():
    try:
        df_p = pd.read_parquet('dataset_limpio_final.parquet')
        cols = df_p.columns.tolist()
        
        # --- NORMALIZACIÓN DE COLUMNAS (Mapeo Inteligente) ---
        # Buscamos la columna de país
        col_pais = next((c for c in ['country_long', 'country', 'pais'] if c in cols), 'country_long')
        # Buscamos la columna de combustible (Aquí estaba el error)
        col_fuel = next((c for c in ['primary_fuel', 'fuel', 'fuel1', 'type'] if c in cols), 'primary_fuel')
        # Buscamos la capacidad
        col_cap = next((c for c in ['capacity_mw', 'capacity', 'mw'] if c in cols), 'capacity_mw')
        
        # Renombramos para que el código sea estándar
        df_p = df_p.rename(columns={col_pais: 'country_long', col_fuel: 'primary_fuel', col_cap: 'capacity_mw'})
        return df_p[['country_long', 'capacity_mw', 'primary_fuel']]
    except Exception as e:
        st.sidebar.error(f"⚠️ Error de Infraestructura: {e}")
        return pd.DataFrame()

# --- 4. FLUJO DE DATOS PRINCIPAL ---
df_sentimientos = pd.DataFrame(procesar_sentimientos(extraer_datos()))
df_parquet = cargar_historico_parquet()

# --- 5. PANEL DE CONTROL (Interactividad) ---
st.sidebar.title("🛡️ Panel de Control")
pais_seleccionado = st.sidebar.selectbox(
    "Selecciona un País:", ["Todos", "Brasil", "Venezuela"], key="filtro_ejecutivo"
)

# Mapeo de nombres para el Parquet (Brasil -> Brazil)
mapeo = {"Brasil": "Brazil", "Venezuela": "Venezuela"}
nombre_parquet = mapeo.get(pais_seleccionado, "Todos")

# Aplicar Filtros
if pais_seleccionado != "Todos":
    df_s_filt = df_sentimientos[df_sentimientos["pais"] == pais_seleccionado]
    df_p_filt = df_parquet[df_parquet['country_long'] == nombre_parquet]
else:
    df_s_filt = df_sentimientos
    df_p_filt = df_parquet

# --- 6. DASHBOARD VISUAL (Visualización con Semáforos) ---
st.title(f"📊 Monitor Estratégico Energético: {pais_seleccionado}")

# KPIs con Deltas de color
score_avg = df_s_filt["score"].mean() if not df_s_filt.empty else 0
cap_total = df_p_filt['capacity_mw'].sum() if not df_p_filt.empty else 0

k1, k2, k3 = st.columns(3)
with k1:
    st.metric("Sentimiento Promedio", round(score_avg, 2), 
              delta="Positivo" if score_avg > 0 else "Crítico",
              delta_color="normal" if score_avg > 0 else "inverse")
with k2:
    st.metric("Capacidad Total (MW)", f"{cap_total:,.2f}", 
              delta="Estable" if cap_total > 500 else "Baja",
              delta_color="normal" if cap_total > 500 else "off")
with k3:
    st.metric("Fuentes Analizadas", len(df_s_filt))

st.divider()

col_izq, col_der = st.columns(2)
with col_izq:
    st.subheader("📈 Opinión Pública")
    if not df_s_filt.empty:
        st.bar_chart(df_s_filt["sentimiento"].value_counts(), color="#29b5e8")

with col_der:
    st.subheader("⚡ Matriz Energética (Parquet)")
    if not df_p_filt.empty and 'primary_fuel' in df_p_filt.columns:
        grafico = df_p_filt.groupby('primary_fuel')['capacity_mw'].sum()
        st.bar_chart(grafico, color="#ffaa00")
    else:
        st.warning("⚠️ No se detectó la columna de combustible en el archivo.")

# --- 7. EXPORTACIÓN ---
st.sidebar.write("---")
csv = df_s_filt.to_csv(index=False).encode('utf-8')
st.sidebar.download_button("📥 Descargar Reporte CSV", data=csv, file_name=f"reporte_{pais_seleccionado}.csv")

st.caption("🛡️ Proyecto Integrador | Big Data & Visualización de Datos")
