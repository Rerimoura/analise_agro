import streamlit as st
import pandas as pd
import psycopg2
import plotly.express as px
import plotly.graph_objects as go
from datetime import date, timedelta

st.set_page_config(
    page_title='Agro-e | Commodities Dashboard',
    page_icon='🌾',
    layout='wide'
)

CONN_STRING = "postgresql://neondb_owner:npg_tXh95PpLqCAO@ep-patient-hill-am77ex2z-pooler.c-5.us-east-1.aws.neon.tech/neondb?sslmode=require"

@st.cache_data(ttl=3600)
def load_data():
    conn = psycopg2.connect(CONN_STRING)
    df = pd.read_sql("""
        SELECT c.nome AS commodity, c.ticker,
               f.data_ref, f.preco, f.volume
        FROM fato_preco f
        JOIN dim_commodity c ON f.id_commodity = c.id_commodity
        ORDER BY f.data_ref
    """, conn)
    conn.close()
    df['data_ref'] = pd.to_datetime(df['data_ref'])
    return df

df = load_data()

# ── Sidebar ───────────────────────────────────────────────────────
st.sidebar.image("https://img.icons8.com/color/96/wheat.png", width=60)
st.sidebar.title("🌾 Filtros")

commodities_disp = sorted(df['commodity'].unique().tolist())
commodities_sel = st.sidebar.multiselect(
    "Produto", commodities_disp, default=commodities_disp[:4]
)

data_min = df['data_ref'].min().date()
data_max = df['data_ref'].max().date()
periodo = st.sidebar.date_input(
    "Período", [data_max - timedelta(days=365), data_max],
    min_value=data_min, max_value=data_max
)

st.sidebar.markdown("---")
st.sidebar.caption("Fonte: Yahoo Finance / CBOT")
st.sidebar.caption(f"Atualizado em: {date.today().strftime('%d/%m/%Y')}")

# ── Filtrar ───────────────────────────────────────────────────────
if len(periodo) == 2:
    df_f = df[
        df['commodity'].isin(commodities_sel) &
        (df['data_ref'] >= pd.to_datetime(periodo[0])) &
        (df['data_ref'] <= pd.to_datetime(periodo[1]))
    ]
else:
    df_f = df[df['commodity'].isin(commodities_sel)]

# ── Header ────────────────────────────────────────────────────────
st.title("🌾 Painel de Commodities Agrícolas")
st.caption("Dados históricos de futuros agrícolas — Yahoo Finance / CBOT")
st.markdown("---")

# ── KPIs ─────────────────────────────────────────────────────────
k1, k2, k3, k4 = st.columns(4)
k1.metric("📦 Registros", f"{len(df_f):,}")
k2.metric("🌱 Commodities", df_f['commodity'].nunique())

if len(df_f) > 0:
    preco_atual = df_f.sort_values('data_ref').groupby('commodity')['preco'].last().mean()
    preco_ant   = df_f.sort_values('data_ref').groupby('commodity')['preco'].nth(-30).mean()
    delta_pct   = ((preco_atual - preco_ant) / preco_ant * 100) if preco_ant else 0
    k3.metric("💰 Preço Médio (USD)", f"{df_f['preco'].mean():.2f}")
    k4.metric("📈 Variação 30d", f"{delta_pct:.1f}%", delta=f"{delta_pct:.1f}%")

st.markdown("---")

# ── Aba 1: Evolução de preços ─────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📈 Evolução", "📊 Comparativo", "📦 Distribuição", "⚠️ Anomalias"])

with tab1:
    st.subheader("Evolução de Preços ao Longo do Tempo")
    fig = px.line(
        df_f.sort_values('data_ref'),
        x='data_ref', y='preco', color='commodity',
        labels={'data_ref': 'Data', 'preco': 'Preço (USD)', 'commodity': 'Commodity'},
        color_discrete_sequence=px.colors.qualitative.Set2
    )
    fig.update_layout(hovermode='x unified', legend=dict(orientation='h', y=-0.2))
    st.plotly_chart(fig, use_container_width=True)

    # Média móvel 30 dias
    st.subheader("Média Móvel 30 dias")
    fig2 = go.Figure()
    for comm in commodities_sel:
        sub = df_f[df_f['commodity'] == comm].sort_values('data_ref')
        sub['mm30'] = sub['preco'].rolling(30).mean()
        fig2.add_trace(go.Scatter(x=sub['data_ref'], y=sub['mm30'], name=comm, mode='lines'))
    fig2.update_layout(
        xaxis_title='Data', yaxis_title='Preço Médio Móvel (USD)',
        hovermode='x unified', legend=dict(orientation='h', y=-0.2)
    )
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.subheader("Preço Médio por Commodity")
    avg = df_f.groupby('commodity')['preco'].mean().reset_index()
    avg.columns = ['commodity', 'preco_medio']
    avg = avg.sort_values('preco_medio', ascending=True)
    fig3 = px.bar(
        avg, x='preco_medio', y='commodity', orientation='h',
        color='preco_medio', color_continuous_scale='Greens',
        labels={'preco_medio': 'Preço Médio (USD)', 'commodity': 'Commodity'}
    )
    st.plotly_chart(fig3, use_container_width=True)

    st.subheader("Volume Negociado (Top 5)")
    vol = df_f.groupby('commodity')['volume'].sum().reset_index()
    vol.columns = ['commodity', 'volume_total']
    vol = vol.sort_values('volume_total', ascending=False).head(5)
    fig4 = px.bar(
        vol, x='commodity', y='volume_total', color='commodity',
        color_discrete_sequence=px.colors.qualitative.Set2,
        labels={'volume_total': 'Volume Total', 'commodity': 'Commodity'}
    )
    st.plotly_chart(fig4, use_container_width=True)

with tab3:
    st.subheader("Distribuição de Preços (Boxplot)")
    fig5 = px.box(
        df_f, x='commodity', y='preco', color='commodity',
        color_discrete_sequence=px.colors.qualitative.Set2,
        labels={'preco': 'Preço (USD)', 'commodity': 'Commodity'},
        points='outliers'
    )
    st.plotly_chart(fig5, use_container_width=True)

    st.subheader("Histograma por Commodity")
    comm_hist = st.selectbox("Selecione a commodity", commodities_sel)
    sub_hist = df_f[df_f['commodity'] == comm_hist]
    fig6 = px.histogram(
        sub_hist, x='preco', nbins=40, color_discrete_sequence=['#4CAF50'],
        labels={'preco': 'Preço (USD)', 'count': 'Frequência'}
    )
    fig6.add_vline(x=sub_hist['preco'].mean(), line_dash='dash',
                   line_color='red', annotation_text=f"Média: {sub_hist['preco'].mean():.2f}")
    st.plotly_chart(fig6, use_container_width=True)

with tab4:
    st.subheader("⚠️ Registros Anômalos (3-sigma)")
    anomalias = []
    for comm in df_f['commodity'].unique():
        sub = df_f[df_f['commodity'] == comm]
        media, desvio = sub['preco'].mean(), sub['preco'].std()
        out = sub[(sub['preco'] > media + 3*desvio) | (sub['preco'] < media - 3*desvio)].copy()
        out['media'] = round(media, 2)
        out['limite_sup'] = round(media + 3*desvio, 2)
        out['limite_inf'] = round(media - 3*desvio, 2)
        anomalias.append(out)

    if anomalias:
        df_anom = pd.concat(anomalias)
        st.warning(f"{len(df_anom)} anomalias detectadas no período selecionado")
        st.dataframe(
            df_anom[['commodity','data_ref','preco','media','limite_sup','limite_inf']]
            .sort_values('data_ref', ascending=False)
            .reset_index(drop=True),
            use_container_width=True
        )
        fig7 = px.scatter(
            df_anom, x='data_ref', y='preco', color='commodity',
            symbol_sequence=['x'], size_max=12,
            labels={'data_ref': 'Data', 'preco': 'Preço (USD)'}
        )
        st.plotly_chart(fig7, use_container_width=True)
    else:
        st.success("Nenhuma anomalia no período selecionado.")