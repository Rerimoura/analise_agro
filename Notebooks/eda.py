import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import psycopg2
from dotenv import load_dotenv
import os
load_dotenv()
CONN_STRING = os.getenv("CONN_STRING")

# ── 1. Carregar dados ─────────────────────────────────────────────
conn = psycopg2.connect(CONN_STRING)
df = pd.read_sql("""
    SELECT c.nome AS commodity, f.data_ref, f.preco, f.volume
    FROM fato_preco f
    JOIN dim_commodity c ON f.id_commodity = c.id_commodity
    ORDER BY f.data_ref
""", conn)
conn.close()

df['data_ref'] = pd.to_datetime(df['data_ref'])
print(f"Registros carregados: {len(df)}")

# ── 2. Estatísticas descritivas ───────────────────────────────────
print("\n=== ESTATÍSTICAS DESCRITIVAS ===")
stats = df.groupby('commodity')['preco'].agg(
    media='mean',
    mediana='median',
    desvio_padrao='std',
    minimo='min',
    maximo='max',
    q1=lambda x: x.quantile(0.25),
    q3=lambda x: x.quantile(0.75)
).round(2)
print(stats)
stats.to_csv('docs/q8_estatisticas.csv')

# ── 3. Detecção de outliers (IQR) ─────────────────────────────────
print("\n=== OUTLIERS DETECTADOS (IQR) ===")
outliers_list = []
for comm in df['commodity'].unique():
    sub = df[df['commodity'] == comm]['preco']
    Q1, Q3 = sub.quantile(0.25), sub.quantile(0.75)
    IQR = Q3 - Q1
    out = df[(df['commodity'] == comm) & 
             ((df['preco'] < Q1 - 1.5*IQR) | (df['preco'] > Q3 + 1.5*IQR))]
    outliers_list.append(out)
    print(f"  {comm}: {len(out)} outliers")

outliers_df = pd.concat(outliers_list)

# ── 4. GRÁFICO 1: Boxplot por commodity ──────────────────────────
fig, ax = plt.subplots(figsize=(14, 6))
commodities = df['commodity'].unique()
dados_box = [df[df['commodity'] == c]['preco'].values for c in commodities]
bp = ax.boxplot(dados_box, labels=commodities, patch_artist=True, notch=False)
colors = ['#2196F3','#4CAF50','#FF9800','#E91E63','#9C27B0','#00BCD4','#FF5722','#795548']
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.7)
ax.set_title('Distribuição de Preços por Commodity (Boxplot)', fontsize=14, fontweight='bold')
ax.set_xlabel('Commodity')
ax.set_ylabel('Preço (USD)')
ax.grid(axis='y', alpha=0.3)
plt.xticks(rotation=15)
plt.tight_layout()
plt.savefig('docs/q8_boxplot.png', dpi=150, bbox_inches='tight')
plt.show()
print("Salvo: q8_boxplot.png")

# ── 5. GRÁFICO 2: Histograma de preços normalizados ───────────────
fig, axes = plt.subplots(2, 4, figsize=(16, 8))
axes = axes.flatten()
for i, comm in enumerate(commodities):
    sub = df[df['commodity'] == comm]['preco']
    axes[i].hist(sub, bins=30, color=colors[i], alpha=0.7, edgecolor='white')
    axes[i].set_title(comm.upper(), fontweight='bold')
    axes[i].set_xlabel('Preço (USD)')
    axes[i].set_ylabel('Frequência')
    axes[i].axvline(sub.mean(), color='red', linestyle='--', linewidth=1.5, label=f'Média: {sub.mean():.1f}')
    axes[i].legend(fontsize=8)
    axes[i].grid(alpha=0.3)
plt.suptitle('Histograma de Preços por Commodity', fontsize=14, fontweight='bold')
plt.tight_layout()
plt.savefig('docs/q8_histogramas.png', dpi=150, bbox_inches='tight')
plt.show()
print("Salvo: q8_histogramas.png")

# ── 6. GRÁFICO 3: Scatter — evolução temporal ─────────────────────
fig, ax = plt.subplots(figsize=(16, 7))
for i, comm in enumerate(commodities):
    sub = df[df['commodity'] == comm].sort_values('data_ref')
    # Normalizar preços (0-100) para comparar escala
    preco_norm = (sub['preco'] - sub['preco'].min()) / (sub['preco'].max() - sub['preco'].min()) * 100
    ax.plot(sub['data_ref'], preco_norm, label=comm, color=colors[i], linewidth=1.5, alpha=0.8)

ax.set_title('Evolução Normalizada de Preços (0-100) por Commodity', fontsize=14, fontweight='bold')
ax.set_xlabel('Data')
ax.set_ylabel('Preço Normalizado')
ax.legend(loc='upper left', ncol=2)
ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig('docs/q8_scatter_temporal.png', dpi=150, bbox_inches='tight')
plt.show()
print("Salvo: q8_scatter_temporal.png")

print("\n=== EDA concluída ===")
print(f"Total de outliers detectados: {len(outliers_df)}")