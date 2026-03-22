# 🌾 Agro-e | Avaliação Técnica — Analista de Dados (Webscraping)

## Visão Geral
Pipeline completo de dados de commodities agrícolas: coleta via Yahoo Finance,
ETL com Python, armazenamento em PostgreSQL (Neon), análise exploratória e
dashboard interativo com Streamlit.

**Fonte:** Yahoo Finance / CBOT (futuros agrícolas)  
**Commodities:** soja, milho, trigo, café, algodão, açúcar, boi gordo, cacau  
**Período:** 2 anos de histórico diário (~502 registros por commodity)  
**Total:** 4.020 registros

---

## Estrutura do Projeto
```
agro-e-analise/
├── data/
│   ├── raw/                  # CSVs brutos do scraper
│   ├── processed/            # Parquet após ETL
│   └── curated/              # Exportações para análise
├── scraper/
│   └── cepea_scraper.py      # Q1 — Coleta Yahoo Finance
├── etl/
│   └── transform_load.py     # Q4 — ETL
├── sql/
│   ├── 01_create_tables.sql  # Q3 — DDL
│   ├── 03_queries_analytics.sql # Q6 — Análises
│   └── 04_indexes.sql        # Q7 — Otimização
├── notebooks/
│   └── eda.py                # Q8 — EDA Pandas/Matplotlib
├── app/
│   └── streamlit_app.py      # Q9 — Dashboard
├── tests/
│   └── test_etl.py           # Testes unitários
└── docs/
    └── prints/               # Screenshots de todas as etapas
```

---

## Q1 — Coleta de Dados (Webscraping)

**Fonte escolhida:** Yahoo Finance via biblioteca `yfinance`

**Tentativas e decisão técnica:**
- **CEPEA/ESALQ:** retornou erro 403 (proteção Cloudflare/WAF) — bloqueio de scraping direto
- **BCB/SGS:** timeout persistente por instabilidade do servidor na data da coleta
- **Yahoo Finance:** API pública estável, sem bloqueio, dados reais de futuros CBOT

**Desafios encontrados e resolvidos:**
- Colunas em MultiIndex dependendo da versão do yfinance (resolvido com flatten)
- Dados em USD (futuros internacionais) — documentado nas análises
- Volume negociado disponível (permite Q6b — ranking por liquidez)
```bash
pip install yfinance pandas
python scraper/cepea_scraper.py
```

---

## Q2 — Camada Raw

**Formato escolhido:** CSV para raw (legível, inspecionável), Parquet para processed/curated.

| Formato | Vantagens | Uso |
|---------|-----------|-----|
| CSV | Legível, compatível com Excel | Raw |
| JSON | Flexível, ideal para aninhados | APIs |
| Parquet | Compressão 5-10x, tipagem forte | Processed/Curated |

**Equivalente AWS S3:**
```
s3://agro-e-datalake/
    raw/yahoo_finance/2026/03/commodities_2026-03-22.csv
    processed/precos/2026/03/precos_2026-03-22.parquet
    curated/dashboard/precos_mensais.parquet
```

---

## Q3 — Tabelas PostgreSQL (Neon)

Modelo estrela normalizado (3FN):
- `dim_commodity` — 8 commodities com ticker e unidade
- `dim_regiao` — mercado de referência (Chicago/CBOT)
- `fato_preco` — 4.020 registros com constraint UNIQUE para idempotência

**Prints:** `docs/prints/q3_tabelas_populadas.png`

---

## Q4 — ETL

Pipeline em 3 etapas:
1. **Extract:** lê todos os CSVs de `data/raw/`
2. **Transform:** padroniza commodity (lowercase), converte datas, detecta anomalias (3-sigma)
3. **Load:** inserção em lote com `execute_values` — 3 queries ao banco (vs 16.000 em loop)

**Resultado:** 4.020 registros | 27 anomalias detectadas | 0 nulos
```bash
pip install psycopg2-binary pandas pyarrow
python etl/transform_load.py
```

---

## Q5 — Data Lake

| Camada | Conteúdo | Formato |
|--------|----------|---------|
| raw | Dados brutos, imutáveis | CSV |
| processed | Dados limpos, tipados | Parquet |
| curated | Agregações para BI | Parquet / Views PG |

---

## Q6 — Análises SQL

### a) Preço médio mensal com variação % (LAG)
Açúcar variou -6,81% em abril/2024. Café mostrou maior volatilidade mensal.  
**Print:** `docs/prints/q6a_lag.png`

### b) Top 5 mais negociados (último ano)
1. Milho — 37.2M de volume
2. Soja — 22.4M
3. Açúcar — 15.2M
4. Trigo — 13.1M
5. Boi Gordo — 3.9M

**Print:** `docs/prints/q6b_top5.png`

### c) Anomalias detectadas
27 registros fora do intervalo ±3-sigma — principalmente algodão e trigo.  
**Print:** `docs/prints/q6c_anomalias.png`

---

## Q7 — Otimização e Indexação
```sql
CREATE INDEX idx_fato_data      ON fato_preco(data_ref DESC);
CREATE INDEX idx_fato_commodity ON fato_preco(id_commodity);
CREATE INDEX idx_fato_comp      ON fato_preco(id_commodity, data_ref DESC);
CREATE INDEX idx_fato_preco_val ON fato_preco(preco);
```

**EXPLAIN ANALYZE:** Execution Time 1.730ms. Com 4.020 registros o planner
opta por Seq Scan (correto para tabelas pequenas). Os índices serão ativados
automaticamente pelo planner em produção com volume acima de ~10k registros.  
**Print:** `docs/prints/q7_explain_analyze.png`

---

## Q8 — Análise Exploratória (Pandas)

| Commodity | Média | Mediana | Desvio Padrão |
|-----------|-------|---------|---------------|
| cacau | 8.023 | 8.195 | 1.847 |
| soja | 1.061 | 1.048 | 67 |
| trigo | 549 | 543 | 50 |
| milho | 431 | 428 | 30 |
| boi_gordo | 207 | 206 | 18 |
| cafe | 316 | 285 | 71 |
| algodao | 68 | 68 | 5 |
| acucar | 18 | 18 | 2 |

**Prints:** `docs/prints/q8_boxplot.png` | `docs/prints/q8_histogramas.png` | `docs/prints/q8_scatter_temporal.png`

---

## Q9 — Dashboard Streamlit

App com 4 abas: Evolução, Comparativo, Distribuição, Anomalias.  
Filtros por produto, período. KPIs dinâmicos e média móvel 30 dias.
```bash
pip install streamlit plotly psycopg2-binary
streamlit run app/streamlit_app.py
```

**Prints:** `docs/prints/q9_aba_*.png`

---

## Q10 — Insights e Documentação

**Padrões identificados:**
- Cacau com maior volatilidade absoluta (+340% de variação no período)
- Café com distribuição bimodal — dois regimes de preço distintos
- Milho e soja são os mais líquidos — maior volume negociado
- Boi gordo em tendência de alta consistente em 2025

**Aplicações práticas para o agronegócio:**
- Alertas automáticos de anomalia de preço para cooperativas e tradings
- Modelo preditivo de preço com séries temporais (Prophet/ARIMA)
- Suporte à decisão de hedge — identificar janelas de preço favorável
- Correlação entre commodities para diversificação de portfólio

**Limitações da fonte:**
- Preços em USD (futuros CBOT) — não refletem diretamente o mercado físico brasileiro
- Futuros ≠ preço spot — há diferença de base regional
- Yahoo Finance não garante SLA de disponibilidade para uso em produção
- Para uso comercial recomenda-se CEPEA/ESALQ ou Bloomberg como fonte primária

---

## Como Reproduzir
```bash
git clone https://github.com/Rerimoura/analise_agro
cd analise_agro
pip install -r requirements.txt
python scraper/yfinance.py
python ETL/transform_load.py
streamlit run app/app.py
```
```

---

Cria também o `requirements.txt` na raiz:
```
yfinance
pandas
psycopg2-binary
pyarrow
matplotlib
plotly
streamlit
pytest