# Documentação de Modelagem — agro-e

## Q2 — Camada Raw

### Organização dos arquivos brutos
```
data/
└── raw/
    └── commodities_2026-03-22.csv   # Gerado via yfinance (Yahoo Finance / CBOT)
```

### Comparativo de formatos

| Formato | Vantagens | Desvantagens | Uso neste projeto |
|---------|-----------|--------------|-------------------|
| **CSV** | Legível, compatível com Excel, fácil debug | Sem tipagem, grande em volume | ✅ Camada raw |
| **JSON** | Flexível, ideal para dados aninhados | Verboso, lento para analytics | APIs e logs |
| **Parquet** | Compressão 5-10x, tipagem forte, rápido em analytics | Binário, precisa de ferramenta | ✅ Camada processed/curated |

**Escolha:** CSV na camada raw por ser inspecionável sem ferramentas.  
Parquet na camada processed para performance no ETL e Streamlit.

### Equivalente em AWS S3
```
s3://agro-e-datalake/
    raw/yahoo_finance/2026/03/commodities_2026-03-22.csv
    processed/precos/2026/03/precos_2026-03-22.parquet
    curated/dashboard/precos_mensais.parquet
```

**Configurações recomendadas no S3:**
- Versionamento habilitado no bucket (nunca perder o raw)
- Lifecycle policy: mover raw para S3-Glacier após 90 dias
- Particionamento por ano/mês para eficiência em queries Athena
- Bucket policy bloqueando acesso público direto

### Fonte dos dados
- **Fonte:** Yahoo Finance via biblioteca `yfinance`
- **Commodities:** soja, milho, trigo, café, algodão, açúcar, boi gordo, cacau
- **Período:** 2 anos de histórico diário (dias úteis)
- **Desafios encontrados:**
  - CEPEA retornou 403 (proteção Cloudflare)
  - BCB/SGS retornou timeout por instabilidade do servidor
  - Yahoo Finance: colunas em MultiIndex dependendo da versão do yfinance (tratado no scraper)