import pandas as pd
import psycopg2
import os, glob
from datetime import datetime
from dotenv import load_dotenv
import os
load_dotenv()
CONN_STRING = os.getenv("CONN_STRING")

RAW_DIR      = 'data/raw'
PROCESSED_DIR = 'data/processed'
os.makedirs(PROCESSED_DIR, exist_ok=True)

# ── 1. EXTRAÇÃO ──────────────────────────────────────────────────
def extract():
    arquivos = glob.glob(f'{RAW_DIR}/commodities_*.csv')
    if not arquivos:
        raise FileNotFoundError(f'Nenhum CSV encontrado em {RAW_DIR}')
    dfs = [pd.read_csv(f, encoding='utf-8-sig') for f in arquivos]
    df = pd.concat(dfs, ignore_index=True)
    print(f'Extraídos: {len(df)} registros de {len(arquivos)} arquivo(s)')
    return df

# ── 2. TRANSFORMAÇÃO ─────────────────────────────────────────────
def transform(df):
    # Padronizar commodity (soja, SOJA, Soja → soja)
    df['commodity'] = df['commodity'].str.strip().str.lower()

    # Converter data
    df['data_ref'] = pd.to_datetime(df['data_ref'], errors='coerce').dt.date

    # Converter preço (já é float do yfinance, mas garante)
    df['preco'] = pd.to_numeric(df['preco_raw'], errors='coerce').round(4)

    # Volume
    df['volume'] = pd.to_numeric(df['volume'], errors='coerce')

    # Região padrão para futuros CBOT
    df['regiao'] = 'Chicago / CBOT'

    # Remover nulos em campos críticos
    antes = len(df)
    df = df.dropna(subset=['data_ref', 'preco', 'commodity'])
    print(f'Removidos {antes - len(df)} registros com nulos críticos')

    # Remover duplicatas
    df = df.drop_duplicates(subset=['commodity', 'data_ref'])

    # Detectar anomalias (3-sigma por commodity)
    stats = df.groupby('commodity')['preco'].agg(
        media='mean', desvio='std'
    ).reset_index()
    df = df.merge(stats, on='commodity')
    df['anomalia'] = (
        (df['preco'] < 0) |
        (df['preco'] > df['media'] + 3 * df['desvio']) |
        (df['preco'] < df['media'] - 3 * df['desvio'])
    )
    df = df.drop(columns=['media', 'desvio'])

    print(f'Anomalias detectadas: {df["anomalia"].sum()}')
    print(f'Registros limpos: {len(df)}')
    return df[['commodity', 'ticker', 'regiao', 'data_ref', 'preco', 'volume', 'anomalia', 'fonte']]

# ── 3. LOAD ──────────────────────────────────────────────────────
from psycopg2.extras import execute_values

def load(df):
    print('Conectando ao Neon...')
    conn = psycopg2.connect(CONN_STRING, connect_timeout=30)
    print('Conectado!')
    cur = conn.cursor()

    # ── 1. Inserir commodities únicas ────────────────────────────
    commodities = df[['commodity', 'ticker']].drop_duplicates().values.tolist()
    execute_values(cur, """
        INSERT INTO dim_commodity (nome, ticker)
        VALUES %s
        ON CONFLICT (nome) DO NOTHING
    """, commodities)
    conn.commit()
    print(f'dim_commodity: {len(commodities)} registros')

    # ── 2. Inserir regiões únicas ────────────────────────────────
    regioes = df[['regiao']].drop_duplicates().values.tolist()
    execute_values(cur, """
        INSERT INTO dim_regiao (nome)
        VALUES %s
        ON CONFLICT (nome) DO NOTHING
    """, regioes)
    conn.commit()
    print(f'dim_regiao: {len(regioes)} registros')

    # ── 3. Buscar IDs gerados ────────────────────────────────────
    cur.execute("SELECT id_commodity, nome FROM dim_commodity")
    map_comm = {row[1]: row[0] for row in cur.fetchall()}

    cur.execute("SELECT id_regiao, nome FROM dim_regiao")
    map_reg = {row[1]: row[0] for row in cur.fetchall()}

    # ── 4. Inserir fatos em lote ─────────────────────────────────
    df_map = df.copy()
    df_map['id_commodity'] = df_map['commodity'].map(map_comm)
    df_map['id_regiao']    = df_map['regiao'].map(map_reg)

    fatos = list(df_map[['id_commodity','id_regiao','data_ref','preco','volume','fonte']].itertuples(index=False, name=None))

    print(f'Inserindo {len(fatos)} fatos em lote...')
    execute_values(cur, """
        INSERT INTO fato_preco (id_commodity, id_regiao, data_ref, preco, volume, fonte)
        VALUES %s
        ON CONFLICT (id_commodity, id_regiao, data_ref) DO NOTHING
    """, fatos, page_size=1000)

    conn.commit()
    cur.close()
    conn.close()
    print(f'fato_preco: {len(fatos)} registros inseridos')


if __name__ == '__main__':
    print('=== ETL iniciado ===')
    df_raw   = extract()
    df_clean = transform(df_raw)

    ts   = datetime.today().strftime('%Y-%m-%d')
    path = f'{PROCESSED_DIR}/precos_{ts}.parquet'
    df_clean.to_parquet(path, index=False)
    print(f'Salvo processed: {path}')

    print('Iniciando load...')
    try:
        load(df_clean)
    except Exception as e:
        print(f'ERRO no load: {e}')

    print('=== ETL concluído ===')