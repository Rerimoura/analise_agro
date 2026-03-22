import yfinance as yf
import pandas as pd, os
from datetime import datetime

RAW_DIR = 'data/raw'
os.makedirs(RAW_DIR, exist_ok=True)

COMMODITIES = {
    'soja':      'ZS=F',
    'milho':     'ZC=F',
    'trigo':     'ZW=F',
    'cafe':      'KC=F',
    'algodao':   'CT=F',
    'acucar':    'SB=F',
    'boi_gordo': 'LE=F',
    'cacau':     'CC=F',
}

registros = []
for nome, ticker in COMMODITIES.items():
    print(f'Coletando: {nome} ({ticker})')
    try:
        df = yf.download(ticker, period='2y', interval='1d', progress=False, auto_adjust=True)

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0] for col in df.columns]

        df = df.reset_index()

        df_clean = pd.DataFrame({
            'data_ref':    df['Date'],
            'commodity':   nome,
            'ticker':      ticker,
            'preco_raw':   df['Close'],
            'volume':      df['Volume'] if 'Volume' in df.columns else None,
            'fonte':       'Yahoo Finance / CBOT',
            'data_coleta': datetime.today().strftime('%Y-%m-%d'),
        })

        registros.append(df_clean)
        print(f'  -> {len(df_clean)} registros')
    except Exception as e:
        print(f'  ERRO em {nome}: {e}')

final = pd.concat(registros, ignore_index=True)
ts = datetime.today().strftime('%Y-%m-%d')
path = f'{RAW_DIR}/commodities_{ts}.csv'
final.to_csv(path, index=False, encoding='utf-8-sig')
print(f'\nSalvo: {path}')
print(f'Total: {len(final)} linhas | {final["commodity"].nunique()} commodities')
print(final.head())