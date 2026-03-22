import pytest
import pandas as pd
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'ETL'))
from transform_load import transform


def test_padroniza_commodity():
    df = pd.DataFrame({
        'commodity': ['SOJA', 'Soja', 'soja'],
        'ticker':    ['ZS=F'] * 3,
        'data_ref':  ['2024-01-15'] * 3,
        'preco_raw': [125.30] * 3,
        'volume':    [1000] * 3,
        'fonte':     ['Yahoo Finance'] * 3
    })
    result = transform(df)
    assert result['commodity'].nunique() == 1
    assert result['commodity'].iloc[0] == 'soja'


def test_converte_preco():
    df = pd.DataFrame({
        'commodity': ['milho'],
        'ticker':    ['ZC=F'],
        'data_ref':  ['2024-02-01'],
        'preco_raw': [431.50],
        'volume':    [5000],
        'fonte':     ['Yahoo Finance']
    })
    result = transform(df)
    assert result['preco'].iloc[0] == pytest.approx(431.50)


def test_remove_nulos():
    df = pd.DataFrame({
        'commodity': ['soja', 'milho'],
        'ticker':    ['ZS=F', 'ZC=F'],
        'data_ref':  ['2024-01-15', '2024-01-16'],
        'preco_raw': [None, 431.50],
        'volume':    [1000, 2000],
        'fonte':     ['Yahoo Finance'] * 2
    })
    result = transform(df)
    assert len(result) == 1
    assert result['commodity'].iloc[0] == 'milho'


def test_detecta_anomalia():
    # 20 preços normais + 1 outlier extremo
    precos = [100.0] * 20 + [99999.0]
    datas  = [f'2024-01-{str(i+1).zfill(2)}' for i in range(21)]
    df = pd.DataFrame({
        'commodity': ['soja'] * 21,
        'ticker':    ['ZS=F'] * 21,
        'data_ref':  datas,
        'preco_raw': precos,
        'volume':    [1000] * 21,
        'fonte':     ['Yahoo Finance'] * 21
    })
    result = transform(df)
    assert result['anomalia'].sum() == 1


def test_remove_duplicatas():
    df = pd.DataFrame({
        'commodity': ['soja', 'soja'],
        'ticker':    ['ZS=F', 'ZS=F'],
        'data_ref':  ['2024-01-15', '2024-01-15'],  # mesma data
        'preco_raw': [125.30, 126.00],
        'volume':    [1000, 1000],
        'fonte':     ['Yahoo Finance'] * 2
    })
    result = transform(df)
    assert len(result) == 1