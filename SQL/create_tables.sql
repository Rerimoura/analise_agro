-- Dimensão: commodities
CREATE TABLE dim_commodity (
    id_commodity SERIAL PRIMARY KEY,
    nome         VARCHAR(50) NOT NULL UNIQUE,  -- 'soja', 'milho', etc.
    unidade      VARCHAR(20)                   -- 'R$/saca 60kg', 'R$/arroba'
);

-- Dimensão: regiões
CREATE TABLE dim_regiao (
    id_regiao SERIAL PRIMARY KEY,
    nome      VARCHAR(100) NOT NULL,
    estado    CHAR(2)
);

-- Fato: preços
CREATE TABLE fato_preco (
    id_preco     SERIAL PRIMARY KEY,
    id_commodity INT  REFERENCES dim_commodity(id_commodity),
    id_regiao    INT  REFERENCES dim_regiao(id_regiao),
    data_ref     DATE NOT NULL,
    preco        NUMERIC(10,2),
    volume       NUMERIC(15,2),  -- pode ser NULL se não disponível
    fonte        VARCHAR(50) DEFAULT 'CEPEA'
);
