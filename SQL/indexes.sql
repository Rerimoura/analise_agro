-- Criar índices
CREATE INDEX idx_fato_data       ON fato_preco(data_ref DESC);
CREATE INDEX idx_fato_commodity  ON fato_preco(id_commodity);
CREATE INDEX idx_fato_comp       ON fato_preco(id_commodity, data_ref DESC);
CREATE INDEX idx_fato_preco_val  ON fato_preco(preco);