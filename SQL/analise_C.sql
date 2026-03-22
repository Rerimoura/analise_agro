WITH stats AS (
    SELECT
        id_commodity,
        AVG(preco)    AS media,
        STDDEV(preco) AS desvio
    FROM fato_preco
    GROUP BY id_commodity
)
SELECT
    c.nome          AS commodity,
    f.data_ref,
    f.preco,
    ROUND(s.media::numeric, 2)                  AS media_historica,
    ROUND((s.media + 3 * s.desvio)::numeric, 2) AS limite_superior,
    ROUND((s.media - 3 * s.desvio)::numeric, 2) AS limite_inferior,
    CASE
        WHEN f.preco < 0                          THEN 'Preço negativo'
        WHEN f.preco > s.media + 3 * s.desvio     THEN 'Acima 3-sigma'
        WHEN f.preco < s.media - 3 * s.desvio     THEN 'Abaixo 3-sigma'
    END AS tipo_anomalia
FROM fato_preco f
JOIN stats          s ON f.id_commodity = s.id_commodity
JOIN dim_commodity  c ON f.id_commodity = c.id_commodity
WHERE f.preco < 0
   OR f.preco > s.media + 3 * s.desvio
   OR f.preco < s.media - 3 * s.desvio
ORDER BY c.nome, f.data_ref;