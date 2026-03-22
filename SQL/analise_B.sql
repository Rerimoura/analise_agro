SELECT
    c.nome                          AS commodity,
    COUNT(*)                        AS total_registros,
    ROUND(SUM(f.volume)::numeric, 0)AS volume_total,
    ROUND(AVG(f.preco)::numeric, 2) AS preco_medio
FROM fato_preco f
JOIN dim_commodity c ON f.id_commodity = c.id_commodity
WHERE f.data_ref >= CURRENT_DATE - INTERVAL '1 year'
  AND f.volume IS NOT NULL
GROUP BY c.nome
ORDER BY volume_total DESC
LIMIT 5;