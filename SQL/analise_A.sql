WITH mensal AS (
    SELECT
        c.nome                              AS commodity,
        DATE_TRUNC('month', f.data_ref)     AS mes,
        ROUND(AVG(f.preco)::numeric, 2)     AS preco_medio
    FROM fato_preco f
    JOIN dim_commodity c ON f.id_commodity = c.id_commodity
    GROUP BY c.nome, DATE_TRUNC('month', f.data_ref)
)
SELECT
    commodity,
    TO_CHAR(mes, 'YYYY-MM')                                                          AS mes,
    preco_medio,
    LAG(preco_medio) OVER (PARTITION BY commodity ORDER BY mes)                      AS preco_mes_anterior,
    ROUND(
        (preco_medio - LAG(preco_medio) OVER (PARTITION BY commodity ORDER BY mes))
        / NULLIF(LAG(preco_medio) OVER (PARTITION BY commodity ORDER BY mes), 0)
        * 100
    , 2)                                                                             AS variacao_pct
FROM mensal
ORDER BY commodity, mes;