INSERT OVERWRITE TABLE `{{database}}`.usd
    PARTITION (dt = date '{{ds}}')
SELECT /*+ REPARTITION(1) */
    prices.time,
    prices.price,
    prices.symbol
FROM `{{database_temp}}`.usd AS prices