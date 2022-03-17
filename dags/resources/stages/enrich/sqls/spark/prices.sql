INSERT OVERWRITE TABLE `{{database}}`.usd
    PARTITION (dt = date '{{ds}}')
SELECT /*+ REPARTITION(1) */
    prices_usd.time,
    prices_usd.price,
    prices_usd.symbol
FROM `{{database_temp}}`.`prices_{{ds_in_table}}` AS prices_usd