INSERT OVERWRITE TABLE `{{database}}`.usd
    PARTITION (dt = date '{{ds}}')
SELECT /*+ REPARTITION(1) */
    prices_usd.symbol,
    prices_usd.time,
    prices_usd.price
FROM `{{database_temp}}`.`usd_{{ds_in_table}}` AS prices_usd