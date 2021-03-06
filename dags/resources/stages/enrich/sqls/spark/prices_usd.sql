INSERT OVERWRITE TABLE `{{database}}`.usd
    PARTITION (dt = date '{{ds}}')
SELECT /*+ REPARTITION(1) */
    prices_usd.minute,
    prices_usd.price,
    prices_usd.decimals,
    prices_usd.contract_address,
    prices_usd.symbol
FROM `{{database_temp}}`.`usd_{{ds_in_table}}` AS prices_usd