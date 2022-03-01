SELECT
    tokens.address,
    tokens.symbol,
    tokens.name,
    tokens.decimals,
    tokens.total_supply
FROM {{database_temp}}.tokens_{{ds_in_table}} AS tokens
WHERE tokens.address IN
(
    SELECT address FROM {{database_temp}}.tokens_{{ds_in_table}}
    EXCEPT
    SELECT address FROM {{database}}.tokens
)
