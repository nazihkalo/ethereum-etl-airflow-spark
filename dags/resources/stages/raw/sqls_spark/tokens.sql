CREATE TABLE IF NOT EXISTS {{database}}.tokens
(
    address      STRING,
    symbol       STRING,
    name         STRING,
    decimals     STRING,
    total_supply STRING,
    block_number BIGINT
) USING json
OPTIONS (
    path "{{file_path}}"
)
PARTITIONED BY (block_date STRING);