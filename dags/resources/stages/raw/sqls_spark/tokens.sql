CREATE TABLE IF NOT EXISTS {{params.database}}.tokens
(
    address      STRING,
    symbol       STRING,
    name         STRING,
    decimals     STRING,
    total_supply STRING,
    block_number BIGINT
) USING json
OPTIONS (
    path "{{params.file_path}}"
)
PARTITIONED BY (block_date STRING);