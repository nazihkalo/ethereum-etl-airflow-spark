CREATE TABLE IF NOT EXISTS {{database_temp}}.{{table}}
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
);