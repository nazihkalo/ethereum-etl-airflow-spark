CREATE TABLE IF NOT EXISTS {{database}}.transactions
(
    hash                     STRING,
    nonce                    BIGINT,
    block_hash               STRING,
    block_number             BIGINT,
    transaction_index        BIGINT,
    from_address             STRING,
    to_address               STRING,
    value                    DECIMAL(38, 0),
    gas                      BIGINT,
    gas_price                BIGINT,
    input                    STRING,
    max_fee_per_gas          BIGINT,
    max_priority_fee_per_gas BIGINT,
    transaction_type         BIGINT
) USING json
OPTIONS (
    path "{{file_path}}"
)
PARTITIONED BY (block_date STRING);