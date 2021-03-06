CREATE TABLE IF NOT EXISTS `{{database_temp}}`.`{{table}}`
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
) USING {{file_format}}
    OPTIONS (path "{{file_path}}");