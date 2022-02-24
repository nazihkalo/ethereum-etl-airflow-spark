CREATE TABLE IF NOT EXISTS {{params.database}}.logs
(
    log_index         BIGINT,
    transaction_hash  STRING,
    transaction_index BIGINT,
    block_hash        STRING,
    block_number      BIGINT,
    address           STRING,
    data              STRING,
    topics            STRING
) USING json
OPTIONS (
    path "{{params.file_path}}"
)
PARTITIONED BY (block_date STRING);