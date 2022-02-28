CREATE TABLE IF NOT EXISTS {{database}}.{{table}}
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
    path "{{file_path}}"
);