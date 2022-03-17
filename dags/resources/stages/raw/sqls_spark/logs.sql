CREATE TABLE IF NOT EXISTS `{{database_temp}}`.`{{table}}`
(
    log_index         BIGINT,
    transaction_hash  STRING,
    transaction_index BIGINT,
    block_hash        STRING,
    block_number      BIGINT,
    address           STRING,
    data              STRING,
    topics            STRING
) USING {{file_format}}
    OPTIONS (path "{{file_path}}");