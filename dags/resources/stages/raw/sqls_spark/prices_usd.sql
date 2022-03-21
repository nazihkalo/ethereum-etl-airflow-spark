CREATE TABLE IF NOT EXISTS `{{database_temp}}`.`{{table}}`
(
    minute           TIMESTAMP,
    price            DOUBLE,
    decimals         BIGINT,
    contract_address STRING,
    symbol           STRING,
    dt               DATE
) USING {{file_format}}
    OPTIONS (path "{{file_path}}", header true);