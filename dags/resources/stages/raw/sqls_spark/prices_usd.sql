CREATE TABLE IF NOT EXISTS `{{database_temp}}`.`{{table}}`
(
    minutes          TIMESTAMP,
    prices           DOUBLE,
    decimals         BIGINT,
    contract_address STRING,
    symbol           STRING,
    dt               DATE
) USING {{file_format}}
    OPTIONS (path "{{file_path}}", header true);