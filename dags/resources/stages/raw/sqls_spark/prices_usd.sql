CREATE TABLE IF NOT EXISTS `{{database_temp}}`.`{{table}}`
(
    symbol STRING,
    time   TIMESTAMP,
    price  DOUBLE,
    dt     DATE
) USING {{file_format}}
    OPTIONS (path "{{file_path}}", header true);