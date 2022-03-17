CREATE TABLE IF NOT EXISTS `{{dataset_temp}}`.`{{table}}`
(
    time   TIMESTAMP,
    price  DOUBLE,
    symbol STRING,
    dt     DATE
) USING {{file_format}}
    OPTIONS (path "{{file_path}}");