CREATE TABLE IF NOT EXISTS `{{database_temp}}`.`{{table}}`
(
    address            STRING,
    bytecode           STRING,
    function_sighashes STRING,
    is_erc20           BOOLEAN,
    is_erc721          BOOLEAN,
    block_number       BIGINT
) USING {{file_format}}
    OPTIONS (path "{{file_path}}");