CREATE TABLE IF NOT EXISTS {{database}}.contracts
(
    address            STRING,
    bytecode           STRING,
    function_sighashes STRING,
    is_erc20           BOOLEAN,
    is_erc721          BOOLEAN,
    block_number       BIGINT
) USING json
OPTIONS (
    path "{{file_path}}"
)
PARTITIONED BY (block_date STRING);