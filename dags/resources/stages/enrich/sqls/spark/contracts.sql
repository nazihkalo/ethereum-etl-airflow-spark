INSERT OVERWRITE TABLE {{params.database}}.contracts
PARTITION(dt = {{ds}})
AS SELECT
    contracts.address,
    contracts.bytecode,
    contracts.function_sighashes,
    contracts.is_erc20,
    contracts.is_erc721,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{params.database_temp}}.contracts AS contracts
    JOIN {{params.database_temp}}.blocks AS blocks ON contracts.block_number = blocks.number