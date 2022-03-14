INSERT OVERWRITE {{database}}.contracts
    PARTITION (dt= date '{{ds}}')
    SELECT /*+ REPARTITION(1) */
        contracts.address,
        contracts.bytecode,
        contracts.function_sighashes,
        contracts.is_erc20,
        contracts.is_erc721,
        TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
        blocks.number                       AS block_number,
        blocks.hash                         AS block_hash,
        TO_DATE('{{ds}}')                   AS dt
    FROM {{database_temp}}.contracts_{{ds_in_table}} AS contracts
        JOIN {{database_temp}}.blocks_{{ds_in_table}} AS blocks
    ON contracts.block_number = blocks.number