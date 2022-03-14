INSERT OVERWRITE {{database}}.token_transfers
    PARTITION (dt= date '{{ds}}')
    SELECT /*+ REPARTITION(1) */
        token_transfers.token_address,
        token_transfers.from_address,
        token_transfers.to_address,
        token_transfers.value,
        token_transfers.transaction_hash,
        token_transfers.log_index,
        TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
        blocks.number                       AS block_number,
        blocks.hash                         AS block_hash
    FROM {{database_temp}}.blocks_{{ds_in_table}} AS blocks
        JOIN {{database_temp}}.token_transfers_{{ds_in_table}} AS token_transfers
    ON blocks.number = token_transfers.block_number
