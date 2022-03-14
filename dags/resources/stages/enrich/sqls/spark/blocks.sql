INSERT OVERWRITE {{database}}.blocks
    PARTITION (dt= date '{{ds}}')
    SELECT /*+ REPARTITION(1) */
        TIMESTAMP_SECONDS(blocks.timestamp) AS timestamp,
        blocks.number,
        blocks.hash,
        blocks.parent_hash,
        blocks.nonce,
        blocks.sha3_uncles,
        blocks.logs_bloom,
        blocks.transactions_root,
        blocks.state_root,
        blocks.receipts_root,
        blocks.miner,
        blocks.difficulty,
        blocks.total_difficulty,
        blocks.size,
        blocks.extra_data,
        blocks.gas_limit,
        blocks.gas_used,
        blocks.transaction_count,
        blocks.base_fee_per_gas,
        TO_DATE('{{ds}}')                   AS dt
    FROM {{database_temp}}.blocks_{{ds_in_table}} AS blocks