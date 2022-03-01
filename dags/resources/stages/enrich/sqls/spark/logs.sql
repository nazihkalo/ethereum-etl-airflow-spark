SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.address,
    logs.data,
    logs.topics,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash,
    TO_DATE('{{ds}}') as dt
FROM {{database_temp}}.blocks_{{ds_in_table}} AS blocks
    JOIN {{database_temp}}.logs_{{ds_in_table}} AS logs ON blocks.number = logs.block_number