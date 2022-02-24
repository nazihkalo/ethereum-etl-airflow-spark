INSERT OVERWRITE TABLE {{params.database}}.logs
PARTITION(dt = {{ds}})
AS SELECT
    logs.log_index,
    logs.transaction_hash,
    logs.transaction_index,
    logs.address,
    logs.data,
    logs.topics,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{params.database_temp}}.blocks AS blocks
    JOIN {{params.database_temp}}.logs AS logs ON blocks.number = logs.block_number