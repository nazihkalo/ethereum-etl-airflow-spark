INSERT OVERWRITE TABLE {{params.database}}.token_transfers
PARTITION(dt = {{ds}})
AS SELECT
    token_transfers.token_address,
    token_transfers.from_address,
    token_transfers.to_address,
    token_transfers.value,
    token_transfers.transaction_hash,
    token_transfers.log_index,
    TO_DATE(TIMESTAMP_SECONDS(blocks.timestamp)) AS dt,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash
FROM {{params.database_temp}}.blocks AS blocks
    JOIN {{params.database_temp}}.token_transfers AS token_transfers ON blocks.number = token_transfers.block_number
