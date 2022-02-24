INSERT OVERWRITE TABLE {{params.database}}.transactions
PARTITION(dt = {{ds}})
AS SELECT
    transactions.hash,
    transactions.nonce,
    transactions.transaction_index,
    transactions.from_address,
    transactions.to_address,
    transactions.value,
    transactions.gas,
    transactions.gas_price,
    transactions.input,
    TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
    blocks.number AS block_number,
    blocks.hash AS block_hash,
    transactions.max_fee_per_gas,
    transactions.max_priority_fee_per_gas,
    transactions.transaction_type
FROM {{params.dataset_temp}}.blocks AS blocks
    JOIN {{params.database_temp}}.transactions AS transactions ON blocks.number = transactions.block_number