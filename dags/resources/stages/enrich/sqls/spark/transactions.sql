SELECT
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
    transactions.transaction_type,
    TO_DATE('{{ds}}') as dt
FROM {{dataset_temp}}.blocks_{{ds_in_table}} AS blocks
    JOIN {{database_temp}}.transactions_{{ds_in_table}} AS transactions ON blocks.number = transactions.block_number