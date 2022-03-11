INSERT OVERWRITE {{database}}.logs
    PARTITION (dt= date '{{ds}}', address_hash, selector_hash)
SELECT /*+ REPARTITION(1) */
    log_index,
    transaction_hash,
    transaction_index,
    address,
    data,
    topics,
    block_timestamp,
    block_number,
    block_hash,
    topics_arr,
    unhex_data,
    topics_arr[0]                 as selector,
    address_hash,
    abs(hash(topics_arr[0])) % 10 as selector_hash
FROM (
         SELECT logs.log_index,
                logs.transaction_hash,
                logs.transaction_index,
                logs.address,
                logs.data,
                logs.topics,
                TIMESTAMP_SECONDS(blocks.timestamp) AS block_timestamp,
                blocks.number                       AS block_number,
                blocks.hash                         AS block_hash,
                abs(hash(address)) % 10             as address_hash,
                IF(
                        topics rlike ',',
                        IF(topics rlike '^[0-9]+', split(replace(topics, '"', ''), ','),
                           from_json(topics, 'array<string>')),
                        array(topics)
                    )                               AS topics_arr,
                unhex(substr(data, 3))              as unhex_data
         FROM {{database_temp}}.blocks_{{ds_in_table}} AS blocks
    JOIN {{database_temp}}.logs_{{ds_in_table}} AS logs
         ON blocks.number = logs.block_number
     ) as l