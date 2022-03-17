INSERT OVERWRITE TABLE `{{database}}`.logs
    PARTITION (dt = date '{{ds}}', address_hash, selector_hash)
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
    topics_arr[0]                 AS selector,
    address_hash,
    abs(hash(topics_arr[0])) % 10 AS selector_hash
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
                abs(hash(logs.address)) % 10        AS address_hash,
                IF(
                            logs.topics rlike ',',
                            IF(logs.topics rlike '^[0-9]+', split(replace(logs.topics, '"', ''), ','),
                               from_json(logs.topics, 'array<string>')),
                            array(logs.topics)
                    )                               AS topics_arr,
                unhex(substr(logs.data, 3))         AS unhex_data
         FROM `{{database_temp}}`.`blocks_{{ds_in_table}}` AS blocks
                  JOIN `{{database_temp}}`.`logs_{{ds_in_table}}` AS logs
                       ON blocks.number = logs.block_number
     )
