CREATE TABLE IF NOT EXISTS {{database}}.{{table}}
(
    block_number      BIGINT,
    transaction_hash  STRING,
    transaction_index BIGINT,
    from_address      STRING,
    to_address        STRING,
    value             DECIMAL(38, 0),
    input             STRING,
    output            STRING,
    trace_type        STRING,
    call_type         STRING,
    reward_type       STRING,
    gas               BIGINT,
    gas_used          BIGINT,
    subtraces         BIGINT,
    trace_address     STRING,
    error             STRING,
    status            BIGINT,
    trace_id          STRING
) USING json
OPTIONS (
    path "{{file_path}}"
);