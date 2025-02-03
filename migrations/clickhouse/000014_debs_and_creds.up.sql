-- spacebox.debs_and_creds definition

CREATE TABLE spacebox.debs_and_creds
(
    `height` Int64,
    `type` String,
    `address` String,
    `coins` String,
    `amount` Int64,
    `denom` String
)
ENGINE = MergeTree
ORDER BY (height,
 address,
 denom)
SETTINGS index_granularity = 8192;


-- spacebox.debs_and_creds_begin_block_events_writer source

CREATE MATERIALIZED VIEW spacebox.debs_and_creds_begin_block_events_writer TO spacebox.debs_and_creds
(
    `height` Int64,
    `type` String,
    `address` String,
    `coins` String,
    `amount` Int64,
    `denom` String
)
AS WITH txs_events AS
    (
        WITH b64 AS
            (
                SELECT
                    height,
                    JSONExtractString(arrayJoin(JSONExtractArrayRaw(begin_block_events)),
 'type') AS type,
                    JSONExtractString(arrayJoin(JSONExtractArrayRaw(begin_block_events)),
 'attributes') AS attributes
                FROM spacebox.raw_block_results
            )
        SELECT
            height,
            type,
            arrayMap(x -> concat('{"key":"',
 JSONExtractString(x,
 'key'),
 '",
"value":"',
 JSONExtractString(x,
 'value'),
 '",
"index":',
 JSONExtractRaw(x,
 'index'),
 '}'),
 JSONExtractArrayRaw(attributes)) AS attributes
        FROM b64
    )
SELECT
    height,
    type,
    if(type = 'coin_spent',
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'spender'),
 attributes)[1],
 'value'),
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'receiver'),
 attributes)[1],
 'value')) AS address,
    arrayJoin(splitByChar(',',
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'amount'),
 attributes)[1],
 'value'))) AS coins,
    if(type = 'coin_spent',
 -toInt128OrZero(extract(coins,
 '^(\\d+)')),
 toInt128OrZero(extract(coins,
 '^(\\d+)'))) AS amount,
    extract(coins,
 '^\\d+(.*)') AS denom
FROM txs_events
WHERE (type = 'coin_received') OR (type = 'coin_spent');

-- spacebox.debs_and_creds_end_block_events_writer source

CREATE MATERIALIZED VIEW spacebox.debs_and_creds_end_block_events_writer TO spacebox.debs_and_creds
(
    `height` Int64,
    `type` String,
    `address` String,
    `coins` String,
    `amount` Int64,
    `denom` String
)
AS WITH txs_events AS
    (
        WITH b64 AS
            (
                SELECT
                    height,
                    JSONExtractString(arrayJoin(JSONExtractArrayRaw(end_block_events)),
 'type') AS type,
                    JSONExtractString(arrayJoin(JSONExtractArrayRaw(end_block_events)),
 'attributes') AS attributes
                FROM spacebox.raw_block_results
            )
        SELECT
            height,
            type,
            arrayMap(x -> concat('{"key":"',
 JSONExtractString(x,
 'key'),
 '",
"value":"',
 JSONExtractString(x,
 'value'),
 '",
"index":',
 JSONExtractRaw(x,
 'index'),
 '}'),
 JSONExtractArrayRaw(attributes)) AS attributes
        FROM b64
    )
SELECT
    height,
    type,
    if(type = 'coin_spent',
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'spender'),
 attributes)[1],
 'value'),
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'receiver'),
 attributes)[1],
 'value')) AS address,
    arrayJoin(splitByChar(',',
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'amount'),
 attributes)[1],
 'value'))) AS coins,
    if(type = 'coin_spent',
 -toInt128OrZero(extract(coins,
 '^(\\d+)')),
 toInt128OrZero(extract(coins,
 '^(\\d+)'))) AS amount,
    extract(coins,
 '^\\d+(.*)') AS denom
FROM txs_events
WHERE (type = 'coin_received') OR (type = 'coin_spent');

-- spacebox.debs_and_creds_transactions_block_events_writer source

CREATE MATERIALIZED VIEW spacebox.debs_and_creds_transactions_block_events_writer TO spacebox.debs_and_creds
(
    `height` Int64,
    `type` String,
    `address` String,
    `coins` String,
    `amount` Int64,
    `denom` String
)
AS WITH txs_events AS
    (
        WITH b64 AS
            (
                SELECT
                    height,
                    JSONExtractString(arrayJoin(JSONExtractArrayRaw(JSONExtractString(arrayJoin(JSONExtractArrayRaw(JSONExtractString(txs_results))),
 'events'))),
 'type') AS type,
                    JSONExtractString(arrayJoin(JSONExtractArrayRaw(JSONExtractString(arrayJoin(JSONExtractArrayRaw(JSONExtractString(txs_results))),
 'events'))),
 'attributes') AS attributes
                FROM spacebox.raw_block_results
            )
        SELECT
            height,
            type,
            arrayMap(x -> concat('{"key":"',
 JSONExtractString(x,
 'key'),
 '",
"value":"',
 JSONExtractString(x,
 'value'),
 '",
"index":',
 JSONExtractRaw(x,
 'index'),
 '}'),
 JSONExtractArrayRaw(attributes)) AS attributes
        FROM b64
    )
SELECT
    height,
    type,
    if(type = 'coin_spent',
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'spender'),
 attributes)[1],
 'value'),
 JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'receiver'),
 attributes)[1],
 'value')) AS address,
    arrayJoin(splitByChar(',',JSONExtractString(arrayFilter(x -> (JSONExtractString(x,'key') = 'amount'),attributes)[1],
 'value'))) AS coins,
    if(type = 'coin_spent',
 -toInt128OrZero(extract(coins,
 '^(\\d+)')),
 toInt128OrZero(extract(coins,
 '^(\\d+)'))) AS amount,
    extract(coins,
 '^\\d+(.*)') AS denom
FROM txs_events
WHERE (type = 'coin_received') OR (type = 'coin_spent');