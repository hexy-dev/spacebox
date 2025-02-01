-- spacebox.swap definition

CREATE TABLE spacebox.swap
(
    `height` Int64,
    `timestamp` DateTime,
    `pool_id` UInt32,
    `batch_index` UInt32,
    `swap_requester` String,
    `offer_coin_denom` String,
    `offer_coin_amount` Int256,
    `demand_coin_denom` String,
    `exchanged_demand_coin_amount` Int256,
    `transacted_coin_amount` Int256,
    `remaining_offer_coin_amount` Int256,
    `offer_coin_fee_amount` Int256,
    `order_expiry_height` Int64,
    `exchanged_coin_fee_amount` Float64,
    `order_price` Float64,
    `swap_price` Float64,
    `success` Bool
)
ENGINE = MergeTree
ORDER BY (timestamp,
 height,
 pool_id,
 swap_requester,
 offer_coin_denom,
 demand_coin_denom)
SETTINGS index_granularity = 8192;

-- spacebox.swap_writer for new blocks source


CREATE MATERIALIZED VIEW spacebox.swap_new_blocks_writer TO spacebox.swap
(

    `height` Int64,

    `timestamp` DateTime,

    `pool_id` String,

    `batch_index` String,

    `swap_requester` String,

    `offer_coin_denom` String,

    `offer_coin_amount` String,

    `demand_coin_denom` String,

    `exchanged_demand_coin_amount` String,

    `transacted_coin_amount` String,

    `remaining_offer_coin_amount` String,

    `offer_coin_fee_amount` String,

    `order_expiry_height` String,

    `exchanged_coin_fee_amount` String,

    `order_price` String,

    `swap_price` String,

    `success` Bool
)
AS SELECT
    height,
    timestamp,
    pool_id,
    batch_index,
    swap_requester,
    offer_coin_denom,
    offer_coin_amount,
    demand_coin_denom,
    exchanged_demand_coin_amount,
    transacted_coin_amount,
    remaining_offer_coin_amount,
    offer_coin_fee_amount,
    order_expiry_height,
    exchanged_coin_fee_amount,
    order_price,
    swap_price,
    success
FROM
(SELECT
    height,
    timestamp,
    arrayJoin(JSONExtractArrayRaw(end_block_events)) AS event,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'pool_id'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS pool_id,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'batch_index'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS batch_index,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'swap_requester'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS swap_requester,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'offer_coin_denom'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS offer_coin_denom,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'offer_coin_amount'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS offer_coin_amount,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'demand_coin_denom'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS demand_coin_denom,
        toInt256OrNull(
    nullIf(
        JSONExtractString(
            arrayFirst(x -> JSONExtractString(x, 'key') = 'exchanged_demand_coin_amount',
            JSONExtractArrayRaw(JSONExtractString(event, 'attributes'))),
        'value'),
    '')
) AS exchanged_demand_coin_amount,
        toInt256OrNull(
    nullIf(
        JSONExtractString(
            arrayFirst(x -> JSONExtractString(x, 'key') = 'transacted_coin_amount',
            JSONExtractArrayRaw(JSONExtractString(event, 'attributes'))),
        'value'),
    '')
) AS transacted_coin_amount,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'remaining_offer_coin_amount'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS remaining_offer_coin_amount,
        toInt256OrNull(
    nullIf(
        JSONExtractString(
            arrayFirst(x -> JSONExtractString(x, 'key') = 'offer_coin_fee_amount',
            JSONExtractArrayRaw(JSONExtractString(event, 'attributes'))),
        'value'),
    '')
) AS offer_coin_fee_amount,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'order_expiry_height'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS order_expiry_height,
        toFloat64OrNull(
    nullIf(
        JSONExtractString(
            arrayFirst(x -> JSONExtractString(x, 'key') = 'exchanged_coin_fee_amount',
            JSONExtractArrayRaw(JSONExtractString(event, 'attributes'))),
        'value'),
    '')
) AS exchanged_coin_fee_amount,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'order_price'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS order_price,
        JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'swap_price'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') AS swap_price,
        if(JSONExtractString(arrayFilter(x -> (JSONExtractString(x,
 'key') = 'success'),
 JSONExtractArrayRaw(JSONExtractString(event,
 'attributes')))[1],
 'value') = 'success',
 true,
 false) AS success
    FROM
    (
        SELECT *
        FROM spacebox.raw_block_results
        WHERE end_block_events ILIKE '%swap_transacted%'
    )
    WHERE JSONExtractString(event,
 'type') = 'swap_transacted'
);