WITH PreviousClose AS (
    SELECT
        isin_number,
        close AS prev_close,
        trade_date,
        ROW_NUMBER() OVER (PARTITION BY isin_number ORDER BY trade_date DESC) AS rn
    FROM
        sm.equity_historical_data
    WHERE
        DATE(trade_date) < %(trade_date)s
        AND exchange = %(exchange)s
)
, LatestClose AS (
    SELECT
        exchange,
        isin_number,
        close AS current_close,
        trade_date
    FROM
        sm.equity_historical_data
    WHERE
        DATE(trade_date) = %(trade_date)s
        AND exchange = %(exchange)s
)
, PreviousCloseFiltered AS (
    SELECT
        isin_number,
        prev_close,
        trade_date
    FROM
        PreviousClose
    WHERE
        rn = 1
)
SELECT
    lc.exchange,
    el.issuer_name,
    lc.isin_number,
    lc.trade_date,
    lc.current_close,
    pc.prev_close,
    ((lc.current_close - pc.prev_close) / pc.prev_close) * 100 AS percentage_change
FROM
    LatestClose lc
JOIN
    PreviousCloseFiltered pc ON lc.isin_number = pc.isin_number
JOIN
    sm.equity_list el ON el.isin_number = lc.isin_number
WHERE
    el.status = 'Active'
    AND pc.prev_close IS NOT NULL
-- ORDER BY clause & LIMIT will be appended dynamically in the Python code
;