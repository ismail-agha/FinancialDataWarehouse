WITH BSE_missing AS (
    SELECT 'BSE' AS exchange, CURRENT_DATE trade_date, el.isin_number
    FROM sm.equity_list el
    LEFT JOIN sm.equity_historical_data ehd
        ON el.isin_number = ehd.isin_number
        AND ehd.exchange = 'BSE'
        AND date(ehd.trade_date) = CURRENT_DATE
    WHERE el.status = 'Active'
        AND el.bse = TRUE
        AND ehd.isin_number IS NULL
),
NSE_missing AS (
    SELECT 'NSE' AS exchange, CURRENT_DATE trade_date, el.isin_number
    FROM sm.equity_list el
    LEFT JOIN sm.equity_historical_data ehd
        ON el.isin_number = ehd.isin_number
        AND ehd.exchange = 'NSE'
        AND date(ehd.trade_date) = CURRENT_DATE
    WHERE el.status = 'Active'
        AND el.nse = TRUE
        AND ehd.isin_number IS NULL
)
SELECT exchange, trade_date, count(*) count, STRING_AGG(isin_number, ', ') AS isin_numbers FROM BSE_missing
GROUP BY exchange, trade_date
UNION ALL
SELECT exchange, trade_date, count(*) count, STRING_AGG(isin_number, ', ') AS isin_numbers FROM NSE_missing
GROUP BY exchange, trade_date;