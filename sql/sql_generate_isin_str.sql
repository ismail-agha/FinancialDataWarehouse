WITH numbered_data AS (
    SELECT
        isin_number,
        CASE
            WHEN nse AND bse THEN 'BOTH'
            WHEN nse THEN 'NSE'
            WHEN bse THEN 'BSE'
        END AS exchange,
        CEIL(ROW_NUMBER()
			 OVER (ORDER BY CASE WHEN nse AND bse THEN 'BOTH' WHEN nse THEN 'NSE' WHEN bse THEN 'BSE' END, isin_number)
			 / CASE WHEN nse AND bse THEN 250.0 ELSE 495.0 END) AS group_number
    FROM
        sm.equity_list
    WHERE
        status = 'Active' and isin_number in ('INE00CE01017', 'INE737W01013') -- to be removed
)
, grouped_data AS (
    SELECT
        exchange,
        group_number,
		count(isin_number) cnt,
        STRING_AGG(CASE
                      WHEN exchange = 'BOTH' THEN 'NSE_EQ|' || isin_number || ',BSE_EQ|' || isin_number
                      WHEN exchange = 'NSE' THEN 'NSE_EQ|' || isin_number
                      WHEN exchange = 'BSE' THEN 'BSE_EQ|' || isin_number
                   END, ',') AS isin_numbers
    FROM
        numbered_data
    GROUP BY
        exchange,
        group_number
)
SELECT
    exchange,
    cnt,
    STRING_AGG(isin_numbers, ',') AS isin_numbers
FROM
    grouped_data
WHERE exchange='NSE' -- to be removed
GROUP BY
    exchange, cnt,
    group_number
ORDER BY
    exchange,
    group_number;
