# BSE
headers = {
            'authority': 'api.bseindia.com',
            'method': 'GET',
            'path': '/BseIndiaAPI/api/getScripHeaderData/w?Debtflag=&scripcode=500002&seriesid=',
            'scheme': 'https',
            'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'accept-encoding': 'gzip, deflate, br',
            'accept-language': 'en,gu;q=0.9,hi;q=0.8',
            'cache-control': 'max-age=0',
            'if-modified-since': 'Tue, 30 May 2023 18:28:57 GMT',
            'sec-ch-ua': 'Google Chrome',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': 'Windows',
            'sec-fetch-dest': 'document',
            'sec-fetch-mode': 'navigate',
            'sec-fetch-site': 'none',
            'sec-fetch-user': '?1',
            'upgrade-insecure-requests': '1',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
           }

bse_api_base_url = 'https://api.bseindia.com/BseIndiaAPI/api/'

endpoint_equity_list_json = bse_api_base_url + 'ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status='

endpoint_equity_list_csv = bse_api_base_url + 'LitsOfScripCSVDownload/w?segment=Equity&status=&industry=&Group=&Scripcode='

endpoint_equity_historical_csv = bse_api_base_url + 'StockPriceCSVDownload/w?pageType=0&rbType=D&Scode={}&FDates={}&TDates={}'

#Daily Load
endpoint_equity_daily_json_part1 = bse_api_base_url + 'getScripHeaderData/w?Debtflag=&scripcode={}&seriesid=' #PrevClose, Open, High, Low, LTP, Ason

endpoint_equity_daily_json_part2 = bse_api_base_url + 'HighLow/w?Type=EQ&flag=C&scripcode={}' #Fifty2WkHigh_adj, Fifty2WkHigh_adjDt, Fifty2WkLow_adj, Fifty2WkLow_adjDt, Fifty2WkHigh_unadj, Fifty2WkLow_unadj, MonthHighLow, WeekHighLow

endpoint_equity_daily_json_part3 = bse_api_base_url + 'StockTrading/w?flag=&quotetype=EQ&scripcode={}' #Turnover (Cr.), MarketCap, CktLimit (Upper/Lower Price Band)

endpoint_equity_daily_json_part4 = bse_api_base_url + 'ComHeader/w?quotetype=EQ&scripcode={}&seriesid=' #ROE , P/E, P/B, EPS(TTM), CEPS(TTM), FaceValue



# ----------------------------------------------------------------------------------------------------------------------
# UPSTOX
# ----------------------------------------------------------------------------------------------------------------------

upstox_base_url = 'https://api.upstox.com/v2/'

url_upstox_token = upstox_base_url + 'login/authorization/token'

upstox_historical = upstox_base_url + 'historical-candle/{}_EQ%7C{}/{}/{}/{}'

upstox_eq_full_market_quote = upstox_base_url + 'market-quote/quotes?instrument_key={}'

upstox_headers_market_quote = {
    'Accept': 'application/json',
    'Authorization': 'Bearer {}'
}

