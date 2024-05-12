# ----------------------------------------------------------------------------------------------------------------------
# BSE
# ----------------------------------------------------------------------------------------------------------------------

curl_cmd_bse_equity = [
    "curl",
    "https://api.bseindia.com/BseIndiaAPI/api/ListofScripData/w?Group=&Scripcode=&industry=&segment=Equity&status=",
    "-H", "accept: application/json, text/plain, */*",
    "-H", "accept-language: en-US,en;q=0.9,la;q=0.8",
    "-H", "origin: https://www.bseindia.com",
    "-H", "priority: u=1, i",
    "-H", "referer: https://www.bseindia.com/",
    "-H", "sec-ch-ua: \"Chromium\";v=\"124\", \"Google Chrome\";v=\"124\", \"Not-A.Brand\";v=\"99\"",
    "-H", "sec-ch-ua-mobile: ?0",
    "-H", "sec-ch-ua-platform: \"Windows\"",
    "-H", "sec-fetch-dest: empty",
    "-H", "sec-fetch-mode: cors",
    "-H", "sec-fetch-site: same-site",
    "-H", "user-agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
]

# ----------------------------------------------------------------------------------------------------------------------
# NSE
# ----------------------------------------------------------------------------------------------------------------------

nse_equity_request_params = {
    "url": "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "headers": {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Upgrade-Insecure-Requests": "1",
        "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": "Windows"
    }
}

# ----------------------------------------------------------------------------------------------------------------------
# UPSTOX
# ----------------------------------------------------------------------------------------------------------------------

# Base UEL
upstox_base_url = 'https://api.upstox.com/v2/'

# Token
url_upstox_token = upstox_base_url + 'login/authorization/token'

headers_upstox_token = {
    'accept': 'application/json',
    'Content-Type': 'application/x-www-form-urlencoded',
}

# Historical Candle
upstox_historical = upstox_base_url + 'historical-candle/{}_EQ%7C{}/{}/{}/{}'

# Daily Market Quote
upstox_eq_full_market_quote = upstox_base_url + 'market-quote/quotes?instrument_key={}'

upstox_headers_market_quote = {
    'Accept': 'application/json',
    'Authorization': 'Bearer {}'
}

# Exchange Holiday List
url_upstox_market_holidays = upstox_base_url + "market/holidays/"
