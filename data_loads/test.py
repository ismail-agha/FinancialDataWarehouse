import json
from datetime import datetime
import pandas as pd
# Your JSON string
json_string = '''
{
    "status": "success",
    "data": {
        "NSE_EQ:ORIENTBELL": {
            "ohlc": {
                "open": 376.0,
                "high": 376.0,
                "low": 363.0,
                "close": 368.15
            },
            "depth": {
                "buy": [
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ],
                "sell": [
                    {
                        "quantity": 9,
                        "price": 368.15,
                        "orders": 1
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ]
            },
            "timestamp": "2024-02-04T14:11:45.632+05:30",
            "instrument_token": "NSE_EQ|INE607D01018",
            "symbol": "ORIENTBELL",
            "last_price": 368.0,
            "volume": 50511,
            "average_price": 368.93,
            "oi": 0.0,
            "net_change": -6.199999999999989,
            "total_buy_quantity": 0.0,
            "total_sell_quantity": 9.0,
            "lower_circuit_limit": 299.4,
            "upper_circuit_limit": 449.0,
            "last_trade_time": "1706867989788",
            "oi_day_high": 0.0,
            "oi_day_low": 0.0
        },
        "NSE_EQ:MURUDCERA": {
            "ohlc": {
                "open": 0.0,
                "high": 52.9,
                "low": 52.55,
                "close": 52.7
            },
            "depth": {
                "buy": [
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ],
                "sell": [
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ]
            },
            "timestamp": "2024-02-04T14:11:45.632+05:30",
            "instrument_token": "NSE_EQ|INE692B01014",
            "symbol": "MURUDCERA",
            "last_price": 52.70000076293945,
            "volume": 16432,
            "average_price": 52.73,
            "oi": 0.0,
            "net_change": -0.5,
            "total_buy_quantity": 0.0,
            "total_sell_quantity": 0.0,
            "lower_circuit_limit": 52.150001525878906,
            "upper_circuit_limit": 54.25,
            "last_trade_time": "1706867080000",
            "oi_day_high": 0.0,
            "oi_day_low": 0.0
        },
        "NSE_EQ:REGENCERAM": {
            "ohlc": {
                "open": 38.95,
                "high": 38.95,
                "low": 35.4,
                "close": 36.0
            },
            "depth": {
                "buy": [
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ],
                "sell": [
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ]
            },
            "timestamp": "2024-02-04T14:11:45.632+05:30",
            "instrument_token": "NSE_EQ|INE277C01012",
            "symbol": "REGENCERAM",
            "last_price": 36.0,
            "volume": 1930,
            "average_price": 37.7,
            "oi": 0.0,
            "net_change": -1.1000000000000014,
            "total_buy_quantity": 0.0,
            "total_sell_quantity": 0.0,
            "lower_circuit_limit": 35.25,
            "upper_circuit_limit": 38.95,
            "last_trade_time": "1706867546605",
            "oi_day_high": 0.0,
            "oi_day_low": 0.0
        },
        "NSE_EQ:KAJARIACER": {
            "ohlc": {
                "open": 1384.8,
                "high": 1390.4,
                "low": 1342.5,
                "close": 1344.8
            },
            "depth": {
                "buy": [
                    {
                        "quantity": 71,
                        "price": 1344.8,
                        "orders": 5
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ],
                "sell": [
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    },
                    {
                        "quantity": 0,
                        "price": 0.0,
                        "orders": 0
                    }
                ]
            },
            "timestamp": "2024-02-04T14:11:45.632+05:30",
            "instrument_token": "NSE_EQ|INE217B01036",
            "symbol": "KAJARIACER",
            "last_price": 1344.800048828125,
            "volume": 130954,
            "average_price": 1356.63,
            "oi": 0.0,
            "net_change": -32.799999999999955,
            "total_buy_quantity": 71.0,
            "total_sell_quantity": 0.0,
            "lower_circuit_limit": 1102.1,
            "upper_circuit_limit": 1653.1,
            "last_trade_time": "1706868839211",
            "oi_day_high": 0.0,
            "oi_day_low": 0.0
        }
    }
}
'''

# Parse the JSON string
parsed_json = json.loads(json_string)

# Now you can access elements of the JSON data
print(parsed_json['status'])  # Output: success
print(parsed_json['data']['NSE_EQ:ORIENTBELL']['ohlc']['open'])  # Output: 376.0
print(len(parsed_json['data']))

df = pd.DataFrame()

for i in parsed_json['data']:
    print(i)
    print(parsed_json['data'][i]['instrument_token'])
    # print(parsed_json['data'][i]['ohlc']['open'])
    # print(parsed_json['data'][i]['ohlc']['high'])
    # print(parsed_json['data'][i]['ohlc']['low'])
    # print(parsed_json['data'][i]['ohlc']['close'])
    #
    # print(parsed_json['data'][i]['average_price'])
    # print(parsed_json['data'][i]['volume'])
    # print(parsed_json['data'][i]['net_change'])
    # print(parsed_json['data'][i]['last_trade_time'])
    # print(datetime.fromtimestamp(int(parsed_json['data'][i]['last_trade_time'])/1000))
    #
    # print(parsed_json['data'][i]['total_buy_quantity'])
    # print(parsed_json['data'][i]['total_sell_quantity'])
    # print(parsed_json['data'][i]['lower_circuit_limit'])
    # print(parsed_json['data'][i]['upper_circuit_limit'])

    new_row = {'exchange': parsed_json['data'][i]['instrument_token'][:3],
               'isin_number': parsed_json['data'][i]['instrument_token'][7:],

               'open': parsed_json['data'][i]['ohlc']['open'],
               'high': parsed_json['data'][i]['ohlc']['high'],
               'low': parsed_json['data'][i]['ohlc']['low'],
               'close': parsed_json['data'][i]['ohlc']['close'],

               'average_price': parsed_json['data'][i]['average_price'],
               'volume': parsed_json['data'][i]['volume'],
               'net_change': parsed_json['data'][i]['net_change'],
               'total_buy_quantity': parsed_json['data'][i]['total_buy_quantity'],
               'total_sell_quantity': parsed_json['data'][i]['total_sell_quantity'],
               'lower_circuit_limit': parsed_json['data'][i]['lower_circuit_limit'],
               'upper_circuit_limit': parsed_json['data'][i]['upper_circuit_limit'],
               'trade_date': datetime.fromtimestamp(int(parsed_json['data'][i]['last_trade_time'])/1000),
               }

    df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)

print(df.to_string())
