from typing import Optional
from fastapi import FastAPI, status, Response
import requests

PERIODS = ["1m", "3m", "5m", "15m", "30m", "1h",
                    "2h", "4h", "6h", "8h", "12h", "1d", "3d", "1w", "1M"]

def arr2dict(data, keys):
    if type(data) is dict:
        return data
    response = []
    for stick in data:
        dict_data = {}
        for key, val in zip(keys, stick):
            dict_data[key] = val
        response.append(dict_data)
    return response


def get_params(**kwargs):
    final = kwargs.copy()
    for k, v in kwargs.items():
        if not v:
            final.pop(k)
    return final


def handle_request(res, method, url, params={}):
    base_url = "https://fapi.binance.com"
    method_map = {
        'GET': requests.get,
    }

    request = method_map.get(method)

    if not request:
        return "Request method not recognised or implemented"

    response = request(base_url+url, params=params, verify=True)
    if response.status_code in [200, 201]:
        return response.json()
    else:
        res.status_code = response.status_code
        return {
                "Error": "Please check 'https://rapidapi.com/daviesnzan/api/binance-api8' for more info",
                "Code": response.status_code,
                "reason": response.reason
            }


app = FastAPI()


@app.get("/server-time")
def server_time(response:Response):
    return handle_request(response, "GET", "/fapi/v1/time")

@app.get("/exchange-info")
def exchange_info(response:Response):
    return handle_request(response, "GET", "/fapi/v1/exchangeInfo")

@app.get("/market-order-book/{pair}")
def market_order_book(response:Response, pair: str, limit: Optional[int] = 500):
    if limit not in [5, 10, 20, 50, 100, 500, 1000]:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"Error": "Valid limits are [5, 10, 20, 50, 100, 500, 1000]"}
    data = handle_request(response, "GET", f"/fapi/v1/depth", get_params(symbol=pair, limit=limit))
    return data


@app.get("/trades/{pair}")
def trades(response:Response, pair: str, limit: Optional[int] = 500):
    if limit > 1000:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"Error": "Maximum limit is 1000"}
    return handle_request(response, "GET", "/fapi/v1/trades", get_params(symbol=pair, limit=limit))


# @app.get("/historical-trades/{pair}")
# def historical_trades(response:Response, pair: str, fromid: Optional[int] = None, limit: Optional[int] = 500):
#     if limit > 1000:
#         return {'Error': "Maximum limit is 1000"}
#     return handle_request(response, "GET", "/fapi/v1/historicalTrades", get_params(symbol=pair, limit=limit, fromId=fromid))


@app.get("/candlestick/{pair}/{interval}")
def candlestick(response:Response, pair: str, interval: str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 500):
    if interval not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Interval is invalid. Allowed intervals are "+str(PERIODS)}
    if limit > 1500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 1500"}
    data = handle_request(response, "GET", "/fapi/v1/klines", get_params(symbol=pair,
                          interval=interval, startTime=startTime, endTime=endTime, limit=limit))
    keys = ["open-time", "open", "high", "low", "close", "volume", "close-time", "quote-asset-volume",
                "number-of-trades", "taker-buy-base-asset-volume", "taker-buy-quote-asset-volume"]
    return arr2dict(data, keys)


@app.get("/contract-candlestick/{pair}/{contract}/{interval}")
def contract_candlestick(response:Response, pair: str, contract:str, interval: str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 500):
    contractTypes = ["PERPETUAL","CURRENT_QUARTER","NEXT_QUARTER"]
    if contract not in contractTypes:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"Error": "The allowed contract types are "+contractTypes}
    if interval not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"Error": "The allowed intervals are "+PERIODS}
    if limit > 1500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 1500"}
    keys = ["open-time", "open", "high", "low", "close", "volume", "close-time", "quote-asset-volume",
                "number-of-trades", "taker-buy-volume", "taker-buy-quote-asset-volume"]
    data = handle_request(response, "GET", "/fapi/v1/continuousKlines", get_params(pair=pair, contractType=contract, interval=interval, startTime=startTime, endTime=endTime, limit=limit))
    return arr2dict(data, keys)

@app.get("/index-price-candlestick/{pair}/{interval}")
def index_price_candlestick(response:Response, pair: str, interval: str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 500):
    if interval not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"Error": "The allowed intervals are "+PERIODS}
    if limit > 1500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 1500"}
    keys = ["open-time", "open", "high", "low", "close", "ignore", "close-time"]
    data = handle_request(response, "GET", "/fapi/v1/indexPriceKlines", get_params(pair=pair, interval=interval, startTime=startTime, endTime=endTime, limit=limit))
    return arr2dict(data, keys)

@app.get("/mark-price-candlestick/{pair}/{interval}")
def mark_price_candlestick(response:Response, pair: str, interval: str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 500):
    if interval not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {"Error": "The allowed intervals are "+PERIODS}
    if limit > 1500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 1500"}
    keys = ["open-time", "open", "high", "low", "close", "ignore", "close-time"]
    data = handle_request(response, "GET", "/fapi/v1/markPriceKlines", get_params(symbol=pair, interval=interval, startTime=startTime, endTime=endTime, limit=limit))
    return arr2dict(data, keys)

@app.get("/premium-index")
def mark_price_candlestick(response:Response, pair: Optional[str]=None):
    return handle_request(response, "GET", "/fapi/v1/premiumIndex", get_params(symbol=pair))

@app.get("/funding-rate")
def get_funding_rate(response:Response, pair:Optional[str]=None,  startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 100):
    if limit > 1000:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 1000"}
    return handle_request(response, "GET", '/fapi/v1/fundingRate', get_params(symbol=pair, startTime=startTime, endTime=endTime, limit=limit))

@app.get("/24hr-stats")
def daily_stats(response:Response, pair:Optional[str]=None):
    return handle_request(response, "GET", '/fapi/v1/ticker/24hr', get_params(symbol=pair))

@app.get("/price")
def get_price(response:Response, pair:Optional[str]=None):
    return handle_request(response, "GET", '/fapi/v1/ticker/price', get_params(symbol=pair))

@app.get("/order-book")
def order_book(response:Response, pair:Optional[str]=None):
    return handle_request(response, "GET", '/fapi/v1/ticker/bookTicker', get_params(symbol=pair))

@app.get("/open-interest/{pair}")
def open_interest(response:Response, pair:str):
    return handle_request(response, "GET", '/fapi/v1/openInterest', get_params(symbol=pair))

@app.get("/open-interest-hist/{pair}/{period}")
def open_inyterest_hist(response:Response, pair:str, period:str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 30):
    if period not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Period is invalid. Allowed periods are "+str(PERIODS)}
    if limit > 500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 500"}
    return handle_request(response, "GET", "/futures/data/openInterestHist", get_params(symbol=pair, period=period, startTime=startTime, endTime=endTime, limit=limit))

@app.get("/top-long-short-account-ratio/{pair}/{period}")
def top_long_short_account_ratio(response:Response, pair:str, period:str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 30):
    if period not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Period is invalid. Allowed periods are "+str(PERIODS)}
    if limit > 500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 500"}
    return handle_request(response, "GET", "/futures/data/topLongShortAccountRatio", get_params(symbol=pair, period=period, startTime=startTime, endTime=endTime, limit=limit))

@app.get("/top-long-short-position-ratio/{pair}/{period}")
def top_long_short_position_ratio(response:Response, pair:str, period:str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 30):
    if period not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Period is invalid. Allowed periods are "+str(PERIODS)}
    if limit > 500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 500"}
    return handle_request(response, "GET", "/futures/data/topLongShortPositionRatio", get_params(symbol=pair, period=period, startTime=startTime, endTime=endTime, limit=limit))

@app.get("/global-long-short-account-ratio/{pair}/{period}")
def global_long_short_account_ratio(response:Response, pair:str, period:str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 30):
    if period not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Period is invalid. Allowed periods are "+str(PERIODS)}
    if limit > 500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 500"}
    return handle_request(response, "GET", "/futures/data/globalLongShortAccountRatio", get_params(symbol=pair, period=period, startTime=startTime, endTime=endTime, limit=limit))

@app.get("/taker-long-short-ratio/{pair}/{period}")
def taker_long_short_ratio(response:Response, pair:str, period:str, startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 30):
    if period not in PERIODS:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Period is invalid. Allowed periods are "+str(PERIODS)}
    if limit > 500:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 500"}
    return handle_request(response, "GET", "/futures/data/takerlongshortRatio", get_params(symbol=pair, period=period, startTime=startTime, endTime=endTime, limit=limit))

@app.get("/blvt-nav-candlestick/{pair}/{interval}")
def BLVT_NAV_candlestick(response:Response, pair:str, interval:str,  startTime: Optional[int]=None, endTime: Optional[int]=None, limit: Optional[int] = 500):
    if limit > 1000:
        response.status_code = status.HTTP_400_BAD_REQUEST
        return {'Error': "Maximum limit is 500"}
    keys = ["open-time", "open", "high", "low", "close", "real-leverage", "close-time", "ignore", "number-of-nav-update"]
    data = handle_request(response, "GET", "/fapi/v1/lvtKlines", get_params(symbol=pair, interval=interval, startTime=startTime, endTime=endTime, limit=limit))
    return arr2dict(data, keys)

@app.get("/index-info")
def index_info(response:Response, pair:Optional[str]=None):
    return handle_request(response, "GET", "/fapi/v1/indexInfo", get_params(symbol=pair))

@app.get("/asset-info")
def index_info(response:Response, pair:Optional[str]=None):
    return handle_request(response, "GET", "/fapi/v1/assetIndex", get_params(symbol=pair))


