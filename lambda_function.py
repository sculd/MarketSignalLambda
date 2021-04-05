import boto3
import datetime, decimal, json, pytz, threading
from boto3.dynamodb.conditions import Key, Attr
import os, requests

_POLYGON_API_KEY = os.getenv('API_KEY_POLYGON')
_URL_FORMAT = "https://api.polygon.io/v1/last/stocks/{symbol}?&apiKey={api_key}"

_BINANCE_URL_FORMAT = 'https://api.binance.com/api/v3/avgPrice?symbol={symbol}'

_TIMEZONE_EASTERN = pytz.timezone('US/Eastern')

_RESOURCE_DYNAMODB = 'dynamodb'
_TABLE_NAME = 'financial_signal'
_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'

_EVENT_KEY_QUERY_STRING_PARAMETER = 'queryStringParameters'
_PARAM_KEY_FROM = 'from'
_PARAM_KEY_TO = 'to'
_PARAM_KEY_MARKET = 'market'
_PARAM_KEY_SYMBOL = 'symbol'
_DATABASE_KEY_MARKET = 'market'
_DATABASE_KEY_DATE_ET = 'date_et'
_DATABASE_KEY_TIMESTAMP = 'timestamp'
_DATABASE_KEY_MIN_DROP = 'min_drop'
_DATABASE_KEY_MAX_JUMP = 'max_jump'
_RESPONSE_KEY_DATE = 'date'
_RESPONSE_KEY_DATETIME = 'datetime'


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return int(obj)
        elif isinstance(obj, datetime.datetime):
            return obj.strftime(_DATETIME_FORMAT)
        # Let the base class default method raise the TypeError
        return json.JSONEncoder.default(self, obj)


def dict_to_response(blob):
    ret = {}
    for k, v in blob.items():
        key = k
        val = v
        if k == _DATABASE_KEY_MIN_DROP or k == _DATABASE_KEY_MAX_JUMP:
            val = float(v)

        if k == _DATABASE_KEY_DATE_ET:
            key = _RESPONSE_KEY_DATE
        elif k == _DATABASE_KEY_TIMESTAMP:
            key = _RESPONSE_KEY_DATETIME
            val = _TIMEZONE_EASTERN.localize(datetime.datetime.fromtimestamp(int(v)))
        ret[key] = val
    return ret


def _get_items(date_str, from_epoch, to_epoch, market, symbol):
    dynamodb = boto3.resource(_RESOURCE_DYNAMODB)
    table = dynamodb.Table(_TABLE_NAME)

    if symbol:
        response = table.query(
            KeyConditionExpression=Key(_DATABASE_KEY_DATE_ET).eq(date_str) & Key(_DATABASE_KEY_TIMESTAMP).between(
                from_epoch, to_epoch),
            FilterExpression=Attr(_PARAM_KEY_SYMBOL).eq(symbol)
        )
    else:
        response = table.query(
            KeyConditionExpression=Key(_DATABASE_KEY_DATE_ET).eq(date_str) & Key(_DATABASE_KEY_TIMESTAMP).between(
                from_epoch, to_epoch)
        )

    items = response['Items']
    items = [i for i in items if i[_DATABASE_KEY_MARKET] == market]
    items = [i for i in items if i[_DATABASE_KEY_TIMESTAMP] >= from_epoch and i[_DATABASE_KEY_TIMESTAMP] <= to_epoch]
    return items

def _get_recent_price_crypto(symbol):
    url = _BINANCE_URL_FORMAT.format(symbol = symbol)
    r = requests.get(url)
    if not r.ok:
        print(r.reason)
        return 0
    js = r.json()
    return round(float(js['price']), 2)


def _get_recent_price_symbol(symbol):
    print('getting the recent price of {s}'.format(s=symbol))
    url = _URL_FORMAT.format(symbol=symbol, api_key=_POLYGON_API_KEY)
    r = requests.get(url)
    if not r.ok:
        print(r.reason)
        return 0
    js = r.json()
    if 'last' not in js:
        print('the key last not in', js)
        return 0
    if 'price' not in js['last']:
        print('the key price not in the last price', js)
        return 0
    return js['last']['price']


def _get_recent_price(recent_prices, recent_prices_lock, i, market, symbol):
    with recent_prices_lock:
        if market == 'crypto':
            recent_prices[i] = _get_recent_price_crypto(symbol)
        else:
            recent_prices[i] = _get_recent_price_symbol(symbol)

def _add_recent_prices(market, result):
    recent_prices = [0 for _ in result]
    recent_prices_lock = threading.Lock()
    threads = list()

    b = 0
    batch_size = 20
    while b * batch_size < len(result):
        print('batch:', b)
        b_head = b * batch_size
        for bi, entry in enumerate(result[b_head : b_head + batch_size]):
            x = threading.Thread(target=_get_recent_price,
                                 args=(recent_prices, recent_prices_lock, b_head + bi, market, entry['symbol'],))
            threads.append(x)
            x.start()

        for thread in threads:
            thread.join()
        b += 1

    for i, entry in enumerate(result):
        entry['recent_price'] = recent_prices[i]

def lambda_handler(event, context):
    query_string_parameters = event[_EVENT_KEY_QUERY_STRING_PARAMETER]
    print("query_string_parameters:", query_string_parameters)
    market = 'stock'
    symbol = None
    from_epoch = int((datetime.datetime.now() - datetime.timedelta(hours=12)).timestamp())
    to_epoch = int(datetime.datetime.now().timestamp())

    if query_string_parameters:
        if _PARAM_KEY_MARKET in query_string_parameters:
            market = query_string_parameters[_PARAM_KEY_MARKET]

        if _PARAM_KEY_SYMBOL in query_string_parameters:
            symbol = query_string_parameters[_PARAM_KEY_SYMBOL]

        if _PARAM_KEY_FROM in query_string_parameters:
            t = datetime.datetime.strptime(query_string_parameters[_PARAM_KEY_FROM], _DATETIME_FORMAT)
            from_epoch = int(t.timestamp())

        if _PARAM_KEY_TO in query_string_parameters:
            t = datetime.datetime.strptime(query_string_parameters[_PARAM_KEY_TO], _DATETIME_FORMAT)
            to_epoch = int(t.timestamp())

    print("market:", market)
    print("from_epoch:", from_epoch, ", to_epoch:", to_epoch)
    items = []
    t = datetime.datetime.fromtimestamp(from_epoch)
    date_str = t.strftime('%Y-%m-%d')
    date_str_to = datetime.datetime.fromtimestamp(to_epoch).strftime('%Y-%m-%d')
    print("date_str_to:", date_str_to)
    while True:
        print("date_str:", date_str)
        items += _get_items(date_str, from_epoch, to_epoch, market, symbol)
        if date_str == date_str_to:
            break
        t += datetime.timedelta(days=1)
        date_str = t.strftime('%Y-%m-%d')

    result = list(map(lambda blob: dict_to_response(blob), items))
    result.sort(key=lambda blob: blob[_RESPONSE_KEY_DATETIME], reverse=True)
    result = result[:30]
    _add_recent_prices(market, result)
    return {
        'statusCode': 200,
        'headers': {
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'OPTIONS,POST,GET'
        },
        'body': json.dumps(result, cls=DecimalEncoder)
    }
