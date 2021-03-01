import boto3
import datetime, decimal, json
from boto3.dynamodb.conditions import Key, Attr

_RESOURCE_DYNAMODB = 'dynamodb'
_TABLE_NAME = 'market_signal_alert_policy'
_DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'
_DATABASE_KEY_USER = 'user'

_EVENT_KEY_QUERY_STRING_PARAMETER = 'queryStringParameters'
_EVENT_KEY_PATH_PARAMETER = 'pathParameters'
_PARAM_KEY_USERNAME = 'username'


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
        ret[key] = val
    return ret

def _get_policies(user_name):
    dynamodb = boto3.resource(_RESOURCE_DYNAMODB)
    table = dynamodb.Table(_TABLE_NAME)

    response = table.query(
        KeyConditionExpression=Key(_DATABASE_KEY_USER).eq(user_name),
    )

    items = response['Items']
    return items

def lambda_handler(event, context):
    path_parameters = event[_EVENT_KEY_PATH_PARAMETER]
    query_string_parameters = event[_EVENT_KEY_QUERY_STRING_PARAMETER]
    print("path_parameters:", path_parameters)
    print("query_string_parameters:", query_string_parameters)
    user_name = None
    
    if path_parameters:
        if _PARAM_KEY_USERNAME in path_parameters:
            user_name = path_parameters[_PARAM_KEY_USERNAME]
    
    '''    
    if query_string_parameters:
        if _PARAM_KEY_USERNAME in query_string_parameters:
            user_name = query_string_parameters[_PARAM_KEY_USERNAME]
    '''    
    
    items = _get_policies(user_name)

    return {
        'statusCode': 200,
        'body': json.dumps(list(map(lambda blob: dict_to_response(blob), items)), cls=DecimalEncoder)
    }
