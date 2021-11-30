import datetime
import json


def get_at_callback():
    pass


def get_kyanda_callback( data):
    def process_details(details):
        bill_receipt = details.get('biller_Receipt', None)
        if bill_receipt:
            bill_receipt = bill_receipt['stringValue']

        tokens = details.get('tokens', None)
        if tokens:
            tokens = tokens['stringValue']

        units = details.get('units', None)
        if units:
            units = units['stringValue']

        return {
            'bill_receipt': bill_receipt,
            'tokens': tokens,
            'units': units
        }

    content = {
        'merchant_id': data['MerchantID']['stringValue'],
        'amount': data['amount']['stringValue'],
        'category': data['category']['stringValue'],
        'destination': data['destination']['stringValue'],
        'details': process_details(data['details']['mapValue']['fields']),
        'message': data['message']['stringValue'],
        'source': data['source']['stringValue'],
        'status': data['status']['stringValue'],
        'status_code': data['status_code']['stringValue'],
        'transactionDate': data['transactionDate']['stringValue'],
        'transactionRef': data['transactionRef']['stringValue'],
    }
    print(f'process_kyanda_callback:: content --> {content}')
    return content



operations = {
    '/at-callback': get_at_callback,
    '/kyanda-callback': get_kyanda_callback
}




def get_transaction_id(name):
    return name.split('/')[-1]


def process_status_stk_callback(result_code):
    if result_code == 0:
        status = 'PAID'
    elif result_code in [1032, 1031]:
        status = 'CANCELLED'
    elif result_code in [1037, 1036]:
        status = 'TIMEOUT'
    elif result_code == 2001:
        status = 'INVALID'
    elif result_code == 1001:
        status = 'LOCKED'
    elif result_code == 2026:
        status = 'EXPIRED'
    elif result_code == 26:
        status = 'REJECTED'
    elif result_code == 17:
        status = 'LIMITED'
    elif result_code == 1:
        status = 'INSUFFICIENT'
    elif result_code is None:
        status = 'INCOMPLETE'
    else:
        status = 'UNKNOWN'

    return status


def string_to_datetime(str_datetime):
    return datetime.datetime.strptime(str_datetime, "%Y-%m-%dT%H:%M:%S.%fZ")


def datetime_to_string(_datetime):
    return _datetime.strftime('%Y-%m-%d %H:%M:%S.%f')


def get_execution_time(create_time, update_time):
    if create_time and update_time:
        c_time = string_to_datetime(create_time)
        u_time = string_to_datetime(update_time)
        execution_time = (u_time - c_time).microseconds
        return execution_time, datetime_to_string(c_time), datetime_to_string(u_time)
    return -1


def process_callbacks(data, context):
    """ Triggered by a change to a Firestore document.
    Args:
        data (dict): The event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        value = data["value"]
        create_time = value.get('createTime', None)
        update_time = value.get('updateTime', None)
        fields = value.get('fields', None)
        if fields:
            ref = fields.get('ref', None)
            path = fields.get('path', None)
            _data = fields.get('data', None)
            if ref and path:
                ref = ref['stringValue']
                path = path['stringValue']
                if _data:
                    print(f'process_callbacks:: operations[path] --> {operations[path]}')
                    content = operations[path](_data['mapValue']['fields'])

        execution_time, create_time, update_time = get_execution_time(create_time, update_time)

        print(f'update_transaction:: value --> {json.dumps(data["value"])}')
        print(f'update_transaction:: execution_time --> {execution_time}')
        print(f'update_transaction:: fields --> {json.dumps(fields)}')
        print(f'update_transaction:: data --> {json.dumps(_data)}')
        print(f'update_transaction:: ref --> {json.dumps(ref)}')
        print(f'update_transaction:: path --> {json.dumps(path)}')

    except Exception as ex:
        print(f'update_transaction:: ex --> {ex}')


if '__main__' == __name__:
    print(process_callbacks(""))
