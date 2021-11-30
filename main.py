import datetime


def bq_create_transaction(vals):
    try:
        print(f'create_transaction:: vals --> {vals}')
        client = bigquery.Client()
        dataset_ref = client.dataset('rfm')

        table_ref = dataset_ref.table('raw_data')
        table = client.get_table(table_ref)

        rows_to_insert = [vals]

        errors = client.insert_rows_json(table, rows_to_insert)  # API request
        print(f'create_transaction:: errors --> {errors}')
        assert errors == []
    except Exception as ex:
        print(f'create_transaction:: ex --> {ex}')


def get_transaction_id(name):
    return name.split('/')[-1]


def validate_carrier(phone_number):
    try:
        if phone_number:
            ke_number = phonenumbers.parse(phone_number, "KE")
            if ke_number:
                _carrier = carrier.name_for_number(ke_number, "en")
                if _carrier == 'JTL':
                    _carrier = 'FAIBA'
                return _carrier.upper()

        return None
    except Exception as ex:
        print(f'validate_carrier:: ex --> {ex}')
        return None


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


def process_transaction(transaction):
    try:
        transaction_id = get_transaction_id(transaction['name'])
        phone_number = int(transaction["fields"]["phone_number"]["stringValue"])
        amount_paid = int(transaction["fields"]["amount_paid"]["integerValue"])
        amount = int(transaction["fields"]["amount"]["stringValue"])
        transaction_type = transaction["fields"]["transaction_type"]["stringValue"]
        vendor = transaction["fields"]["vendor"].get("stringValue", None)
        if transaction_type == 'AIRTIME':
            _carrier = validate_carrier(transaction["fields"]["other_phone_number"]["stringValue"])
        else:
            _carrier = transaction_type

        vals = {
            'id': transaction_id,
            'user_id': phone_number,
            'amount_paid': amount_paid,
            'amount': amount,
            'type': transaction_type,
            'vendor': vendor,
            'carrier': _carrier,
        }
        return vals
    except Exception as ex:
        print(f'process_transaction:: ex --> {ex}')
        return None


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


def process_stk_callback(data):
    vals = process_transaction(data["value"])
    vals['status'] = process_status_stk_callback(
        int(data["value"]["fields"]["stk_callback"]['mapValue']['fields']['ResultCode']['integerValue']))
    return vals


def process_bonga_transaction(data):
    vals = process_transaction(data["value"])
    bonga_airtime_response = data['value']['fields']['bonga_airtime_response']['mapValue']['fields']
    _status_message = bonga_airtime_response.get('status_message', None)
    if _status_message:
        vals['status'] = _status_message['stringValue'].upper()
    else:
        vals['status'] = 'FAILED'
    return vals


def process_kyanda_ipn(data):
    vals = process_transaction(data["value"])
    kyanda_ipn_transaction = data['value']['fields']['kyanda_ipn_transaction']['mapValue']['fields']
    _status = kyanda_ipn_transaction.get('status', None)
    if _status:
        vals['status'] = _status['stringValue'].upper()
    else:
        vals['status'] = 'FAILED'
    return vals


def update_bq(create_time, update_time, execution_time, vals):
    vals['create_time'] = create_time
    vals['update_time'] = update_time
    vals['duration'] = execution_time
    bq_create_transaction(vals)


def process_callbacks(data, context):
    """ Triggered by a change to a Firestore document.
    Args:
        data (dict): The event payload.
        context (google.cloud.functions.Context): Metadata for the event.
    """
    try:
        value = data["value"]
        update_mask = data["updateMask"]
        create_time = value.get('createTime', None)
        update_time = value.get('updateTime', None)
        execution_time, create_time, update_time = get_execution_time(create_time, update_time)
        print(f'update_transaction:: value --> {value}')
        print(f'update_transaction:: update_mask --> {update_mask}')
        print(f'update_transaction:: execution_time --> {execution_time}')
        if 'stk_callback' in update_mask['fieldPaths']:
            # TODO: check for c2b_callback
            vals = process_stk_callback(data)
            status = vals.get('status', None)
            if status and status != 'PAID':
                update_bq(create_time, update_time, execution_time, vals)
        elif 'bonga_airtime_response.status' in update_mask['fieldPaths']:
            vals = process_bonga_transaction(data)
            update_bq(create_time, update_time, execution_time, vals)
        elif 'kyanda_ipn_transaction' in update_mask['fieldPaths']:
            vals = process_kyanda_ipn(data)
            update_bq(create_time, update_time, execution_time, vals)


    except Exception as ex:
        print(f'update_transaction:: ex --> {ex}')


if '__main__' == __name__:
    print(process_transaction(""))
