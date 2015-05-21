import string
import math


def make_boolean(bool_string):
    if not isinstance(bool_string, str):
        if bool_string:
            return True
        else:
            return False

    if bool_string.strip().lower() in ['y', 'yes', 't', 'true']:
        return True
    elif bool_string.strip().lower() in ['n', 'no', 'f', 'false']:
        return False
    else:
        raise Exception('Not a boolean string')


def parse_int(value):
    return int(parse_int_remove_nan(value))


def parse_string(value):
    return string.strip(str(value))


def parse_string_remove_nan(value):
    value = str(value)
    if value == 'nan':
        return ''
    else:
        return value


def parse_int_remove_nan(value):
    if math.isnan(value):
        return -1.0
    else:
        return value


def parse_date_remove_nan(value):
    if math.isnan(value):
        return None
    else:
        return value


def parse_float(value):
    return float(parse_string_remove_nan(value))


def parse_boolean(value):
    return make_boolean(str(value))


def parse_datetime(value):
    if len(string.strip(str(value))) == 0:
        return None


def is_integer():
    return (parse_int, )


def is_string():
    return (parse_string, )


def is_float():
    return (parse_float, )


def is_datetime():
    return (parse_datetime, )


def is_boolean():
    return (parse_boolean, )


def is_foreign_key(ParentCSVModel, backref=None):
    return (parse_int, ParentCSVModel, backref)
