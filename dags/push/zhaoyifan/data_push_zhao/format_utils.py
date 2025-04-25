import pandas as pd
import string


def convert_timestamp_to_str(timestamp):
    if pd.isnull(timestamp):
        return None
    else:
        return str(timestamp)


def percent_convert(num):
    return str(round(num * 100, 2)) + '%'


def amount_convert(amount):
    if amount is not None:
        if amount >= 10000:
            result = f"{amount / 10000:.1f}万"
        else:
            result = f"{amount:.1f}元"
        return result
    else:
        return None


def col_convert(col_num):
    alphabeta = string.ascii_uppercase[:26]
    alphabeta = [i for i in alphabeta] + [i + j for i in alphabeta for j in alphabeta]
    return alphabeta[col_num - 1]
