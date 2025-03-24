import pendulum

def timestamp2datetime(timestamp):
    try:
        timestamp = int(timestamp)
        if timestamp == 0:
            return None
        else:
            return pendulum.from_timestamp(timestamp / 1000, tz='Asia/Shanghai')
    except BaseException as e:
        return None