from datetime import datetime
import pendulum

import logging

logger = logging.getLogger(__name__)


def timestamp_now():
    return int(datetime.now().timestamp() * 1000)


