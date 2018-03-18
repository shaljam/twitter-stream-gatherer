from datetime import datetime
from pytz import timezone, UTC


def beautiful_now(tehran=True):
    date = datetime.today()

    if tehran:
        date = date.replace(tzinfo=timezone('Asia/Tehran'))
        tz = 'Tehran'
    else:
        date = date.replace(tzinfo=UTC)
        tz = 'UTC'

    return date.strftime('%a %b %d %Y %H:%M:%S ' + tz)


print(beautiful_now())