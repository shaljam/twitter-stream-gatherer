from datetime import datetime, timezone


def beautiful_now():
    return datetime.today()\
        .replace(tzinfo=timezone.utc).astimezone(tz=None)\
        .strftime('%A, %b %d, %Y %H:%M:%S')