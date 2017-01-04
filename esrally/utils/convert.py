def bytes_to_kb(b):
    return b / 1024.0 if b else b


def bytes_to_mb(b):
    return b / 1024.0 / 1024.0 if b else b


def bytes_to_gb(b):
    return b / 1024.0 / 1024.0 / 1024.0 if b else b


def mb_to_bytes(mb):
    return int(mb * 1024 * 1024) if mb else mb


def seconds_to_ms(s):
    return s * 1000 if s else s


def seconds_to_hour_minute_seconds(s):
    if s:
        hours = s // 3600
        minutes = (s - 3600 * hours) // 60
        seconds = s - 3600 * hours - 60 * minutes
        return hours, minutes, seconds
    else:
        return s, s, s


def ms_to_seconds(ms):
    return ms / 1000.0 if ms else ms


def ms_to_minutes(ms):
    return ms / 1000.0 / 60.0 if ms else ms


def to_bool(value):
    if value in ["True", "true", "Yes", "yes", "t", "y", "1", True]:
        return True
    elif value in ["False", "false", "No", "no", "f", "n", "0", False]:
        return False
    else:
        return ValueError("Cannot convert [%s] to bool." % value)

