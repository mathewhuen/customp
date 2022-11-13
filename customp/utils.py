import signal


def get(val, default=None):
    """Return the given value, replace with default if None"""
    return val if val is not None else default


def nmax(*xs):
    """Ignoring NoneType entries, calculate max of xs. Return -inf if all None"""
    m = -float('inf')
    for x in xs:
        if x is None:
            continue
        m = max(m, x)
    return m


def signal_override():
    """Force the current process to ignore ctrl-c"""
    signal.signal(signal.SIGINT, signal.SIG_IGN)

