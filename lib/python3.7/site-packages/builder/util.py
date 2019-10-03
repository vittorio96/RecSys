
import re
import sys
import itertools
import datetime as dt

import dateutil as du
import arrow
try:
    import pandas as pd
except ImportError:
    pd = None

def _parse_frequency(freq):
    '''
    '13H' -> return (13, 'H'), 'D' -> return (1, 'D')
    '''
    m = re.search(r"\s*(?P<multiplier>-?\d*)\s*(?P<frequency>[a-zA-Z_]*)\s*", freq)
    multiplier = int(m.group("multiplier")) if m.group("multiplier") else 1
    frequency = m.group("frequency")
    return (multiplier, frequency)

def convert_to_timedelta(time_val):
    '''
    Returns a timedelta object representing the corresponding timedelta for
    a given frequency. E.g.

        "5 minutes" -> datetime.timedelta(0, 300)
        "2h" -> datetime.timedelta(0, 7200)
        "4 days" -> datetime.timedelta(4)
    '''
    if not time_val:
        return None

    try:
        time_val = int(time_val)
    except ValueError:
        pass
    if time_val == 10:
        time_val = '10sec'
    elif time_val == 300:
        time_val = '5min'
    elif time_val == 3600:
        time_val = '1h'
    elif time_val == 86400:
        time_val = '1d'

    mult, freq = _parse_frequency(time_val)
    freq = freq.lower()

    matches = {
        'minute': [
            'm',
            't', # 'T' is minute in pandas
            'min',
            'mins',
            'minute',
            'minutes'
        ],
        'hour': [
            'h',
            'hour',
            'hours'
        ],
        'day': [
            'd',
            'day',
            'days'
        ],
        'month': [
            'month',
            'months'
        ],
        'second': [
            's',
            'sec',
            'secs',
            'second',
            'seconds'
        ]
    }

    if any(freq.endswith(match) for match in matches['minute']):
        return dt.timedelta(minutes=mult)
    elif any(freq.endswith(match) for match in matches['month']):
        return du.relativedelta.relativedelta(months=mult)
    elif any(freq.endswith(match) for match in matches['hour']):
        return dt.timedelta(hours=mult)
    elif any(freq.endswith(match) for match in matches['day']):
        return dt.timedelta(days=mult)

    # This check must go last because endswith 's' will short circuit endswith
    # 'hours', 'days', etc.
    elif any(freq.endswith(match) for match in matches['second']):
        return dt.timedelta(seconds=mult)

class BuilderArrowFactory(arrow.ArrowFactory):


    pipedream_expr = re.compile(r'((\d{4}-\d{1,2}-\d{1,2})|(\d{4}-\d{1,2}-\d{1,2}-\d{1,2})|(\d{4}-\d{1,2}-\d{1,2}-\d{1,2}-\d{1,2}))$')

    @classmethod
    def range(cls, frame, start, end, tz=None, limit=sys.maxsize,
        start_inclusive=True, end_inclusive=True):
        return BuilderArrow.range(frame, start, end, tz, limit,
        start_inclusive=start_inclusive, end_inclusive=end_inclusive)

    def get(self, *args, **kwargs):
        if pd is not None and len(args) == 1 and isinstance(args[0], pd.Timestamp):
            return super(BuilderArrowFactory, self).get(args[0])
        elif len(args) == 1 and isinstance(
            args[0], basestring
            ) and BuilderArrowFactory.pipedream_expr.match(args[0]):
            return super(BuilderArrowFactory, self).get(parse_datetime(
                args[0], time_sep='-'))
        else:
            return super(BuilderArrowFactory, self).get(*args, **kwargs)

BuilderArrowFactory.strptime = arrow.arrow.Arrow.strptime

class BuilderArrow(arrow.Arrow):

    @classmethod
    def range(cls, frame, start, end, tz=None, limit=sys.maxsize,
        start_inclusive=True, end_inclusive=True):
        if frame in cls._ATTRS:
            results = super(BuilderArrow, cls).range(frame, start=start,
                end=end, tz=tz, limit=limit)
        else:
            delta = convert_to_timedelta(frame)

            current = start
            results = []
            while current <= end and len(results) < limit:
                results.append(current)
                current += delta

        if (not start_inclusive) and (start in results):
            results.remove(start)
        if (not end_inclusive) and (start in results):
            results.remove(end)

        return results

    def span(self, frame):

        if frame in self._ATTRS:
            return super(BuilderArrow, self).span(frame)

        if frame is None:
            raise ValueError("Null timeframe not supported")
        delta = convert_to_timedelta(frame)
        if delta is None:
            raise ValueError("Incorrect timeframe requested")
        time_step = delta.total_seconds()

        floored_offset = self.timestamp % time_step
        floored = self.timestamp - floored_offset
        return (self.utcfromtimestamp(floored), self.utcfromtimestamp(floored + time_step))

    @property
    def pandas(self):
        return pd.Timestamp(self.datetime)

arrow_factory = BuilderArrowFactory(BuilderArrow)

def parse_datetime_timeperiod(date_str, time_sep='t'):
    '''
    Similar to ISO 8601, but allows for a custom separator between the day
    and hour values. We most commonly use this feature to specify a '-' as
    the separator.

    https://en.wikipedia.org/wiki/ISO_8601
    '''
    date_str = date_str.lower()

    arrow_re_string = r"(?P<year>\d{4})"
    arrow_re_string += r"(-(?P<month>\d{1,2}))?"
    arrow_re_string += r"(-(?P<day>\d{1,2}))?"
    arrow_re_string += r"({}(?P<hour>\d{{1,2}}))?".format(time_sep)
    arrow_re_string += r"(-(?P<minute>\d{1,2}))?"
    arrow_re_string += r"(-(?P<second>\d{1,2}))?"

    parsed = re.search(arrow_re_string, date_str)

    if parsed is None:
        # No date string could be found
        return None, None

    template = ''
    period = ''

    if parsed.group("year"):
        template += ('Y'*len(parsed.group("year")))
        period = 'year'
    if parsed.group("month"):
        template += ('-' + 'M'*len(parsed.group("month")))
        period = 'month'
    if parsed.group("day"):
        template += ('-' + 'D'*len(parsed.group("day")))
        period = 'day'
    if parsed.group("hour"):
        template += ('{}'.format(time_sep) + 'H'*len(parsed.group("hour")))
        period = 'hour'
    if parsed.group("minute"):
        template += ('-' + 'm'*len(parsed.group("minute")))
        period = 'minute'
    if parsed.group("second"):
        template += ('-' + 's'*len(parsed.group("second")))
        period = 'second'

    # Since arrow is just checking for integers in both the 'X' and 'YYYY'
    # templates we need to only use the correct template for the current
    # situation. If we find a string of consecutive numbers longer than 4
    # we know it's not a year number, so assume it's a unix timestamp.
    def _longest_consecutive_number_substring(s):
        is_number_str = lambda s: [c.isdigit() for c in s]
        groups = lambda s: itertools.groupby(is_number_str(s))
        return max(len(list(v)) if g else 0 for g, v in groups(s))

    longest_number_substring = _longest_consecutive_number_substring(date_str)
    if longest_number_substring > 4:
        template, period = 'X', 'unix'

    try:
        arrow_time = arrow.get(date_str, template)
        utc_timestamp = int(arrow_time.float_timestamp)
        return utc_timestamp, period
    except arrow.parser.ParserError:
        pass

    # Error
    return None, None


def parse_datetime(date_str, time_sep='t'):
    '''
    Just return the timestamp, ditch the period
    '''
    ts, period = parse_datetime_timeperiod(date_str, time_sep)
    return ts

def floor_timestamp_given_time_step(timestamp, time_step):
    ts = arrow_factory.get(timestamp)
    if time_step == 'month':
        return ts.floor('month')

    if isinstance(time_step, basestring):
        delta = convert_to_timedelta(time_step)
        time_step = delta.total_seconds()

    floored_offset = ts.timestamp % time_step
    floored = ts.timestamp - floored_offset
    return arrow_factory.get(floored)
