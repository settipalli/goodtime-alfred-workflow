# encoding: utf-8

"""app.py [<date>]

Fetch and compute details of time slots in a day that are considered auspicious as per the Hindu Astrology.

Usage:
    app.py [<date>]...
    app.py (-h|--help)

Options:
    [<date>]...   Date in yyyy-mmm-dd format
    -h, --help    Show this message
"""

import datetime
import os
import re
import sys
from functools import total_ordering
from pprint import pformat

import pytz
import requests
import yaml
from bs4 import BeautifulSoup
from docopt import docopt
from workflow import Workflow3

log = None
config = None
timezone = None


# == Interval data model

@total_ordering
class Interval:
    # datetime interval (start, stop)
    def __init__(self, start, stop):
        self.start = start
        self.stop = stop

    def duration(self):
        return self.stop - self.start

    # compareTo
    def __eq__(self, other):
        return self.start == other.start and self.stop == other.stop

    def __lt__(self, other):
        if (self.start == other.start):
            return self.stop < other.stop
        return self.start < other.start

    def __str__(self):
        return '{} - {}'.format(self.start, self.stop)

    def __repr__(self):
        return r'{} - {}'.format(self.start, self.stop)

    def __unicode__(self):
        return u'{} - {}'.format(self.start, self.stop)


# == Helper routines

def try_strptime(s, given, fmts=None):
    fmts = fmts if fmts else ['%I:%M %p', '%b %d %I:%M %p', '%b%d', '%d%b', '%b%d%Y', '%Y%b%d', '%d%b%Y', '%d%Y%b']
    for fmt in fmts:
        try:
            dt = datetime.datetime.strptime(s, fmt)
            if (dt.year <= 1900 and dt.year != given.year): dt = dt.replace(year=given.year)
            if (dt.month == 1 and dt.month != given.month): dt = dt.replace(month=given.month)
            if (dt.day == 1 and dt.day != given.day): dt = dt.replace(day=given.day)
            return timezone.localize(dt)
        except:
            continue

    return None


# == Fetch and parse data

def parse_data(soup):
    master_data = soup.select('div.cal-box.calendar-box')

    panchang = master_data[0].find('table')
    important_timings = master_data[2].find_all('div')
    other_timings = master_data[3].find_all('div')
    sun_rise_set = master_data[4]
    moon_rise_set = master_data[5]

    # ------------

    panchang_data = {}
    for row in panchang.find_all('tr'):
        first_column = row.find('th').contents[0].strip()
        second_column = []

        for div in row.find('td').find_all('div'):
            second_column.append(' '.join(div.text.split()))

        if len(second_column) == 0:
            second_column = [row.select('td')[0].contents[0].strip()]

        panchang_data[first_column] = second_column

    # ------------

    important_timings_data = {}
    for div in important_timings:
        try:
            key = div.span.text.strip()
            values = []
            for li in div.ul.find_all('li'):
                values.append(re.sub(r'^\s*[0-9]\.\s*|\s*$', '', li.text, flags=re.UNICODE))
            important_timings_data[key] = values
        except:
            pass

    # ------------

    other_timings_data = {}

    for row in other_timings[0].find('table').find_all('tr'):
        first_column = row.find('th').contents[0].strip()
        second_column = row.find('td').contents[0].strip()
        other_timings_data[first_column] = second_column

    # Abhijit Muhurata
    key = other_timings[1].find('h5').text.strip()
    value = [other_timings[1].contents[2].strip()]
    important_timings_data[key] = value

    # ------------

    sun_rise_set_data = {}

    sun_rise_data = sun_rise_set.find('div', class_='day-sunrise')
    key = sun_rise_data.span.contents[0].strip()
    value = sun_rise_data.span.contents[2].strip()

    sun_rise_set_data[key] = value

    sun_set_data = sun_rise_set.find('div', class_='day-sunset')
    key = sun_set_data.span.contents[0].strip()
    value = sun_set_data.span.contents[2].strip()

    sun_rise_set_data[key] = value

    # ------------

    moon_rise_set_data = {}

    moon_rise_data = moon_rise_set

    moon_rise_data = moon_rise_set.find('div', class_='day-moonrise')
    key = moon_rise_data.span.contents[0].strip()
    value = moon_rise_data.span.contents[2].strip()

    moon_rise_set_data[key] = value

    moon_set_data = moon_rise_set.find('div', class_='day-moonset')
    key = moon_set_data.span.contents[0].strip()
    value = moon_set_data.span.contents[2].strip()

    moon_rise_set_data[key] = value

    # ------------

    data = {}
    data['panchang'] = panchang_data
    data['important_timings'] = important_timings_data
    data['other_timings'] = other_timings_data
    data['sun_timings'] = sun_rise_set_data
    data['moon_timings'] = moon_rise_set_data

    return data


def download_and_parse_data(url):
    headers = {
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en',
        'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/73.0.3683.103 Safari/537.36',
        'accept': '*/*',
        'referer': 'https://www.prokerala.com/general/calendar/hinducalendar.php',
        'authority': 'www.prokerala.com',
        'x-requested-with': 'XMLHttpRequest'
    }

    data = {}
    with requests.Session() as s:
        s.headers.update(headers)
        r = s.get(url)
        soup = BeautifulSoup(r.text, 'html.parser')
        data = parse_data(soup)

    return data


def build_intervals(data, date):
    """ create intervals from data and return them """
    day = {}
    for k, l in data['important_timings'].items():
        intervals = []
        for slist in l:
            s = slist.split(u'–')
            if (len(s) != 2): continue
            x = try_strptime(s[0].strip(), date)
            y = try_strptime(s[1].strip(), date)
            i = Interval(x, y)
            intervals.append(i)
        intervals.sort()
        day[k] = intervals

    for k, v in data['other_timings'].items():
        intervals = []
        s = v.split(u'–')
        x = try_strptime(s[0].strip(), date)
        y = try_strptime(s[1].strip(), date)
        i = Interval(x, y)
        intervals.append(i)
        intervals.sort()
        day[k] = intervals

    return day


def sort_and_normalize(intervals, given_start_dt):
    intervals.sort()
    # E.g. 2019-04-30 08:35:00 - 2019-04-30 09:34:00, 2019-04-30 09:19:00 - 2019-04-30 11:08:00
    # remove invalid intervals - i.e. any interval before the given_start_dt
    clean_intervals = []
    for interval in intervals:
        if interval.stop < given_start_dt: continue
        if interval.start < given_start_dt:
            interval.start = given_start_dt
        clean_intervals.append(interval)

    x = clean_intervals[0]
    normalized_intervals = []
    for y in clean_intervals[1:]:
        if y.start < x.stop:
            if y.stop > x.stop:
                x.stop = y.stop
            else:
                pass  # if y is in between x, do nothing
        else:
            # y and x do not overlap
            normalized_intervals.append(x)
            x = y

    normalized_intervals.append(x)
    return normalized_intervals


def find_free_time(day, given):
    next_day = given + datetime.timedelta(seconds=86400)
    free_time_intervals = []

    timezones = ['Rahu', 'Yamaganda', 'Gulika', 'Dur Muhurat', 'Varjyam']

    merged_intervals = []
    # adding a boundary condition so that the computation shows free time after the last interval
    merged_intervals.append(Interval(next_day, next_day + datetime.timedelta(seconds=1)))

    for key in timezones:
        merged_intervals.extend(day[key])

    merged_intervals = sort_and_normalize(merged_intervals, given)

    for interval in merged_intervals:
        delta = interval.start - given
        if (delta.seconds > 0):
            free_time_intervals.append(Interval(given, interval.start - datetime.timedelta(
                seconds=1)))  # go back one second (substract timedelta)
        given = interval.stop + datetime.timedelta(seconds=1)  # go forward 1 second (add timedelta)

    free_time_intervals.sort()
    return free_time_intervals


def get_data_helper(date, url):
    data = download_and_parse_data(url)
    day_intervals = build_intervals(data, date)
    free_time_intervals = find_free_time(day_intervals, date)
    results = {'Free': free_time_intervals}
    results.update(day_intervals)
    return results


def main(wf):
    args = docopt(__doc__, wf.args, version='v0.9.0')
    log.debug('args : {!r}'.format(args))

    date = args.get('<date>')
    today = timezone.localize(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))

    if len(date) > 0:
        date = re.sub(r'[^\w\s]', '', date)
        date = try_strptime(date, today)
    else:
        date = today

    if date is None:
        date = today

    log.debug('date: {!r}'.format(date))

    cache_name = date.strftime('%Y-%b-%d')
    cache_ttl = config['calendar']['cachettl']
    url = config["calendar"]["urltemplate"].format(date.year, date.month, date.day,
                                                   config['location']['london']['num'])
    log.debug("URL: {!r}".format(url))

    args = [date, url]
    intervals = wf.cached_data(cache_name, get_data_helper, max_age=cache_ttl, data_func_args=args)

    wf.add_item(
        title=u'Copy to clipboard',
        icon=os.path.join('icons', 'clipboard.png'),
        arg=unicode(pformat(intervals, indent=2)), # tell alfred to pass the url to the next action in the workflow
        valid=True
    )

    wf.add_item(
        title=u'Open calendar',
        subtitle=unicode(url),
        icon='icon.png',
        arg='https://www.prokerala.com/general/calendar/hinducalendar.php', # tell alfred to pass the url to the next action in the workflow
        valid=True
    )

    icons = {
        'Free': os.path.join('icons', config['icon']['positive']),
        'Rahu': os.path.join('icons', config['icon']['negative']),
        'Dur Muhurat': os.path.join('icons', config['icon']['negative']),
        'Varjyam': os.path.join('icons', config['icon']['negative']),
        'Yamaganda': os.path.join('icons', config['icon']['negative']),
        'Gulika': os.path.join('icons', config['icon']['negative']),
        'Amrit Kaal': os.path.join('icons', config['icon']['positive']),
        'Abhijit Muhurat': os.path.join('icons', config['icon']['positive'])
    }

    # order of keys decide the order of results.
    keys = ['Free', 'Amrit Kaal', 'Abhijit Muhurat', 'Rahu', 'Dur Muhurat', 'Varjyam', 'Yamaganda', 'Gulika']
    # Good time
    for key in keys:
        try:
            for interval in intervals[key]:
                # compute delta hours and mins
                seconds = (interval.stop - interval.start).seconds
                hours = seconds / 60 / 60
                minutes = (seconds / 60) - (hours * 60)
                day, date, month, year = interval.start.strftime('%a-%d-%b-%Y').split('-')
                title = '{} - {} ({}h {}m)'.format(interval.start.strftime('%I:%M %p'), interval.stop.strftime('%I:%M %p'),
                                                   hours, minutes)
                wf.add_item(
                    title=unicode(title),
                    subtitle=unicode('{} - {}, {} {}, {}'.format(key, day, month, date, year)),
                    icon=icons[key]
                )
        except:
            log.debug("Key not found: {}".format(key))

    # send results to Alfred as JSON
    wf.send_feedback()
    return 0


if __name__ == u'__main__':
    wf = Workflow3()
    log = wf.logger
    config = yaml.safe_load(open("config.yml"))
    timezone = pytz.timezone(config['location']['london']['tz'])
    sys.exit(wf.run(main))
