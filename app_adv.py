# encoding: utf-8

"""app_adv.py <start_date> <end_date>

Fetch and compute details of good time slots for a given date range (inclusive) that are considered auspicious as per the Hindu Astrology.

Usage:
    app_adv.py <start_date> <end_date>
    app_adv.py (-h|--help)

Options:
    <start_date>    Date in yyyy-mmm-dd format
    <end_date>      Date in yyyy-mmm-dd format
    -h, --help      Show this message
"""

import datetime
import copy
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
location = None


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
            if '%Y' not in fmt:
                dt = dt.replace(year=given.year)
            if '%b' not in fmt:
                dt = dt.replace(month=given.month)
            if '%d' not in fmt:
                dt = dt.replace(day=given.day)
            return timezone.localize(dt)
        except:
            continue

    return None


# == Fetch and parse data

def parse_data(soup):
    contents = soup.find_all('div', class_='current-date-info')

    # important_timings
    important_timings = {}
    tag_text = ('Dur Muhurat', 'Amrit Kaal', 'Varjyam', 'Ganda Mool Nakshatra')
    spans = contents[2].findAll('span')
    for span in spans:
        if not span.text.strip() in tag_text: continue # filter out span with 'Nil' value
        key = span.text.strip()
        res = span.parent.findChildren('li') # returns a ResultSet and each element is a Tag
        values = []
        for li in res:
            v = re.sub(r'^\s*[0-9]\.\s*|\s*$', '', li.text, flags=re.UNICODE).strip()
            values.append(v if v != 'Nil' else [])
        important_timings[key] = values

    # Special - Abhijit Muhurtha
    key = 'Abhijit Muhurat'
    value = contents[2].find('h5', text=re.compile(key)).find_next('span').text.strip()
    important_timings[key] = value if value != 'Nil' else [] # it is value, not values, can be 'Nil' (str)

    # other_timings
    other_timings = {}
    trs = contents[2].findAll('tr')
    for tr in trs:
        key = tr.td.text.strip()
        value = tr.td.find_next('td').text.strip()
        other_timings[key] = value

    data = {}
    # data['panchang'] = panchang_data
    data['important_timings'] = important_timings
    data['other_timings'] = other_timings
    # data['sun_timings'] = sun_rise_set_data
    # data['moon_timings'] = moon_rise_set_data
    return data


def download_and_parse_data(url):
    headers = {
        'accept-encoding': 'gzip, deflate, br',
        'accept-language': 'en-US,en',
        'user-agent': 'Mozilla/5.0 AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.163 Safari/537.36',
        'accept': '*/*',
        'referer': 'https://www.prokerala.com/general/calendar/hinducalendar.php',
        'authority': 'www.prokerala.com'
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
            parenthesis_index = slist.rfind('(')
            if parenthesis_index > 0:
                slist = slist[:parenthesis_index].strip()
            s = slist.split(u'–')
            if len(s) != 2: continue
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


def sort_and_normalize(intervals, given_start_dt, next_day):
    intervals.sort()
    # E.g. 2019-04-30 08:35:00 - 2019-04-30 09:34:00, 2019-04-30 09:19:00 - 2019-04-30 11:08:00
    # remove invalid intervals - i.e. any interval before the given_start_dt
    clean_intervals = []
    for interval in intervals:
        if interval.stop < given_start_dt: continue
        if interval.start < given_start_dt:
            interval.start = given_start_dt
        if interval.start < next_day and interval.stop > next_day:
            interval.stop = next_day
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

    # merge all non-free intervals.
    merged_intervals = []

    # if you do not add the end datetime, the computation will not show the free time after the last interval until the
    # next day
    merged_intervals.append(Interval(next_day - datetime.timedelta(seconds=1), next_day))

    for key in timezones:
        merged_intervals.extend(copy.deepcopy(day[key])) # deepcopy, else it would be a shallow copy and
                                                         # sort_and_normalize method below would modify the contents.

    merged_intervals = sort_and_normalize(merged_intervals, given, next_day)

    for interval in merged_intervals:
        if given >= next_day: break # some intervals will be on next day and we do not need to compute free time for those
        delta = interval.start - given
        if delta.seconds > 0:
            free_time_intervals.append(Interval(given, interval.start))  # go back one second (substract timedelta)
        given = interval.stop  # go forward 1 second (add timedelta)

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

    start_date = args.get('<start_date>')
    end_date = args.get('<end_date>')
    today = timezone.localize(datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))

    if len(start_date) > 0:
        start_date = re.sub(r'[^\w\s]', '', start_date)
        start_date = try_strptime(start_date, today)
    else:
        print("Start date cannot be empty.")
        return 2

    if start_date is None:
        start_date = today

    if len(end_date) > 0:
        end_date = re.sub(r'[^\w\s]', '', end_date)
        end_date = try_strptime(end_date, today)
    else:
        print("End date cannot be empty.")
        return 3

    if end_date is None:
        end_date = today

    if (start_date > end_date):
        temp = start_date
        start_date = end_date
        end_date = temp

    log.debug('date: {!r} - {!r}'.format(start_date, end_date))

    filename = '{}-to-{}.csv'.format(start_date.strftime('%a-%d-%b-%Y'), end_date.strftime('%a-%d-%b-%Y'))
    csv_headers = 'Date, Free, Amrit Kaal, Abhijit Muhurat, Rahu, Dur Muhurat, Varjyam, Yamaganda, Gulika, Ganda Mool Nakshatra\n'
    seperator = ','
    oneday = datetime.timedelta(seconds=86400)

    temp_date = copy.deepcopy(start_date)

    intervals = {}

    # order of keys decide the order of results.
    keys = ['Free', 'Amrit Kaal', 'Abhijit Muhurat', 'Rahu', 'Dur Muhurat', 'Varjyam', 'Yamaganda', 'Gulika', 'Ganda Mool Nakshatra']

    with open(filename, 'w+') as f:
        f.write(csv_headers)

        while temp_date <= end_date:
            cache_name = temp_date.strftime('%Y-%b-%d')
            cache_ttl = config['calendar']['cachettl']
            url = config["calendar"]["urltemplate"].format(temp_date.year, temp_date.month, temp_date.day,
                                                           location['num'])
            log.debug("URL: {!r}".format(url))

            args = [temp_date, url]
            intervals[temp_date] = wf.cached_data(cache_name, get_data_helper, max_age=cache_ttl, data_func_args=args)

            data = '{}{}'.format(temp_date.strftime('%Y-%b-%d'), seperator)

            # Good time
            for key in keys:
                try:
                    for interval in intervals[temp_date][key]:
                        # compute delta hours and mins
                        delta = interval.stop - interval.start
                        seconds = delta.days * 86400 + delta.seconds
                        hours = seconds // 3600
                        minutes = (seconds // 60) % 60
                        title = '{} - {} ({}h {}m)'.format(interval.start.strftime('%I:%M %p'), interval.stop.strftime('%I:%M %p'),
                                                           hours, minutes)
                        subtitle = interval.start.strftime('%a, %b %d, %Y')
                        if seconds >= 86400:
                            subtitle = '{} to {}'.format(subtitle, interval.stop.strftime('%a, %b %d, %Y'))

                        data = '{} > {}'.format(data, title)
                    data = '{}{}'.format(data, seperator)
                except:
                    log.debug("Key not found: {}".format(key))

            temp_date = temp_date + oneday # part of the while loop
            f.write('{}\n'.format(data))
    # send results to Alfred as JSON
    wf.send_feedback()
    return 0


if __name__ == u'__main__':
    wf = Workflow3()
    log = wf.logger
    config = yaml.safe_load(open('config.yml'))
    local_settings = yaml.safe_load(open('.local.yml'))
    chosen_location = 'bangalore' if 'chosen_timezone' not in \
                                     local_settings.keys() else \
                                     local_settings['chosen_timezone'].strip()
    location = config['location'][chosen_location]
    timezone = pytz.timezone(location['tz'])
    sys.exit(wf.run(main))
