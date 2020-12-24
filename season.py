# scraper.py
# This contains the functionality to pull down data from NHL.com's various APIs.
# Data are configured and returned as pandas dataframes.
########################################################

import pandas as pd
from ratelimit import limits, sleep_and_retry

from datetime import datetime
import requests

REQUEST_DELAY = 3  # seconds between API calls


@sleep_and_retry
@limits(calls=1, period=REQUEST_DELAY)
def _request(uri):
    print(f"request {uri}\n\tat {datetime.now()}")
    r = requests.get(uri)
    return r.json()


def scrape_season(season):
    """Gathers all data for the given season from the appropriate NHL API.

    season: the first year of the desired season (e.g. '2019' for 2019-2020 season)"""
    # constants
    data_limit = 100  # number of players fetched per call
    player_index = 0  # first player index of the next call
    base_uri = 'https://api.nhle.com/stats/rest/en/skater/summary'
    uri_args = [
        'isAggregate=false',
        'isGame=false',
        f'limit={data_limit}',
        'factCayenneExp=gamesPlayed%3E=1',
        f'cayenneExp=gameTypeId=2%20and%20seasonId={season + str(int(season) + 1)}'
    ]

    # the first call
    uri = '?'.join([base_uri, '&'.join(uri_args)])
    r = _request(uri + f"&start={player_index}")
    output = r['data']
    total = r['total']

    # the remaining calls
    player_index += data_limit
    while player_index <= total:
        r = _request(uri + f"&start={player_index}")
        output += r['data']
        player_index += data_limit

    return pd.DataFrame(output)


if __name__ == "__main__":
    data = scrape_season('2019')
    print(data.shape)
    print(data.head())
