# scraper.py
# This contains the functionality to pull down data from NHL.com's various APIs.
# Data are configured and returned as pandas dataframes.
########################################################

import pandas as pd
from ratelimit import limits, sleep_and_retry

import requests

REQUEST_DELAY = 3  # seconds between API calls


@sleep_and_retry
@limits(calls=1, period=REQUEST_DELAY)
def _request(uri):
    r = requests.get(uri)
    return r.json()


def scrape_season(season):
    """Gathers all data for the given season from the appropriate NHL APIs.

    season: the first year of the desired season (e.g. '2019' for 2019-2020 season)"""
    skaters = scrape_skaters(season)
    return skaters


def scrape_skaters(season):
    """Gathers all skater data for the given season from the appropriate NHL APIs.

    season: the first year of the desired season (e.g. '2019' for 2019-2020 season)"""
    final_cols = [
        "season",
        "team",
        "team2",
        "team3",
        "games_played",
        "goals",
        "ev_goals",
        "pp_goals",
        "sh_goals",
        "en_goals",
        "gw_goals",
        "ot_goals",
        "goals_back",
        "goals_defl",
        "goals_slap",
        "goals_snap",
        "goals_tip",
        "goals_wrap",
        "goals_wrist",
        "assists",
        "ev_assists",
        "pp_assists",
        "sh_assists",
        "en_assists",
        "points",
        "ev_points",
        "pp_points",
        "sh_points",
        "en_points",
        "plus_minus",
        "ev_goals_for",
        "ev_goals_against",
        "pp_goals_for",
        "pp_goals_against",
        "sh_goals_for",
        "sh_goals_against",
        "shots",
        "shots_back",
        "shots_defl",
        "shots_slap",
        "shots_snap",
        "shots_tip",
        "shots_wrap",
        "shots_wrist",
        "misses",
        "misses_cross",
        "misses_high",
        "missed_post",
        "missed_wide",
        "blocks",
        "giveaways",
        "hits",
        "takeaways",
        "faceoffs",
        "fo_wins",
        "fo_losses",
        "dz_fo",
        "dz_fo_wins",
        "dz_fo_losses",
        "nz_fo",
        "nz_fo_wins",
        "nz_fo_losses",
        "oz_fo",
        "oz_fo_wins",
        "oz_fo_losses",
        "ev_fo",
        "ev_fo_wins",
        "ev_fo_losses",
        "pp_fo",
        "pp_fo_wins",
        "pp_fo_losses",
        "sh_fo",
        "sh_fo_wins",
        "sh_fo_losses",
        "penalty_minutes",
        "minors",
        "majors",
        "match_penalties",
        "misconducts",
        "game_misconducts",
        "penalties_drawn",
        "shifts",
        "toi",
        "ev_toi",
        "ot_toi",
        "pp_toi",
        "sh_toi"
    ]
    # collect top-level summary stats
    skat_sum_kwargs = {
        'desc': 'skater summary',
        'base_uri': 'https://api.nhle.com/stats/rest/en/skater/summary',
        'uri_args': [
            'isAggregate=false',
            'isGame=false',
            f'limit=100',
            'sort=skaterFullName',
            'factCayenneExp=gamesPlayed%3E=1',
            f'cayenneExp=gameTypeId=2%20and%20seasonId={season}{season + 1}'
        ],
        'drop_cols': [
            'lastName', 'skaterFullName', 'shootsCatches', 'positionCode',
            'pointsPerGame', 'timeOnIcePerGame', 'faceoffWinPct', 'shootingPct'
        ],
        'col_names': {
            'playerId': 'nhl_num', 'seasonId': 'season',
            'teamAbbrevs': 'teams', 'gamesPlayed': 'games_played',
            'plusMinus': 'plus_minus', 'penaltyMinutes': 'penalty_minutes',
            'evGoals': 'ev_goals', 'evPoints': 'ev_points',
            'ppGoals': 'pp_goals', 'ppPoints': 'pp_points',
            'shGoals': 'sh_goals', 'shPoints': 'sh_points',
            'gameWinningGoals': 'gw_goals', 'otGoals': 'ot_goals'
        }
    }

    # collect faceoff stats
    faceoff_kwargs = {
        'desc': 'faceoff stats',
        'base_uri': 'https://api.nhle.com/stats/rest/en/skater/faceoffwins',
        'uri_args': [
            'isAggregate=false',
            'isGame=false',
            f'limit=100',
            'sort=skaterFullName',
            'factCayenneExp=gamesPlayed%3E=1',
            f'cayenneExp=gameTypeId=2%20and%20seasonId={season}{season + 1}'
        ],
        'drop_cols': [
            'lastName', 'skaterFullName', 'teamAbbrevs', 'positionCode', 'seasonId',
            'gamesPlayed', 'faceoffWinPct'
        ],
        'col_names': {
            'playerId': 'nhl_num',
            'totalFaceoffs': 'faceoffs', 'totalFaceoffWins': 'fo_wins', 'totalFaceoffLosses': 'fo_losses',
            'defensiveZoneFaceoffs': 'dz_fo', 'neutralZoneFaceoffs': 'nz_fo', 'offensiveZoneFaceoffs': 'oz_fo',
            'defensiveZoneFaceoffWins': 'dz_fo_wins', 'defensiveZoneFaceoffLosses': 'dz_fo_losses',
            'neutralZoneFaceoffWins': 'nz_fo_wins', 'neutralZoneFaceoffLosses': 'nz_fo_losses',
            'offensiveZoneFaceoffWins': 'oz_fo_wins', 'offensiveZoneFaceoffLosses': 'oz_fo_losses',
            'evFaceoffs': 'ev_fo', 'evFaceoffsWon': 'ev_fo_wins', 'evFaceoffsLost': 'ev_fo_losses',
            'ppFaceoffs': 'pp_fo', 'ppFaceoffsWon': 'pp_fo_wins', 'ppFaceoffsLost': 'pp_fo_losses',
            'shFaceoffs': 'sh_fo', 'shFaceoffsWon': 'sh_fo_wins', 'shFaceoffsLost': 'sh_fo_losses'
        }
    }

    # collect on-ice goal stats
    goals_kwargs = {
        'desc': 'on-ice goal stats',
        'base_uri': 'https://api.nhle.com/stats/rest/en/skater/goalsForAgainst',
        'uri_args': [
            'isAggregate=false',
            'isGame=false',
            f'limit=100',
            'sort=skaterFullName',
            'factCayenneExp=gamesPlayed%3E=1',
            f'cayenneExp=gameTypeId=2%20and%20seasonId={season}{season + 1}'
        ],
        'drop_cols': [
            'lastName', 'skaterFullName', 'teamAbbrevs', 'positionCode', 'seasonId',
            'gamesPlayed', 'goals', 'assists', 'points',
            'evenStrengthGoalsForPct', 'evenStrengthTimeOnIcePerGame', 'evenStrengthGoalDifference',
            'powerPlayTimeOnIcePerGame', 'shortHandedTimeOnIcePerGame'
        ],
        'col_names': {
            'playerId': 'nhl_num',
            'evenStrengthGoalsAgainst': 'ev_goals_against', 'evenStrengthGoalsFor': 'ev_goals_for',
            'powerPlayGoalsAgainst': 'pp_goals_against', 'powerPlayGoalFor': 'pp_goals_for',
            'shortHandedGoalsAgainst': 'sh_goals_against', 'shortHandedGoalsFor': 'sh_goals_for'
        }
    }

    # collect realtime stats
    realtime_kwargs = {
        'desc': 'realtime stats',
        'base_uri': 'https://api.nhle.com/stats/rest/en/skater/realtime',
        'uri_args': [
            'isAggregate=false',
            'isGame=false',
            f'limit=100',
            'sort=skaterFullName',
            'factCayenneExp=gamesPlayed%3E=1',
            f'cayenneExp=gameTypeId=2%20and%20seasonId={season}{season + 1}'
        ],
        'drop_cols': [
            'lastName', 'skaterFullName', 'teamAbbrevs', 'positionCode', 'seasonId',
            'shootsCatches', 'gamesPlayed', 'firstGoals', 'otGoals',
            'blockedShotsPer60', 'giveawaysPer60', 'hitsPer60', 'takeawaysPer60', 'timeOnIcePerGame'
        ],
        'col_names': {
            'playerId': 'nhl_num',
            'emptyNetAssists': 'en_assists', 'emptyNetGoals': 'en_goals', 'emptyNetPoints': 'en_points',
            'blockedShots': 'blocks', 'missedShots': 'misses',
            'missedShotCrossbar': 'misses_cross', 'missedShotGoalpost': 'missed_post',
            'missedShotOverNet': 'misses_high', 'missedShotWideOfNet': 'missed_wide'
        }
    }

    # collect penalty stats
    penalty_kwargs = {
        'desc': 'penalty stats',
        'base_uri': 'https://api.nhle.com/stats/rest/en/skater/penalties',
        'uri_args': [
            'isAggregate=false',
            'isGame=false',
            f'limit=100',
            'sort=skaterFullName',
            'factCayenneExp=gamesPlayed%3E=1',
            f'cayenneExp=gameTypeId=2%20and%20seasonId={season}{season + 1}'
        ],
        'drop_cols': [
            'lastName', 'skaterFullName', 'teamAbbrevs', 'positionCode', 'seasonId',
            'gamesPlayed', 'assists', 'goals', 'points', 'netPenalties', 'penaltyMinutes',
            'netPenaltiesPer60', 'penalties', 'penaltiesDrawnPer60', 'penaltiesTakenPer60',
            'penaltyMinutesPerTimeOnIce', 'penaltySecondsPerGame', 'timeOnIcePerGame'
        ],
        'col_names': {
            'playerId': 'nhl_num',
            'gameMisconductPenalties': 'game_misconducts', 'misconductPenalties': 'misconducts',
            'majorPenalties': 'majors', 'minorPenalties': 'minors', 'matchPenalties': 'match_penalties',
            'penaltiesDrawn': 'penalties_drawn'
        }
    }

    # collect shot stats
    shot_kwargs = {
        'desc': 'shot stats',
        'base_uri': 'https://api.nhle.com/stats/rest/en/skater/shottype',
        'uri_args': [
            'isAggregate=false',
            'isGame=false',
            f'limit=100',
            'sort=skaterFullName',
            'factCayenneExp=gamesPlayed%3E=1',
            f'cayenneExp=gameTypeId=2%20and%20seasonId={season}{season + 1}'
        ],
        'drop_cols': [
            'lastName', 'skaterFullName', 'teamAbbrevs', 'seasonId',
            'gamesPlayed', 'goals', 'shootingPct', 'shootingPctBackhand',
            'shootingPctDeflected', 'shootingPctSlap', 'shootingPctSnap', 'shootingPctTipIn',
            'shootingPctWrapAround', 'shootingPctWrist', 'shots'
        ],
        'col_names': {
            'playerId': 'nhl_num',
            'goalsBackhand': 'goals_back', 'goalsDeflected': 'goals_defl', 'goalsSlap': 'goals_slap',
            'goalsSnap': 'goals_snap', 'goalsTipIn': 'goals_tip', 'goalsWrapAround': 'goals_wrap',
            'goalsWrist': 'goals_wrist',
            'shotsOnNetBackhand': 'shots_back', 'shotsOnNetDeflected': 'shots_defl', 'shotsOnNetSlap': 'shots_slap',
            'shotsOnNetSnap': 'shots_snap', 'shotsOnNetTipIn': 'shots_tip', 'shotsOnNetWrapAround': 'shots_wrap',
            'shotsOnNetWrist': 'shots_wrist'
        }
    }

    # collect time on ice stats
    toi_kwargs = {
        'desc': 'time on ice stats',
        'base_uri': 'https://api.nhle.com/stats/rest/en/skater/timeonice',
        'uri_args': [
            'isAggregate=false',
            'isGame=false',
            f'limit=100',
            'sort=skaterFullName',
            'factCayenneExp=gamesPlayed%3E=1',
            f'cayenneExp=gameTypeId=2%20and%20seasonId={season}{season + 1}'
        ],
        'drop_cols': [
            'lastName', 'skaterFullName', 'teamAbbrevs', 'seasonId', 'positionCode', 'shootsCatches',
            'gamesPlayed', 'evTimeOnIcePerGame', 'otTimeOnIcePerOtGame', 'ppTimeOnIcePerGame',
            'shTimeOnIcePerGame', 'shiftsPerGame', 'timeOnIcePerGame', 'timeOnIcePerShift'
        ],
        'col_names': {
            'playerId': 'nhl_num',
            'evTimeOnIce': 'ev_toi', 'ppTimeOnIce': 'pp_toi', 'shTimeOnIce': 'sh_toi',
            'otTimeOnIce': 'ot_toi', 'timeOnIce': 'toi'
        }
    }

    skater_data = scrape_endpoint(season, **skat_sum_kwargs)
    # parse the comma-separated team list (once we can split data be team we should refactor)
    skater_data['team'] = [x.split(',')[0] for x in skater_data['teams']]
    skater_data['team2'] = [x.split(',')[1] if len(x) > 3 else None for x in skater_data['teams']]
    skater_data['team3'] = [x.split(',')[2] if len(x) > 7 else None for x in skater_data['teams']]
    skater_data = skater_data.drop(columns=['teams'], axis=1)
    skater_data["season"] = skater_data["season"] // 10000

    kwarg_list = [faceoff_kwargs, goals_kwargs, realtime_kwargs, penalty_kwargs, shot_kwargs, toi_kwargs]
    for kwargs in kwarg_list:
        additional_data = scrape_endpoint(season, **kwargs)
        skater_data = skater_data.join(additional_data, how='inner')

    skater_data["ev_assists"] = skater_data["ev_points"] - skater_data["ev_goals"]
    skater_data["pp_assists"] = skater_data["pp_points"] - skater_data["pp_goals"]
    skater_data["sh_assists"] = skater_data["sh_points"] - skater_data["sh_goals"]
    int_cols = [x for x in final_cols if 'team' not in x]
    skater_data[int_cols] = skater_data[int_cols].fillna(value=-1)
    return skater_data.reindex(columns=final_cols)


def scrape_bios(start_year, end_year):
    """Gathers all bio data for the given seasons from the appropriate NHL API.

    start_year:  the first year of the time span (e.g. '2019' for 2019-2020 season)
    end_year:    the last year of the time span (e.g. '2019' for 2019-2020 season)"""
    # constants
    final_cols = [
        "nhl_num",
        "last_name",
        "first_name",
        "birth_date",
        "height",
        "weight",
        "position",
        "shoots_catches",
        "draft_year",
        "draft_round",
        "draft_pos",
        "first_season"
    ]
    desc = 'skater biographicals'
    base_uri = 'https://api.nhle.com/stats/rest/en/skater/bios'
    uri_args = [
        'isAggregate=false',
        'isGame=false',
        f'limit=100',
        'sort=skaterFullName',
        'factCayenneExp=gamesPlayed%3E=1',
        f'cayenneExp=gameTypeId=2%20and%20seasonId>={start_year}{start_year + 1}%20and%20seasonId<={end_year}{end_year + 1}'
    ]
    drop_cols = [
        'assists', 'birthCity', 'birthStateProvinceCode', 'birthCountryCode',
        'currentTeamAbbrev', 'currentTeamName', 'skaterFullName',
        'gamesPlayed', 'goals', 'isInHallOfFameYn',
        'nationalityCode', 'points'
    ]
    col_names = {
        'playerId': 'nhl_num', 'lastName': 'last_name', 'skaterFullName': 'full_name', 'birthDate': 'birth_date',
        'draftOverall': 'draft_pos', 'draftRound': 'draft_round', 'draftYear': 'draft_year',
        'firstSeasonForGameType': 'first_season', 'positionCode': 'position', 'shootsCatches': 'shoots_catches'
    }
    data_limit = 100  # number of players fetched per call
    player_index = 0  # first player index of the next call
    uri = '?'.join([base_uri, '&'.join(uri_args)])

    # the first call
    print(f'\nStarting {desc}\n--------------------')
    r = _request(uri + f"&start={player_index}")
    output = r['data']
    total = r['total']

    # the remaining calls
    print(f'{total} rows found: {total // data_limit} calls remaining')
    player_index += data_limit
    while player_index <= total:
        r = _request(uri + f"&start={player_index}")
        output += r['data']
        player_index += data_limit

    output = pd.DataFrame(output)                   # convert to a data frame
    # process a first name
    output["first_name"] = output[["skaterFullName", "lastName"]].apply(
        lambda x: x[0].replace(x[1], "").strip(), axis=1
    )
    output = output.drop(labels=drop_cols, axis=1)  # drop unnecessary columns
    output.rename(columns=col_names, inplace=True)  # rename columns
    output.fillna(value=-1, inplace=True)
    output["first_season"] = output["first_season"] // 10000
    return output.reindex(columns=final_cols).set_index('nhl_num')


def scrape_endpoint(season, desc, base_uri, uri_args, drop_cols, col_names):
    """Gathers all data for the given season from the appropriate NHL API.

    season:    the first year of the desired season (e.g. '2019' for 2019-2020 season)
    desc:      a description of what is being scraped, for logging purposes
    base_uri:  the non-query argument portion of the URI
    uri_args:  the query args of the URI
    drop_cols: the names of columns we do not collect from the page
    col_names: a mapping from response columns to desired column names"""
    # constants
    data_limit = 100  # number of players fetched per call
    player_index = 0  # first player index of the next call
    uri = '?'.join([base_uri, '&'.join(uri_args)])

    # the first call
    print(f'\nStarting {season} {desc}\n--------------------')
    r = _request(uri + f"&start={player_index}")
    output = r['data']
    total = r['total']

    # the remaining calls
    print(f'{total} rows found: {total // data_limit + 1} calls to make')
    player_index += data_limit
    while player_index <= total:
        if player_index // data_limit % 10 == 0:
            print('/', end='')
        r = _request(uri + f"&start={player_index}")
        print('-', end='')
        output += r['data']
        player_index += data_limit
    print()

    output = pd.DataFrame(output)                   # convert to a data frame
    output = output.drop(labels=drop_cols, axis=1)  # drop unnecessary columns
    output.rename(columns=col_names, inplace=True)  # rename columns
    return output.set_index('nhl_num')


if __name__ == "__main__":
    data = []
    for year in range(2019, 2008, -1):
        data.append(scrape_season(year))
    df = pd.concat(data, sort=False)
    df.to_csv('../data/skater_data.csv', float_format='%.f')
    # skater_bios = scrape_bios(2009, 2019)
    # skater_bios.to_csv('../data/skater_bios.csv', float_format='%.f')
