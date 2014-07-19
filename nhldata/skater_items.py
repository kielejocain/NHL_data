# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

from scrapy.item import Item, Field

class SkatSumItem(Item):
    # define the fields for your item here like:
    # name = Field()
    first_name = Field()
    last_name = Field()
    nhl_num = Field()
    team = Field()
    position = Field()
    games_played = Field()
    goals = Field()
    assists = Field()
    points = Field()
    plus_minus = Field()
    penalty_minutes = Field()
    pp_goals = Field()
    pp_points = Field()
    sh_goals = Field()
    sh_points = Field()
    gw_goals = Field()
    ot_goals = Field()
    shots = Field()
    shot_pct = Field()

class SkatEngItem(Item):
    nhl_num = Field()
    en_goals = Field()
    ps_goals = Field()

class SkatPIMItem(Item):
    nhl_num = Field()
    minors = Field()
    majors = Field()
    misconducts = Field()
    game_misconducts = Field()
    matches = Field()

class SkatPMItem(Item):
    nhl_num = Field()
    team_goals_for = Field()
    team_pp_goals_for = Field()
    team_goals_against = Field()
    team_pp_goals_against = Field()
    

class SkatRTSItem(Item):
    nhl_num = Field()
    hits = Field()
    blocked_shots = Field()
    missed_shots = Field()
    giveaways = Field()
    takeaways = Field()
    faceoff_wins = Field()
    faceoff_losses = Field()

class SkatSOItem(Item):
    nhl_num = Field()
    so_shots = Field()
    so_goals = Field()
    so_pct = Field()
    game_deciding_goals = Field()

class SkatOTItem(Item):
    nhl_num = Field()
    ot_games_played = Field()
    ot_assists = Field()
    ot_points = Field()

class SkatTOIItem(Item):
    nhl_num = Field()
    es_toi = Field()
    sh_toi = Field()
    pp_toi = Field()
    toi = Field()
