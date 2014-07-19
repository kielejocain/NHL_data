# These spiders crawl the NHL Individual Stats pages.
# Each spider crawls a different goalie category ("view").

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import Selector

from nhldata.goalie_items import *

# This spider grabs most classic stats from the 'Summary' pages.

class GoalSumSpider(CrawlSpider):
    name = "goalsum"
    allowed_domains = ["nhl.com"]
    start_urls = []
    year = 0

    rules = (Rule(SgmlLinkExtractor(
                                    allow=('.*&pg=.*'),
                                    restrict_xpaths=('/html//tfoot[@class="paging"]')
                                    ),
                  callback='parse_item', follow=True
                  ),)
    
    # This function allows us to pass an argument to the spider
    # by inserting it into the command line prompt.
    # E.g., scrapy crawl goalsum -a season='1998' etc...
    
    def __init__(self, season, *args, **kwargs):
        super(GoalSumSpider, self).__init__(*args, **kwargs)
        self.year = int(season)
        self.start_urls = ["http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLGAGALL"
                  "&viewName=summary&sort=wins&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        name = ""
        sName = []
        num = 0
        # is this a season with shootouts/without ties?
        shootout = 0
        if self.year > 2005:
            shootout = 1
        CATEG = ['games_played', 'games_started', 'wins', 'losses']
        if not shootout:
            CATEG = CATEG + ['ties']
        CATEG = CATEG + [
                 'overtime_losses', 'shots_against', 'goals_against', 'gaa',
                 'saves_', 'save_pct', 'shutouts', 'goals', 'assists',
                 'penalty_minutes', 'toi'
                 ]
        for cell in cells:
            item = GoalSumItem()
            name = cell.xpath('td[2]/a/text()').extract()
            sName = name[0].split(' ',1)
            item['first_name'] = sName[0]
            item['last_name'] = sName[1]
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            if cell.xpath('td[3]/a/text()').extract():
                item['team'] = cell.xpath('td[3]/a/text()').extract()[0]
            elif cell.xpath('td[3]/text()').extract():
                item['team'] = cell.xpath('td[3]/text()').extract()[0][-3:]
            else:
                item['team'] = ''
            item['position'] = 'G'
            i = 3
            temp = ""
            while i < 19 - shootout:
                i += 1
                if i == 19 - shootout:
                    temp = cell.xpath('td[' + str(19 - shootout) + ']/text()').extract()[0]
                    sTemp = temp.split(':')
                    sTemp[0] = sTemp[0].replace(',', '')
                    item[CATEG[15 - shootout]] = 60*int(sTemp[0])+int(sTemp[1])
                elif i == 12 - shootout or i == 14 - shootout:
                    item[CATEG[i-4]] = float(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
                else:
                    item[CATEG[i-4]] = int(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes penalty shot stats.

class GoalPSSpider(CrawlSpider):
    name = "goalps"
    allowed_domains = ["nhl.com"]
    start_urls = []

    rules = (Rule(SgmlLinkExtractor(
                                    allow=('.*&pg=.*'),
                                    restrict_xpaths=('/html//tfoot[@class="paging"]')
                                    ),
                  callback='parse_item', follow=True
                  ),)
    
    def __init__(self, season, *args, **kwargs):
        super(GoalPSSpider, self).__init__(*args, **kwargs)
        self.start_urls = ["http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLGAGALL"
                  "&viewName=penaltyShot&sort=penaltyShotsAgainst&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = GoalPSItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            item['ps_attempts'] = int(cell.xpath('td[6]/text()').extract()[0])
            item['ps_goals_against'] = int(cell.xpath('td[7]/text()').extract()[0])
            item['ps_saves'] = int(cell.xpath('td[8]/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes shootout stats.

class GoalSOSpider(CrawlSpider):
    name = "goalso"
    allowed_domains = ["nhl.com"]
    start_urls = []

    rules = (Rule(SgmlLinkExtractor(
                                    allow=('.*&pg=.*'),
                                    restrict_xpaths=('/html//tfoot[@class="paging"]')
                                    ),
                  callback='parse_item', follow=True
                  ),)
    
    def __init__(self, season, *args, **kwargs):
        super(GoalSOSpider, self).__init__(*args, **kwargs)
        self.start_urls = ["http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLGAGALL"
                  "&viewName=shootouts&sort=shootoutGamesWon&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = GoalSOItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            item['so_wins'] = int(cell.xpath('td[14]/text()').extract()[0])
            item['so_losses'] = int(cell.xpath('td[15]/text()').extract()[0])
            item['so_shots_against'] = int(cell.xpath('td[16]/text()').extract()[0])
            item['so_goals_against'] = int(cell.xpath('td[17]/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes special teams stats.

class GoalSTSpider(CrawlSpider):
    name = "goalst"
    allowed_domains = ["nhl.com"]
    start_urls = []

    rules = (Rule(SgmlLinkExtractor(
                                    allow=('.*&pg=.*'),
                                    restrict_xpaths=('/html//tfoot[@class="paging"]')
                                    ),
                  callback='parse_item', follow=True
                  ),)
    
    def __init__(self, season, *args, **kwargs):
        super(GoalSTSpider, self).__init__(*args, **kwargs)
        self.start_urls = ["http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLGAGALL"
                  "&viewName=specialTeamSaves&sort=evenStrengthSaves&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = GoalSTItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            i = 5
            CATEG = [
                     'es_shots_against', 'es_goals_against', 'es_saves', 'es_save_pct',
                     'pp_shots_against', 'pp_goals_against', 'pp_saves', 'pp_save_pct',
                     'sh_shots_against', 'sh_goals_against', 'sh_saves', 'sh_save_pct'
                     ]
            while i < 17:
                i += 1
                if i % 4 == 1:
                    try:
                        item[CATEG[i-6]] = float(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
                    except IndexError:
                        item[CATEG[i-6]] = 0.0
                else:
                    item[CATEG[i-6]] = int(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
            items.append(item)
        return items