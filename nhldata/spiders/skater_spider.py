# These spiders crawl the NHL Individual Stats pages.
# Each spider crawls a different skater category ("view").

from scrapy.contrib.spiders import CrawlSpider, Rule
from scrapy.contrib.linkextractors.sgml import SgmlLinkExtractor
from scrapy.selector import Selector

from nhldata.skater_items import *

# This spider grabs most classic stats from the 'Summary' pages.

class SkatSumSpider(CrawlSpider):
    name = "skatsum"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    # This function allows us to pass an argument to the spider
    # by inserting it into the command line prompt.
    # E.g., scrapy crawl skatsum -a season='1998' etc...
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatSumSpider, self).__init__(*args, **kwargs)
        self.year = int(season)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=summary&sort=points&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        name = ""
        sName = []
        num = 0
        CATEG = [
                 'games_played', 'goals', 'assists', 'points', 'plus_minus',
                 'penalty_minutes', 'pp_goals', 'pp_points', 'sh_goals',
                 'sh_points', 'gw_goals', 'ot_goals', 'shots', 'shot_pct'
                 ]
        shootout = 0
        if self.year > 2005:
            shootout = 1
        for cell in cells:
            item = SkatSumItem()
            name = cell.xpath('td[2]/a/text()').extract()
            sName = name[0].split(' ',1)
            item['first_name'] = sName[0]
            item['last_name'] = sName[1]
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            if not cell.xpath('td[3]/a/text()').extract():
                item['team'] = cell.xpath('td[3]/text()').extract()[0][-3:]
            else:
                item['team'] = cell.xpath('td[3]/a/text()').extract()[0]
            item['position'] = cell.xpath('td[4]/text()').extract()[0]
            i = 4
            temp = ""
            while i < 15:
                i += 1
                if i == 9:
                    temp = cell.xpath('td[9]/text()').extract()[0]
                    if temp[0] == "+":
                        item['plus_minus'] = int(temp[1:])
                    else:
                        item['plus_minus'] = int(temp)
                else:
                    item[CATEG[i-5]] = int(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
            item['ot_goals'] = int(cell.xpath('td[' + str(17 - shootout) + ']/text()').extract()[0])
            item['shots'] = int(cell.xpath('td[' + str(18 - shootout) + ']/text()').extract()[0])
            item['shot_pct'] = float(cell.xpath('td[' + str(19 - shootout) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes 'empty net goal' stats from 'goals' pages.

class SkatEngSpider(CrawlSpider):
    name = "skateng"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatEngSpider, self).__init__(*args, **kwargs)
        self.year = int(season)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=goals&sort=goals&pg=1" % season]
        
    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        shootout = 0
        if self.year > 2005:
            shootout = 1
        for cell in cells:
            item = SkatEngItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            item['en_goals'] = int(cell.xpath('td[' + str(21 - shootout) + ']/text()').extract()[0])
            item['ps_goals'] = int(cell.xpath('td[' + str(22 - shootout) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes penalty stats.

class SkatPIMSpider(CrawlSpider):
    name = "skatpim"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatPIMSpider, self).__init__(*args, **kwargs)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=penalties&sort=penaltyMinutes&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = SkatPIMItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            i = 6
            CATEG = ['minors', 'majors', 'misconducts', 'game_misconducts', 'matches']
            while i < 11:
                i += 1
                item[CATEG[i-7]] = int(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes +/- stats.

class SkatPMSpider(CrawlSpider):
    name = "skatpm"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatPMSpider, self).__init__(*args, **kwargs)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=plusMinus&sort=plusMinus&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = SkatPMItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            i = 13
            CATEG = ['team_goals_for', 'team_pp_goals_for', 'team_goals_against', 'team_pp_goals_against']
            while i < 17:
                i += 1
                item[CATEG[i-14]] = int(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes 'real-time' stats.

class SkatRTSSpider(CrawlSpider):
    name = "skatrts"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatRTSSpider, self).__init__(*args, **kwargs)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=rtssPlayerStats&sort=gamesPlayed&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = SkatRTSItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            i = 5
            CATEG = ['hits', 'blocked_shots', 'missed_shots', 'giveaways', 'takeaways', 'faceoff_wins', 'faceoff_losses']
            while i < 12:
                i += 1
                item[CATEG[i-6]] = int(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes shootout stats.

class SkatSOSpider(CrawlSpider):
    name = "skatso"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatSOSpider, self).__init__(*args, **kwargs)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=shootouts&sort=goals&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = SkatSOItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            i = 12
            CATEG = ['so_shots', 'so_goals', 'so_pct', 'game_deciding_goals']
            while i < 16:
                i += 1
                if i == 15:
                    item['so_pct'] = float(cell.xpath('td[15]/text()').extract()[0])
                else:
                    item[CATEG[i-13]] = int(cell.xpath('td[' + str(i) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes overtime stats.

class SkatOTSpider(CrawlSpider):
    name = "skatot"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatOTSpider, self).__init__(*args, **kwargs)
        self.year = int(season)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=scoringLeaders&sort=powerPlayGoals&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        shootout = 0
        if self.year > 2005:
            shootout = 1
        for cell in cells:
            item = SkatOTItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            i = 15
            CATEG = ['ot_games_played', 'ot_goals', 'ot_assists', 'ot_points']
            while i < 19:
                i += 1
                if i == 17:
                    pass
                else:
                    item[CATEG[i-16]] = int(cell.xpath('td[' + str(i + 1 - shootout) + ']/text()').extract()[0])
            items.append(item)
        return items

# This spider scrapes Time On Ice stats, converted to seconds.

class SkatTOISpider(CrawlSpider):
    name = "skattoi"
    allowed_domains = ["nhl.com"]
    start_urls = []
    
    rules = (Rule(SgmlLinkExtractor(
        allow=('.*&pg=.*'),
        restrict_xpaths=('/html//tfoot[@class="paging"]')),
        callback='parse_item', follow=True
        ),)
    
    def __init__(self, season="", *args, **kwargs):
        super(SkatTOISpider, self).__init__(*args, **kwargs)
        self.start_urls = [
            "http://www.nhl.com/ice/playerstats.htm?fetchKey=%s2ALLSASALL"
            "&viewName=timeOnIce&sort=timeOnIce&pg=1" % season]

    def parse_item(self, response):
        sel = Selector(response)
        cells = sel.xpath('/html//div[@class="contentBlock"]/table/tbody/tr')
        items = []
        num = 0
        for cell in cells:
            item = SkatTOIItem()
            num = cell.xpath('td[2]/a/@href').extract()
            sNum = int(num[0][-7:])
            item['nhl_num'] = sNum
            i = 5
            temp = ""
            sTemp = []
            CATEG = ['es_toi', 'sh_toi', 'pp_toi', 'toi']
            while i < 12:
                i += 1
                if i % 2 == 0:
                    temp = cell.xpath('td[' + str(i) + ']/text()').extract()[0]
                    sTemp = temp.split(':')
                    sTemp[0] = sTemp[0].replace(',', '')
                    item[CATEG[(i-6)/2]] = 60*int(sTemp[0])+int(sTemp[1])
                else:
                    pass
            items.append(item)
        return items
