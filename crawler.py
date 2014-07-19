from twisted.internet import reactor
from scrapy.crawler import Crawler
from scrapy import log
from nhldata.spiders.skater_spider import *
from scrapy.utils.project import get_project_settings

spider1 = SkatSumSpider(domain='nhl.com')
spider2 = SkatEngSpider(domain='nhl.com')
spider3 = SkatPIMSpider(domain='nhl.com')
spider4 = SkatPMSpider(domain='nhl.com')
spider5 = SkatRTSSpider(domain='nhl.com')
spider7 = SkatOTSpider(domain='nhl.com')
spider8 = SkatTOISpider(domain='nhl.com')

for spider in [spider1, spider2, spider3, spider4,
               spider5, spider7, spider8]:
    settings = get_project_settings()
    crawler = Crawler(settings)
    crawler.configure()
    crawler.crawl(spider)
    crawler.start()
log.start()
reactor.run() # the script will block here until the spider_closed signal was sent
