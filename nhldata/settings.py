# Scrapy settings for nhldata project
#
# For simplicity, this file contains only the most important settings by
# default. All the other settings are documented here:
#
#     http://doc.scrapy.org/en/latest/topics/settings.html
#

BOT_NAME = 'nhldata'

SPIDER_MODULES = ['nhldata.spiders']
NEWSPIDER_MODULE = 'nhldata.spiders'

LOG_FILE = 'nhldata.log'
LOG_LEVEL = 'DEBUG'

FEED_FORMAT = 'csv'
FEED_URI = 'hesgotdata.csv'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
#USER_AGENT = 'nhldata (+http://www.yourdomain.com)'
