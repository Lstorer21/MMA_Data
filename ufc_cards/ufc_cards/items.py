# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy.loader import ItemLoader
from itemloaders.processors import TakeFirst, MapCompose
from w3lib.html import remove_tags

class UfcCardsItem(scrapy.Item):
    # define the fields for your item here like:
    Name = scrapy.Field(input_processor = MapCompose(remove_tags, str.strip), output_processor = TakeFirst())
    Location = scrapy.Field(input_processor = MapCompose(remove_tags, str.strip), output_processor = TakeFirst())
    Date = scrapy.Field(input_processor = MapCompose(remove_tags, str.strip), output_processor = TakeFirst())
    
