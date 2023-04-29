# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from sqlalchemy import create_engine
import psycopg2
import ufc_cards.creds as creds

class UfcCardsPipeline:
    def __init__(self):
        self.create_connection()

    def create_connection(self):
        self.connection = psycopg2.connect(
            host = creds.DB_HOST,
            user = creds.DB_USER,
            password = creds.DB_PASS,
            database = creds.DB_NAME,
            port = creds.DB_PORT
        )
        self.curr = self.connection.cursor()

       
    def process_item(self, item, spider):
        self.store_db(item)
        return item

    def store_db(self, item):
        try:
            self.curr.execute('''INSERT INTO fight_cards (date, name, location) values (%s, %s, %s)''',(
                item['Date'],
                item['Location'],
                item['Name']
                ))
        except BaseException as e:
            print(e)

        self.connection.commit()