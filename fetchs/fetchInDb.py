# -*- coding: utf-8 -*-

from collections import namedtuple
import datetime
from pymongo import MongoClient
import pprint
from fetchs import fetch
from fetchs import stockFetcher
from random import randint
from time import sleep

FIELDS = {'日期': 'date', '成交股數': 'capacity', '成交金額': 'turnover', '開盤價': 'open', '最高價': 'high',
          '最低價': 'low', '收盤價': 'close', '漲跌價差': 'change', '成交筆數': 'transaction'}


class Data:
    def __init__(self):
        self.client = MongoClient('127.0.0.1', 27017)
        fetch.to_mongo(fetch.TWSE_EQUITIES_URL)
        self.day_collect = self.client['data']['day_collect']
        self.today = datetime.date.today()
        self.companys = self.client['data']['twse_equities'].find()

    def form_old(self, year=0, month=0):
        coms = 0
        while not (year > self.today.year and month > self.today.month):
            for com in self.companys:
                print(coms)
                coms += 1
                if self.find_last_day(com, year, month):
                    continue
                fetcher = stockFetcher.TWSEFetcher()
                fetch_data = fetcher.fetch(year, month, com['code'])
                for line in fetch_data['data']:
                    insert_dict = {'com': com, 'date': line.date, 'capacity': line.capacity,
                                   'turnover': line.turnover, 'open': line.open, 'high': line.high, 'low': line.low,
                                   'close': line.close, 'change': line.change, 'transaction': line.transaction}
                    self.day_collect.insert(insert_dict)
                sleep(randint(1, 8))
            if month == 12:
                year += 1
            else:
                month += 1

    def find_last_day(self, com, year, month):
        last = self.day_collect.find({'com.ISIN': com['ISIN']}).sort('date', -1)
        if last.count() > 0:
            if last.next()['date'] > datetime.datetime(year, month, 1):
                return True
        return False


if __name__ == '__main__':
    data = Data()
    data.form_old(2014, 1)
