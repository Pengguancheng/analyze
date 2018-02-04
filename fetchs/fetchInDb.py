# -*- coding: utf-8 -*-

import datetime
from pymongo import MongoClient
from fetchs import fetch
from fetchs import stockFetcher
from random import randint
from time import sleep
from fetchs import get_proxy

FIELDS = {'日期': 'date', '成交股數': 'capacity', '成交金額': 'turnover', '開盤價': 'open', '最高價': 'high',
          '最低價': 'low', '收盤價': 'close', '漲跌價差': 'change', '成交筆數': 'transaction'}


class Data:
    def __init__(self):
        self.client = MongoClient('127.0.0.1', 27017)
        fetch.to_mongo(fetch.TWSE_EQUITIES_URL)
        self.today = datetime.date.today()
        self.companys = self.client['data']['twse_equities'].find().sort('code', -1)
        self.proxys = self.get_proxy_pool()

    def form_old(self, year=0, month=0):
        coms = 0
        fetcher = stockFetcher.TWSEFetcher()
        while not (year > self.today.year and month > self.today.month or len(self.proxys) == 0):
            day_collect = self.client['data']['day_collect' + str(year)]
            for com in self.companys:
                coms += 1
                if self.find_last_day(com, year, month, day_collect):
                    continue
                print('{a}  code : {b}  name: '
                      ''.format(a=coms, b=com['code']) + com['name'])
                fetch_data = fetcher.fetch(year, month, com['code'], self.proxys)
                for line in fetch_data['data']:
                    insert_dict = {'com': com, 'date': line.date, 'capacity': line.capacity,
                                   'turnover': line.turnover, 'open': line.open, 'high': line.high, 'low': line.low,
                                   'close': line.close, 'change': line.change, 'transaction': line.transaction}
                    day_collect.insert(insert_dict)
            if month == 12:
                year += 1
            else:
                month += 1

    def find_last_day(self, com, year, month, day_collect):
        last = day_collect.find({'com.ISIN': com['ISIN']}).sort('date', -1)
        if last.count() > 0:
            if last.next()['date'] > datetime.datetime(year, month, 1):
                return True
        return False

    def get_proxy_pool(self):
        proxy_get = get_proxy.ProxyGet()
        return proxy_get.proxys


if __name__ == '__main__':
    data = Data()
    data.form_old(2016, 1)
