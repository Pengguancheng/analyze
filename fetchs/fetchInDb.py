# -*- coding: utf-8 -*-

import datetime
from pymongo import MongoClient
from fetchs import fetch
from fetchs import stockFetcher
from fetchs import get_proxy
import traceback
import multiprocessing
import json

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
        last = day_collect.find({'com': com['code']}).sort('date', -1)
        if last.count() > 0:
            if last.next()['date'] > datetime.datetime(year, month, 1):
                return True
        return False

    def get_proxy_pool(self):
        proxy_get = get_proxy.ProxyGet()
        return proxy_get.proxys

    def handle_error(self, e):
        '''處理 child process 的錯誤，不然 code 寫錯時，不會回報任何錯誤'''
        traceback.print_exception(type(e), e, e.__traceback__)

    def match_ata(self, data, fields):
        name_dict = {'date': '日期', 'capacity': '成交股數',
                     'turnover': '成交金額', 'open': '開盤價', 'high': '最高價', 'low': '最低價',
                     'close': '收盤價', 'change': '漲跌價差', 'transaction': '成交筆數'}


if __name__ == '__main__':
    data = Data()
    # data.form_old(2016, 1)
    coms = 0
    year = 2016
    month = 1
    fetcher = stockFetcher.TWSEFetcher()
    while not (year > 2017 and month > 1 or len(data.proxys) == 0):
        day_collect = data.client['data']['day_collect' + str(year)]
        with multiprocessing.Pool(processes=multiprocessing.cpu_count()) as p:
            results = []
            for com in data.companys:
                coms += 1
                # if coms > 10:
                #     break
                if data.find_last_day(com, year, month, day_collect):
                    continue
                print('{a}  code : {b}  name: '
                      ''.format(a=coms, b=com['code']) + com['name'])
                results.append(p.apply_async(fetcher.fetch, args=(year, month, com['code'], data.proxys),
                                             error_callback=data.handle_error))
            print('等待所有 child processes 完成')
            # 關掉 pool 不再加入任何 child process
            p.close()
            # 調用 join() 之前必須先調用close()
            p.join()
        print('所有 child processes 完成')
        for mon in results:
            try:
                day_data = fetcher.purify(json.loads(mon.get()))
                sid = json.loads(mon.get())['sid']
            except Exception:
                try:
                    day_data = fetcher.purify(mon.get())
                    sid = mon.get()['sid']
                except Exception:
                    break
            for line in day_data:
                insert_dict = {'com': sid, 'date': line.date, 'capacity': line.capacity,
                               'turnover': line.turnover, 'open': line.open, 'high': line.high, 'low': line.low,
                               'close': line.close, 'change': line.change, 'transaction': line.transaction}
                day_collect.insert(insert_dict)
        if month == 12:
            year += 1
        else:
            month += 1
