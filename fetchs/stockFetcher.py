# -*- coding: utf-8 -*-

import datetime
import json
import urllib.parse
from collections import namedtuple
from random import randint

import requests

TWSE_BASE_URL = 'http://www.twse.com.tw/'
TPEX_BASE_URL = 'http://www.tpex.org.tw/'
DATATUPLE = namedtuple('Data', ['date', 'capacity', 'turnover', 'open',
                                'high', 'low', 'close', 'change', 'transaction'])


class BaseFetcher(object):
    def fetch(self, year, month, sid, retry):
        pass

    def _convert_date(self, date):
        """Convert '106/05/01' to '2017/05/01'"""
        return '/'.join([str(int(date.split('/')[0]) + 1911)] + date.split('/')[1:])

    def _make_datatuple(self, data):
        pass

    def purify(self, original_data):
        pass


class TWSEFetcher(BaseFetcher):
    REPORT_URL = urllib.parse.urljoin(TWSE_BASE_URL, 'exchangeReport/STOCK_DAY')

    def __init__(self):
        pass

    def fetch(self, year: int, month: int, sid: str, proxys, retry=5):
        params = {'date': '%d%02d01' % (year, month), 'stockNo': sid, 'response':'json'}
        i = 0
        while True:
            try:
                rand = randint(0, len(proxys))
                proxies = {"http": proxys[rand], "https": proxys[rand]} #, proxies=proxies
                r = requests.get(self.REPORT_URL, params=params, timeout=3,
                                 headers=self.get_header(), proxies=proxies)
                break
            except Exception:
                if i > 10:
                    print("fail")
                    return {'data': []}
                i += 1
                continue
        try:
            data = r.json()
        except json.decoder.JSONDecodeError:
            if retry:
                return self.fetch(year, month, sid, retry - 1)
            data = {'stat': '', 'data': []}

        if data['stat'] == 'OK':
            data['data'] = self.purify(data)
        else:
            data['data'] = []
        return data

    def _make_datatuple(self, data):
        for line in range(1, 8):
            if '--' in data[line]:
                data[line] = '0'
        data[0] = datetime.datetime.strptime(self._convert_date(data[0]), '%Y/%m/%d')
        data[1] = int(data[1].replace(',', ''))
        data[2] = int(data[2].replace(',', ''))
        data[3] = float(data[3].replace(',', ''))
        data[4] = float(data[4].replace(',', ''))
        data[5] = float(data[5].replace(',', ''))
        data[6] = float(data[6].replace(',', ''))
        data[7] = float(0.0 if data[7].replace(',', '') == 'X0.00' else data[7].replace(',', ''))  # +/-/X表示漲/跌/不比價
        data[8] = int(data[8].replace(',', ''))
        return DATATUPLE(*data)

    def purify(self, original_data):
        return [self._make_datatuple(d) for d in original_data['data']]

    def get_header(self):

        headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
            "Accept-Encoding": 'gzip, deflate',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'

        }
        return headers
