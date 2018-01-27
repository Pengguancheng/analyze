# -*- coding: utf-8 -*-
#
# Usage: Download all stock code info from TWSE
#
# TWSE equities = 上市證券
# TPEx equities = 上櫃證券
#

import csv
from collections import namedtuple
from pymongo import MongoClient

import requests
from lxml import etree

TWSE_EQUITIES_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
TPEX_EQUITIES_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
ROW = namedtuple('Row', ['type', 'code', 'name', 'ISIN', 'start',
                         'market', 'group', 'CFI'])


def make_row_tuple(typ, row):
    code, name = row[1].split('\u3000')
    return ROW(typ, code, name, *row[2: -1])


def fetch_data(url):
    r = requests.get(url)
    root = etree.HTML(r.text)
    trs = root.xpath('//tr')[1:]

    result = []
    typ = ''
    for tr in trs:
        tr = list(map(lambda x: x.text, tr.iter()))
        if len(tr) == 4:
            # This is type
            typ = tr[2].strip(' ')
        else:
            # This is the row data
            result.append(make_row_tuple(typ, tr))
    return result


def to_mongo(url):
    data = fetch_data(url)
    client = MongoClient('127.0.0.1', 27017)
    collect = client['data']['twse_equities']
    collect.drop()
    for row in data:
        collect.insert({
            'name': row.name,
            'type': row.type,
            'code': row.code,
            'ISIN': row.ISIN,
            'start': row.start,
            'market': row.market,
            'group': row.group
        })


if __name__ == '__main__':
    to_mongo(TWSE_EQUITIES_URL)
