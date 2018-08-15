import urllib3
import csv
import requests
from collections import namedtuple
from lxml.html import etree

TWSE_EQUITIES_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=2'
TPEX_EQUITIES_URL = 'http://isin.twse.com.tw/isin/C_public.jsp?strMode=4'
ROW = namedtuple('Row', ['type', 'code', 'name', 'ISIN', 'start',
                         'market', 'group', 'CFI'])
def fetch_data(url):
    r = requests.get(url)
    return r

if __name__ == '__main__':
    data = fetch_data(TWSE_EQUITIES_URL)
    root = etree.HTML(data.text)
    trs = root.xpath('//tr')[1:]