##encoding=utf8
import requests
from bs4 import BeautifulSoup
import socket
import json

User_Agent = 'Mozilla/5.0 (Windows NT 6.3; WOW64; rv:43.0) Gecko/20100101 Firefox/43.0'
header = {'User-Agent': User_Agent}

'''
获取所有代理IP地址
'''


class ProxyGet:
    def __init__(self):
        self.proxys = self.validateIp(self.getProxyIp())

    def getProxyIp(self):
        proxy = []
        for i in range(1, 4):
            try:
                url = 'http://www.xicidaili.com/nn/' + str(i)
                # url = "http://www.cc.chu.edu.tw/~u8704002/My%20Webs/content1/proxy.htm"
                req = requests.get(url, headers=header)
                res = req.content
                soup = BeautifulSoup(res, "lxml")
                ips = soup.findAll('tr')
                for x in range(1, len(ips)):
                    ip = ips[x]
                    tds = ip.findAll("td")
                    ip_temp = tds[1].contents[0] + "\t" + tds[2].contents[0]
                    proxy.append(ip_temp)
            except Exception:
                continue
        return proxy

    '''
    验证获得的代理IP地址是否可用
    '''

    def validateIp(self, proxy):
        url = "http://ip.chinaz.com/getip.aspx"
        url = "http://www.twse.com.tw/exchangeReport/STOCK_DAY/?response=json&date=201800&stockNo=6591"
        socket.setdefaulttimeout(3)
        proxy_ip = []
        for i in range(0, len(proxy)):
            try:
                ip = proxy[i].strip().split("\t")
                proxy_host = "http://" + ip[0] + ":" + ip[1]
                proxy_temp = {"http": proxy_host}
                print('test : ' + proxy[i])
                res = requests.get(url, proxies=proxy_temp, timeout=1, headers=header)
                json = res.json()
                print(proxy[i])
                proxy_ip.append(proxy_temp)
                if len(proxy_ip) > 20:  # 要刪掉
                    return proxy_ip
            except Exception:
                continue
        return proxy_ip


if __name__ == '__main__':
    proxy_get = ProxyGet()
