# -*- coding:utf8 -*-
import multiprocessing
import threading as thd
import time
import urllib
import urllib2
from bs4 import BeautifulSoup
import requests
import pymssql
import os
import sys
import random

reload(sys)
sys.setdefaultencoding('utf8')

def Linksql(host, user, pwd, db):  # 链接数据库
    conn = pymssql.connect(host=host, user=user, password=pwd, database=db)
    cur = conn.cursor()
    if not cur:
        raise (NameError, "链接数据库失败")
    else:
        print 'success link sql'
        return conn, cur


def insertsql(conn, cur, sql):  # 向数据库插入数据
    cur.execute(sql, True)
    conn.commit()


def selectsql(cur, sql):  # 从数据库中查询数据
    cur.execute(sql)
    resList = cur.fetchall()
    return resList

def url_retry(url,num_retries=3):
    try:
        request = requests.get(url,timeout=60)
        #raise_for_status(),如果不是200会抛出HTTPError错误
        request.raise_for_status()
        html = request.content
    except requests.HTTPError as e:
        html=None
        if num_retries>0:
            #如果不是200就重试，每次递减重试次数
            return url_retry(url,num_retries-1)
    #如果url不存在会抛出ConnectionError错误，这个情况不做重试
    except requests.exceptions.ConnectionError as e:
        return
    return html
def closelink(cur, conn):  # 关闭数据库链接
    cur.close()
    conn.close()
def  insertaika(citylist,Mainurl):
    Mainurl = "http://dealer.xcar.com.cn/"
    '''
    重试次数
    '''
    RETRY_TIME = 2

    '''
    USER_AGENTS 随机头信息
    '''
    USER_AGENTS = [
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; AcooBrowser; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0; Acoo Browser; SLCC1; .NET CLR 2.0.50727; Media Center PC 5.0; .NET CLR 3.0.04506)",
        "Mozilla/4.0 (compatible; MSIE 7.0; AOL 9.5; AOLBuild 4337.35; Windows NT 5.1; .NET CLR 1.1.4322; .NET CLR 2.0.50727)",
        "Mozilla/5.0 (Windows; U; MSIE 9.0; Windows NT 9.0; en-US)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Win64; x64; Trident/5.0; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 2.0.50727; Media Center PC 6.0)",
        "Mozilla/5.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; .NET CLR 1.0.3705; .NET CLR 1.1.4322)",
        "Mozilla/4.0 (compatible; MSIE 7.0b; Windows NT 5.2; .NET CLR 1.1.4322; .NET CLR 2.0.50727; InfoPath.2; .NET CLR 3.0.04506.30)",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN) AppleWebKit/523.15 (KHTML, like Gecko, Safari/419.3) Arora/0.3 (Change: 287 c9dfb30)",
        "Mozilla/5.0 (X11; U; Linux; en-US) AppleWebKit/527+ (KHTML, like Gecko, Safari/419.3) Arora/0.6",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.8.1.2pre) Gecko/20070215 K-Ninja/2.1.1",
        "Mozilla/5.0 (Windows; U; Windows NT 5.1; zh-CN; rv:1.9) Gecko/20080705 Firefox/3.0 Kapiko/3.0",
        "Mozilla/5.0 (X11; Linux i686; U;) Gecko/20070322 Kazehakase/0.4.5",
        "Mozilla/5.0 (X11; U; Linux i686; en-US; rv:1.9.0.8) Gecko Fedora/1.9.0.8-1.fc10 Kazehakase/0.5.6",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_3) AppleWebKit/535.20 (KHTML, like Gecko) Chrome/19.0.1036.7 Safari/535.20",
        "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; fr) Presto/2.9.168 Version/11.52",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/536.11 (KHTML, like Gecko) Chrome/20.0.1132.11 TaoBrowser/2.0 Safari/536.11",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.71 Safari/537.1 LBBROWSER",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E; LBBROWSER)",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 LBBROWSER",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E; QQBrowser/7.0.3698.400)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SV1; QQDownload 732; .NET4.0C; .NET4.0E; 360SE)",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; QQDownload 732; .NET4.0C; .NET4.0E)",
        "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; WOW64; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)",
        "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.1 (KHTML, like Gecko) Chrome/21.0.1180.89 Safari/537.1",
        "Mozilla/5.0 (iPad; U; CPU OS 4_2_1 like Mac OS X; zh-cn) AppleWebKit/533.17.9 (KHTML, like Gecko) Version/5.0.2 Mobile/8C148 Safari/6533.18.5",
        "Mozilla/5.0 (Windows NT 6.1; Win64; x64; rv:2.0b13pre) Gecko/20110307 Firefox/4.0b13pre",
        "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:16.0) Gecko/20100101 Firefox/16.0",
        "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.1271.64 Safari/537.11",
        "Mozilla/5.0 (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10"
    ]
    header = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8",
              "Accept-Encoding": "gzip, deflate",
              "Accept-Language": "zh-CN,zh;q=0.8",
              "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36",
              "Host": "dealer.xcar.com.cn",
              "Upgrade-Insecure-Requests": "1",
              "Cookie": "Hm_lvt_53eb54d089f7b5dd4ae2927686b183e0=1504152310,1506063443; _Xdwuv=5060430936885; gourl=http%3A%2F%2Fdealer.xcar.com.cn%2F; BIGipServerpool-c26-xcar-dealerweb1-80=1221136138.20480.0000; _PVXuv=59c4b48325d6f; uv_firstv_refers=https%3A//www.baidu.com/link%3Furl%3DwU4TbE8xvJGAeqFqYz0idu1anVPfZCiaHhsa_lQ3FG1RPHzELoXvbhnitB76phB6%26wd%3D%26eqid%3Dd63026a5000319160000000359c4b47f; _Xdwnewuv=1; _Xdwstime=1506064236; uv_firstv_refer=www.baidu.com//%28%243%29//; ad__city=475; Hm_lpvt_53eb54d089f7b5dd4ae2927686b183e0=1506064184; place_prid=1; place_crid=475; place_ip=118.26.16.189_1; _locationInfo_=%7Burl%3A%22http%3A%2F%2Fbj.xcar.com.cn%2F%22%2Ccity_id%3A%22475%22%2Cprovince_id%3A%221%22%2Ccity_name%3A%22%25E5%258C%2597%25E4%25BA%25AC%22%7D"
        , "Connection": "keep-alive"
              }

    sql = "SELECT DISTINCT [BrandName],[BrandLink] FROM [Scrab].[DimScrabBrand] WHERE DataSource = '爱卡汽车'  "
    #sql_citylist = "SELECT [CityID],[CityName],[ProvinceName] FROM [Scrab].[DimScrabCity] WHERE DataSource = '爱卡汽车' "


    for city in citylist:
        conn = pymssql.connect(host="yourhost", user="account", password="pwd",
                               database="dbname")
        cur = conn.cursor()
        if not cur:
            raise (NameError, "链接数据库失败")
        else:
            print('success link sql')
            sql = "SELECT DISTINCT [BrandName],[BrandLink] FROM [Scrab].[DimScrabBrand] WHERE DataSource = '爱卡汽车'  "
            cur.execute(sql)
            brand_list = cur.fetchall()
            for brand in brand_list:
                # http://dealer.xcar.com.cn/d1_475/4.htm
                url1 = Mainurl + city[0] + "/" + brand[1].split("/")[2] + "?type=2&page="
                # print(url1)
                for pageindex in range(1, 10000):
                    url = url1 + str(pageindex)
                    print(url)
                    time.sleep(3)
                    #proxies = ProxyIP.get_ip()
                    #print proxies
                    s = requests.session()
                    s.keep_alive = False
                    try:
                        req = s.get(url, headers=header, timeout=60)
                        # raise_for_status(),如果不是200会抛出HTTPError错误
                    except requests.HTTPError as e:
                        html = None

                # 如果url不存在会抛出ConnectionError错误，这个情况不做重试
                    except requests.exceptions.ConnectionError as e:
                        print("url不存在!")
                        break
                    req.encoding = "gb2312"
                    if req.status_code == 200:
                        print("Request OK!")
                        soup = BeautifulSoup(req.text, "html.parser")
                        if len(soup.find_all("div", id="dlists_zh")) > 0:
                            dealer_zh_list = soup.find_all("div", id="dlists_zh")[0].find_all("dl", class_="offer_dl")
                            if len(dealer_zh_list) == 0:
                                print("No more ZH dealers!")
                                break
                            else:
                                for dealer_zh in dealer_zh_list:
                                    print  dealer_zh
                                    dealer_zh_id = dealer_zh.find_all("a")[0].attrs["href"].split("/")[1]
                                    dealer_zh_name = dealer_zh.find_all("a")[0].string
                                    dealer_zh_addr = dealer_zh.find_all("span")[1].string
                                    dealerLocation = city[2] + "/" + city[1] + "/" + dealer_zh_addr
                                    print (dealer_zh_id,dealer_zh_name,dealer_zh_addr)
                                    insertsql = "INSERT INTO [Scrab].[DimScraDealerInfo2]([DealerID],[DealerName],[DealerBrand],[DealerType],[DealerLocation],[DealerSource])VALUES('%s','%s','%s',N'综合店','%s',N'爱卡汽车')" % (dealer_zh_id, dealer_zh_name, brand[0], dealerLocation)
                                    print(insertsql)
                                    cur.execute(insertsql, True)
                                    conn.commit()
                        else:
                            break
                    else:
                        print(req.status_code)
                        os._exit()


                # else:
                #     print("No brand!")

            cur.close()
            conn.close()

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
    # print '\xa0'.decode('utf-8')

    Mainurl = "http://dealer.xcar.com.cn/"
    sql = "SELECT DISTINCT [BrandName],[BrandLink] FROM [Scrab].[DimScrabBrand] WHERE DataSource = '爱卡汽车'  "
    sql_citylist = "SELECT [CityID],[CityName],[ProvinceName] FROM [Scrab].[DimScrabCity] WHERE DataSource = '爱卡汽车' "
    conn = pymssql.connect(host="host", user="acount", password="pwd", database="dbname")
    cur = conn.cursor()
    if not cur:
        raise (NameError, "链接数据库失败")
    else:
        print('success link sql')
        cur.execute(sql_citylist)
        city_list = cur.fetchall()
        # print(city_list)
        # print(brand_list)
    # print starttime
    l = len(city_list)
    a = l / 4
    b = (l / 4) * 2
    c = (l / 4) * 3
    print a, b, c
    relist1 = city_list[0:a]
    relist2 = city_list[a:b]
    relist3 = city_list[b:c]
    relist4 = city_list[c:]
    print relist1#len(relist1), len(relist2), len(relist3), len(relist4)
    p1 = multiprocessing.Process(target=insertaika, args=(relist1, Mainurl))
    p2 = multiprocessing.Process(target=insertaika, args=(relist2, Mainurl))
    p3 = multiprocessing.Process(target=insertaika, args=(relist3, Mainurl))
    p4 = multiprocessing.Process(target=insertaika, args=(relist4, Mainurl))
    p1.start()  # 启动进程
    p2.start()
    p3.start()
    p4.start()
    p1.join()  # 等子进程结束才执行主进程
    p2.join()
    p3.join()
    p4.join()
print("finished!")
