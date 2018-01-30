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
    #如果url不存在会抛出ConnectionError，这个情况不做重试
    except requests.exceptions.ConnectionError as e:
        return
    return html
def closelink(cur, conn):  # 关闭数据库链接
    cur.close()
    conn.close()
def  insertaika(citylist,i):
    print 'A'
    header = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'
    }

    for city in citylist:
        s = requests.session()
        s.keep_alive = False
        try:
            req = s.get(city, headers=header, timeout=60)
            # raise_for_status(),如果不是200会抛出HTTPError错误
        except requests.HTTPError as e:
           print 'C'

            # 如果url不存在会抛出ConnectionError错误，这个情况不做重试
        except requests.exceptions.ConnectionError as e:
            print("url不存在!")
            break
        req.encoding = "gb2312"
        if req.status_code == 200:
            print("Request OK!")
            soup = BeautifulSoup(req.text, "html.parser")
            print ('B')
        # < div
        # id = "topadv_baidu_aibang"
        # bizTotal = "406"
        # baidukey = "汽车租赁" >
            testdd=soup.find("div", id="topadv_baidu_aibang")
            print testdd
            dealercount = int(soup.find("div", id="topadv_baidu_aibang").attrs["biztotal"])
            if dealercount >= 300:
                pagecount = 20
            else:
                pagecount = dealercount/15+1
            for pageindex in range(0, pagecount):
                # http://www.aibang.com/beijing/qichezulin/p1/
                cityname=str(city.split("/")[3])
                pagenum=str(pageindex+1)
                print (pagenum,cityname)
                url = city + "p" +pagenum+"/"
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
                req.encoding = "utf-8"
                if req.status_code == 200:
                    print("Request OK!")
                    soup = BeautifulSoup(req.text, "html.parser")
                    if len(soup.find_all("div",class_="aside")) > 0:
                        dealer_zh_list=soup.find_all("div", class_="aside")
                        if len(dealer_zh_list) == 0:
                            print("No more ZH dealers!")
                            break
                        else:
                            insertsql=""
                            conn = pymssql.connect(host="host", user="sa",
                                                   password="123456",
                                                   database="DB")
                            cur = conn.cursor()
                            if not cur:
                                raise (NameError, "链接数据库失败")
                            else:
                                print('success link sql')
                            for dealer_zh in dealer_zh_list:
                                if len(dealer_zh.find_all("div", class_="part1")) > 0:
                                    dealer_zh_name = dealer_zh.find_all("a", class_="title")[0].string
                                    #dealer_zh_name = dealer_zh.find_all("a")[0].string
                                    part1= dealer_zh.find_all("div", class_="part1")
                                    dealer_p = dealer_zh.find_all("div", class_="part1")[0].find_all("p")[0].string
                                    dealer_tel = dealer_zh.find_all("div", class_="part1")[0].find_all("p")[1].string
                                    print dealer_zh_name
                                    print dealer_tel
                                    print dealer_p
                                    print cityname
                                    insertsql = "INSERT INTO [dbo].[DimScraDealerInfo_AiBang]([DealerName],[DealerPhone],[DealerAddress],[DimCityName],[DealerSource])VALUES('%s','%s','%s','%s',N'爱帮');" % ( dealer_zh_name,dealer_tel, dealer_p,cityname)
                                    print  insertsql
                                    try:
                                        fobj = open('sqltxt.txt', 'a')  # 这里的a意思是追加，这样在加了之后就不会覆盖掉源文件中的内容，如果是w则会覆盖。
                                    except IOError:
                                        print '*** file open error:'
                                    else:
                                        fobj.write('\n' + insertsql)  # 这里的\n的意思是在源文件末尾换行，即新加内容另起一行插入。
                                        fobj.close()
                                    # cur.execute(insertsql, True)
                                    # conn.commit()
                    else:
                            break
                else:
                    print(req.status_code)
                    #os._exit()


                # else:
                #     print("No brand!")

            #cur.close()
            #conn.close()

if __name__ == '__main__':
    reload(sys)
    sys.setdefaultencoding("utf-8")
    # print '\xa0'.decode('utf-8')

    Mainurl = "http://www.aibang.com/wuhan/qichezulin/p1/"
    sql_citylist = [
        "http://www.aibang.com/beijing/qichezulin/",
        "http://www.aibang.com/shanghai/qichezulin/",
        "http://www.aibang.com/guangzhou/qichezulin/",
        "http://www.aibang.com/hangzhou/qichezulin/",
        "http://www.aibang.com/shenzhen/qichezulin/",
        "http://www.aibang.com/wuhan/qichezulin/",
        "http://www.aibang.com/zhengzhou/qichezulin/",
        "http://www.aibang.com/changsha/qichezulin/",
        "http://www.aibang.com/tianjin/qichezulin/",
        "http://www.aibang.com/haerbin/qichezulin/",
        "http://www.aibang.com/nanjing/qichezulin/",
        "http://www.aibang.com/dalian/qichezulin/",
        "http://www.aibang.com/shenyang/qichezulin/",
        "http://www.aibang.com/chengdu/qichezulin/",
        "http://www.aibang.com/qinghai/qichezulin/",
        "http://www.aibang.com/chongqing/qichezulin/",
        "http://www.aibang.com/xian/qichezulin/",
        "http://www.aibang.com/jinan/qichezulin/",
        "http://www.aibang.com/fuzhou/qichezulin/",
        "http://www.aibang.com/suzhou/qichezulin/"
    ]

    l = len(sql_citylist)
    a = l / 4
    b = (l / 4) * 2
    c = (l / 4) * 3
    print a, b, c
    relist1 = sql_citylist[0:a]
    relist2 = sql_citylist[a:b]
    relist3 = sql_citylist[b:c]
    relist4 = sql_citylist[c:]
    print relist1#len(relist1), len(relist2), len(relist3), len(relist4)
    p1 = multiprocessing.Process(target=insertaika, args=(relist1,1))
    p2 = multiprocessing.Process(target=insertaika, args=(relist2,2))
    p3 = multiprocessing.Process(target=insertaika, args=(relist3,3))
    p4 = multiprocessing.Process(target=insertaika, args=(relist4,4))
    p1.start()  # 启动进程
    p2.start()
    p3.start()
    p4.start()
    p1.join()  # 等子进程结束才执行主进程
    p2.join()
    p3.join()
    p4.join()

print("Execute OK!")
