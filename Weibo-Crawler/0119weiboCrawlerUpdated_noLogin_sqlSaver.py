# -*- coding: utf-8 -*
# Improved by BR Duan
# Weibo.cn, News Crawling
# Don't have to login!!

import urllib2
import HTMLParser
from bs4 import BeautifulSoup as bs
import sys
import time
import random
import gzip
import StringIO
import pymssql
from multiprocessing import Pool


class MSSQL:

    def __init__(self, host, user, password, database):
        self.host = host
        self.user = user
        self.password = password
        self.database = database

    def connect(self):
        self.conn = pymssql.connect(host=self.host, user=self.user, password=self.password, database=self.database, charset='utf8')
        cur = self.conn.cursor()

        if not cur:
            raise(NameError, 'connect failed!')
        else:
            print 'connect succeed!'
            return (cur, self.conn)


class Fetcher(object):

    def __init__(self):
        pass
    
    def visit(self, url, error=0):
        headers = {
        "User-agent":"spider",
        "Accept-Encoding":"gzip"
        }
        request = urllib2.Request(url, headers = headers)
        sleeper = random.uniform(0, 2)
        try:
            result = urllib2.urlopen(request, timeout=8)
            isGzip = result.headers.get('Content-Encoding')
            if isGzip :
                compresseddata = result.read()
                compressedstream = StringIO.StringIO(compresseddata)
                gzipper = gzip.GzipFile(fileobj=compressedstream)
                data = gzipper.read()
            else:
                data = result.read()
            time.sleep(sleeper)
            return data
        except:
            print sys.exc_info()
            error = error + 1
            if error < 4:
                result = self.visit(url, error)
            else:
                result = None
            return result

    def newsCrawler(self, url):
        data = self.visit(url)
        if data is not None:
            soup = bs(data)
            contents = soup.find_all('div', class_='c', id=True)
            html_parser = HTMLParser.HTMLParser()
            space = html_parser.unescape('&nbsp;')  # transform the html &nbsp for space

            for content in contents:

                w = open(outputPath, 'a')
                try:
                    userName = content.find('a').string     # username
                    userLink = content.find('a')['href']    # get the link for this user's personal page
                    uid = str(userLink.split('/')[-1].strip())
                    '''
                    userPage = self.visit(userLink)  # visit peronal page for the user
                    if userPage is not None:
                        userSoup = bs(userPage)
                        print userSoup
                        userInfo = userSoup.find('div', class_='u')
                        try:
                            sex = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[0]    # get the sex of the user
                        except:
                            sex = 'Unknown Sex'
                        try:
                            origin = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[1].strip()   # get the origin of the user
                        except:
                            origin = 'Unknown Origin'
                        verifyInfo = userInfo.find('img', alt='V')
                        if verifyInfo is not None:
                            V = 'Verified'
                        else:
                            V = 'Unverified'

                        try:
                            accountInfo = userInfo.find('div', class_='tip2')
                            numPost = accountInfo.find('span', class_='tc').get_text().split('[')[1].replace(']', '')     #get the number of post
                            follow = accountInfo.find_all('a', href=True)[0].get_text().split('[')[1].replace(']', '')     # get the number of followers
                            friends = accountInfo.find_all('a', href=True)[1].get_text().split('[')[1].replace(']', '')     # get the number of friends the user is following
                        except:
                            numPost = ''
                            follow = ''
                            friends = ''
                    '''
                    text = content.find('span', class_=True).get_text().replace(':','')
                    try:
                        device = content.find('span', class_='ct').get_text().split(space)[1]   # get the device this user used
                    except:
                        device = 'Unknown Device'

                    postTime = str(time.strftime("%Y-%m-%d %H:%M", time.localtime(time.time())))

                    try:
                        lat = content.find_all('a', href=True)[2]['href'].split('=')[1].split('&')[0].split(',')[0]
                        lon = content.find_all('a', href=True)[2]['href'].split('=')[1].split('&')[0].split(',')[1]
                        placeName = content.find_all('a', href=True)[1].get_text()

                    except:
                        lat = ''
                        lon = ''
                        placeName = ''

                    try:
                        # result = [userName, uid, sex, origin, numPost, follow, friends, text, postTime, V, device, lat, lon, placeName]
                        # result = [userName, uid, text, postTime, device, lat, lon, placeName]
                        values = (userName.decode('utf-8'), uid.decode('utf-8'), text.decode('utf-8'),
                                  postTime.decode('utf-8'), device.decode('utf-8'), lat.decode('utf-8'),
                                  lon.decode('utf-8'), placeName.decode('utf-8'))
                        # print userName, sex, device, V, origin, placeName
                        # print 'number of weibo: %s, number of followers: %s, number of friends: %s' % (numPost, follow, friends)
                        # print text
                        query = 'select * from Table_1 where uid=%s and text=%s'
                        value = (uid, text.decode('utf-8'))
                        cur.execute(query, value)
                        result = cur.fetchall()
                        if len(result) != 0:
                            print 'repeat weibo'
                        else:
                            try:
                                sql = 'insert into Table_1 values (%s, %s, %s, %s, %s, %s, %s, %s)'
                                cur.execute(sql, values)
                                conn.commit()
                            except Exception, e:
                                print e
                                continue
                    except:
                        continue

                except:
                    continue
                w.close()

    def writeTitle(self):
        w = open(outputPath, 'a')
        w.write('userName;uid;sex;origin;numPost;follow;friends;text;postTime;V;device;lat;lon;placeName' + '\n')
        w.close()

outputPath = 'E:/others/1221weiboLogin/0119_testOutput.txt'
host = '127.0.0.1'
user = 'sa'
pw = 'a123456'
database = 'Test_Weibo'
'''
def worker():

    fet1 = Fetcher()

    for n in range(1, 10):
        t0 = time.time()
        for i in range(1, 10):
            print 'This is page: ' + str(i)
            url1 = 'http://weibo.cn/news/?page=%s' % i # Look up News
            fet1.newsCrawler(url1)
        t1 = time.time()
        t = t1 - t0

        print 'Have completed %s times of crawling, last round: %s seconds' % (n, t)
        time.sleep(5)


def runWorker():
    pool = Pool(processes=4)
    while True:
        pool.map(worker)
'''

mssql = MSSQL(host, user, pw, database)
(cur, conn) = mssql.connect()

k = 0
while True:
    fet1 = Fetcher()
    for n in range(1, 10):
        k = k + 1
        t0 = time.time()
        for i in range(1, 10):
            print 'This is page: ' + str(i)
            url1 = 'http://weibo.cn/news/?page=%s' % i # Look up News
            fet1.newsCrawler(url1)
        t1 = time.time()
        t = t1 - t0
        print 'Have completed %s times of crawling: last round: %s seconds' % (k, t)
        time.sleep(5)

