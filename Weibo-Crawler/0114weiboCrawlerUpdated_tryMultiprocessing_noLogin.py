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
from multiprocessing import Pool


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
                        result = [userName, uid, text, postTime, device, lat, lon, placeName]
                        # print userName, sex, device, V, origin, placeName
                        # print 'number of weibo: %s, number of followers: %s, number of friends: %s' % (numPost, follow, friends)
                        # print text
                        w.write(';'.join(str(a.encode('utf8')) for a in result) + '\n')
                    except:
                        continue

                except:
                    continue
                w.close()

    def writeTitle(self):
        w = open(outputPath, 'a')
        w.write('userName;uid;sex;origin;numPost;follow;friends;text;postTime;V;device;lat;lon;placeName' + '\n')
        w.close()

outputPath = 'E:/others/1221weiboLogin/0118_testOutput.txt'


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



'''
if __name__ == '__main__':
    runWorker()







url1 = 'http://weibo.cn/news/?page=3'  # Look up News
for i in range(1,10):
    url1 = 'http://weibo.cn/search/mblog?hideSearchFrame=&keyword=%E6%88%91%E5%9C%A8%E8%BF%99%E9%87%8C&page=' + str(i) + '&vt=4'
    
    data1 = fet.visit(url1)
    soup = bs(data1)
    contents = soup.find_all('div', class_='c', id=True)
    html_parser = HTMLParser.HTMLParser()
    space = html_parser.unescape('&nbsp;')  # transform the html &nbsp for space
    
    
    for test in contents:
        userName = test.find('a').string
        userLink = test.find('a')['href']
        #userPage = fet.visit(userLink)
        
        #userSoup = bs(userPage)
        #userInfo = userSoup.find('div', class_='u')
        #sex = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[0]
        #origin = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[1]
        #accountInfo = userInfo.find('div', class_='tip2')
        #numPost = accountInfo.find('span', class_='tc').get_text().split('[')[1].replace(']', '')
        #follow = accountInfo.find_all('a', href=True)[0].get_text().split('[')[1].replace(']', '')
        #friends = accountInfo.find_all('a', href=True)[1].get_text().split('[')[1].replace(']', '')
        
        
        text = test.find('span', class_=True).get_text().replace(':','')
        device = test.find('span', class_='ct').get_text().split(space)[1]
        try:
            lat = test.find_all('a', href=True)[2]['href'].split('=')[1].split('&')[0].split(',')[0]
            lon = test.find_all('a', href=True)[2]['href'].split('=')[1].split('&')[0].split(',')[1]
            placeName = test.find_all('a', href=True)[1].get_text()
            print placeName
        except:
            lat = ''
            lon = ''
            placeName = ''
            print 'no check in'

'''