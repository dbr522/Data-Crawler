# -*- coding: utf-8 -*
# Improved by BR Duan
# Weibo.cn Login, News Crawling

import urllib
import urllib2
import cookielib
import lxml.html as HTML
import HTMLParser
from bs4 import BeautifulSoup as bs
import sys
import time
import random
import gzip
import StringIO
from multiprocessing import Pool

class Fetcher(object):
    def __init__(self, username=None, pwd=None, cookie_filename=None):
        # 获取一个保存cookie的对象
        self.cj = cookielib.LWPCookieJar()
        if cookie_filename is not None:
            self.cj.load(cookie_filename)

        # 将一个保存cookie对象，和一个HTTP的cookie的处理器绑定
        self.cookie_processor = urllib2.HTTPCookieProcessor(self.cj)

        # 创建一个opener，将保存了cookie的http处理器，还有设置一个handler用于处理http的URL的打开
        self.opener = urllib2.build_opener(self.cookie_processor, urllib2.HTTPHandler)

        # 将包含了cookie、http处理器、http的handler的资源和urllib2对象绑定在一起
        urllib2.install_opener(self.opener)

        self.username = username
        self.pwd = pwd
        self.headers = {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:14.0) Gecko/20100101 Firefox/14.0.1',
                        'Referer':'','Content-Type':'application/x-www-form-urlencoded'}

    def get_rand(self, url):
        headers = {'User-Agent':'Mozilla/5.0 (Windows;U;Windows NT 5.1;zh-CN;rv:1.9.2.9)Gecko/20100824 Firefox/3.6.9',
                   'Referer':''}
        req = urllib2.Request(url, "", headers)
        login_page = urllib2.urlopen(req).read()
        rand = HTML.fromstring(login_page).xpath("//form/@action")[0]
        passwd = HTML.fromstring(login_page).xpath("//input[@type='password']/@name")[0]
        vk = HTML.fromstring(login_page).xpath("//input[@name='vk']/@value")[0]
        return rand, passwd, vk

    def login(self, username=None, pwd=None, cookie_filename=None):
        if self.username is None or self.pwd is None:
            self.username = username
            self.pwd = pwd
        assert self.username is not None and self.pwd is not None

        url = 'http://3g.sina.com.cn/prog/wapsite/sso/login.php?ns=1&revalid=2&backURL=http%3A%2F%2Fweibo.cn%2F&backTitle=%D0%C2%C0%CB%CE%A2%B2%A9&vt='
        
        # 获取随机数rand、password的name和vk
        rand, passwd, vk = self.get_rand(url)
        data = urllib.urlencode({'mobile': self.username,
                                 passwd: self.pwd,
                                 'remember': 'on',
                                 'backURL': 'http://weibo.cn/',
                                 'backTitle': '新浪微博',
                                 'vk': vk,
                                 'submit': '登录',
                                 'encoding': 'utf-8'})
        url = 'http://login.weibo.cn/login/?rand=' + rand

        # 模拟提交登陆
        page = self.fetch(url, data)
        # print page
        link = HTML.fromstring(page).xpath("//a/@href")[0]
        if not link.startswith('http://'): link = 'http://weibo.cn/%s' % link

        # 手动跳转到微薄页面
        self.fetch(link, "")

        # 保存cookie
        if cookie_filename is not None:
            self.cj.save(filename=cookie_filename)
        elif self.cj.filename is not None:
            self.cj.save()
        print 'login success!', str(username)

    def fetch(self, url, data):
        # print 'fetch url: ', url
        req = urllib2.Request(url, data, headers=self.headers)
        return urllib2.urlopen(req).read()
    
    def visit(self, url, error=0):
        opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(self.cj))
        headers = {
        "User-agent":"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/47.0.2526.106 Safari/537.36", 
        "Accept-Encoding":"gzip"
        }
        request = urllib2.Request(url, headers = headers)
        sleeper = random.uniform(0, 2)
        try:
            result = opener.open(request, timeout=5)
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

    def newsCrawler2(self, url):
        data = self.visit(url)
        if data is not None:
            soup = bs(data)
            print soup
            html_parser = HTMLParser.HTMLParser()
            space = html_parser.unescape('&nbsp;')  # transform the html &nbsp for space

            w = open(outputPath, 'a')
            userInfo = soup.find('div', class_='u')
            try:
                sex = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[0]    # get the sex of the user
            except:
                sex = 'Unknown Sex'
            try:
                origin = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[1].strip()   # get the origin of the user
            except:
                origin = 'Unknown Origin'
            try:
                verifyInfo = userInfo.find('img', alt='V')
            except:
                verifyInfo = None
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

            try:
                result = [sex, origin, numPost, follow, friends, V]
                print result
                # print 'number of weibo: %s, number of followers: %s, number of friends: %s' % (numPost, follow, friends)
                # print text
                w.write(';'.join(str(a.encode('utf8')) for a in result) + '\n')
            except:
                print 'wrong1'


            w.close()

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

                    userPage = self.visit(userLink)  # visit peronal page for the user
                    if userPage is not None:
                        userSoup = bs(userPage)
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
                        result = [userName, uid, sex, origin, numPost, follow, friends, text, postTime, V, device, lat, lon, placeName]
                        print userName, sex, device, V, origin, placeName
                        # print 'number of weibo: %s, number of followers: %s, number of friends: %s' % (numPost, follow, friends)
                        # print text
                        w.write(';'.join(str(a.encode('utf8')) for a in result) + '\n')
                    except:
                        continue

                except:
                    continue
                w.close()

    def userCrawler(self, url):
        userPage = self.visit(url)  # visit peronal page for the user
        if userPage is not None:
            userSoup = bs(userPage)
            print userSoup
            html_parser = HTMLParser.HTMLParser()
            space = html_parser.unescape('&nbsp;')  # transform the html &nbsp for space
            userInfo = userSoup.find('div', class_='u')
            try:
                sex = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[0]    # get the sex of the user
            except:
                sex = 'Unknown Sex'
            try:
                origin = userInfo.find('span', class_='ctt').get_text().split(space)[1].split('/')[1].strip()   # get the origin of the user
            except:
                origin = 'Unknown Origin'
            try:
                verifyInfo = userInfo.find('img', alt='V')
                if verifyInfo is not None:
                    V = 'Verified'
                else:
                    V = 'Unverified'
            except:
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
            try:
                result = [sex, origin, V, numPost, follow, friends]
                print result
            except:
                print 'result wrong'

    def writeTitle(self):
        w = open(outputPath, 'a')
        w.write('userName;uid;sex;origin;numPost;follow;friends;text;postTime;V;device;lat;lon;placeName' + '\n')
        w.close()

outputPath = 'E:/others/1221weiboLogin/0201_testOutput.txt'
accountList = [[username,password]]


'''
def worker(accounts):
    for account in accounts:
        un = account[0]
        pw = account[1]

        fet1 = Fetcher()
        fet1.login(un, pw)

        for n in range(1, 10):
            t0 = time.time()
            for i in range(1, 10):
                print 'This is page: ' + str(i) + ': ' + un
                url1 = 'http://weibo.cn/news/?page=%s' % i # Look up News
                fet1.newsCrawler(url1)
            t1 = time.time()
            t = t1 - t0
            if t < 60:
                print 'Something is wrong...Account: %s' % un
            else:
                print 'Have completed %s times of crawling: last round: %s seconds, account: %s' % (n, t, un)
            time.sleep(90)



def runWorker():
    pool = Pool(processes=2)
    while True:
        pool.map(worker, accountList)


if __name__ == '__main__':
    runWorker()
'''

fet1 = Fetcher()
fet1.login('61049816@qq.com', 'dbr251953')


url1 = 'http://weibo.cn/u/3812956827'
url2 = 'http://weibo.cn/news/?page=1'

fet1.newsCrawler(url1)
t1 = time.time()



