# -*- coding: utf-8 -*-
# Created by Bingruo Duan
# Weibo Checkin Daily Loop

import urllib2
import json
import time
import datetime
import math
import sys
import gzip
import StringIO
import pymssql
from multiprocessing import Pool

# Set global parameter
path = 'D:\\Data\\Regular Collection\\Weibo\\Data\\Beijing_6ring_20151009\\'
tokenFile = 'token_beijing.txt'
coordFile = 'coord_beijing_2000.txt'
searchRange = '1415'  # Radius is 2km
sleeper = 24

baseUrl = 'https://api.weibo.com/2/place/nearby_timeline.json'
count = '50'

host = '127.0.0.1'
user = 'sa'
pw = 'a123456'
database = 'Weibo Checkin'

# Get current time
a = time.localtime()
day = datetime.datetime(a[0], a[1], a[2])
yd = day - datetime.timedelta(days =1)
yyd = day - datetime.timedelta(days =2)

st = str(time.mktime(yyd.timetuple())).replace('.0', '')
et = str(time.mktime(yd.timetuple())).replace('.0', '')


print '-----Global parameter set-----'


# Connect to SQL
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

# Build token list
def genToken():
    tokens = []
    contents = open(path + tokenFile, 'r').readlines()
    for content in contents:
        content = content.replace('\n', '')
        tokens.append(content)
    return tokens


# Build coord list
def genCoord():
    coords = []
    contents = open(path + coordFile, 'r').readlines()
    for content in contents:
        content = content.split(',')
        coords.append([content[0], content[1], content[2].strip().replace('\n', '')])
    return coords


# Define generate URL by coord
def genURL(accessToken, lat, lon, page=1):
    url = baseUrl + "?" + "access_token=" + accessToken + "&lat=" + str(lat) + "&long=" + str(lon) + "&range=" + str(searchRange) + "&starttime=" + st + "&endtime=" + et + "&sort=1&count=" + count + "&page=" + str(page)
    # url = baseUrl + "?" + "access_token=" + accessToken + "&lat=" + str(lat) + "&long=" + str(lon) + "&range=" + str(searchRange) + "&sort=1&count=50&page=" + str(page)
    print url
    return str(url)


# Define fetch the generated url
def fetch(url, error = 0):
    try:
        headers = {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6',
                   "Accept-Encoding":"gzip"}
        request = urllib2.Request(url, headers=headers)

        time.sleep(sleeper)
        response = urllib2.urlopen(request, timeout=5)
        isGzip = response.headers.get('Content-Encoding')
        if isGzip:
            compressedData = response.read()
            compressedStream = StringIO.StringIO(compressedData)
            gzipper = gzip.GzipFile(fileobj=compressedStream)
            data = gzipper.read()
        else:
            data = response.read()
            # print data
        parsedData = json.loads(data)
    except:
        print sys.exc_info()
        time.sleep(sleeper)
        error = error + 1
        if error < 2:
            parsedData = fetch(url, error)
        else:
            parsedData = None

    return parsedData


# Define Get total number
def getTotal(parsedData):
    # Get total page number and retrieve the info on page 1
    try:
        numPosts = int(parsedData['total_number'])
        numPages = int(math.ceil(float(numPosts) / 50))
        # print'-----Total number get-----'
        # print '-----Total number = ' + str(numPages) + '-----'
        return numPages
    except:
        numPages = 0
        return numPages


# Define Extract Info
def extractInfo(content):
    try:
        checkinTime = content['created_at']
    except:
        checkinTime = ''
        print 'fetch detail failed'
        #continue
    try:
        weiboID = content['id']
    except:
        weiboID = ''
        print 'fetch detail failed'
        #continue
    try:
        text = content['text']
    except:
        text = ''
        print 'fetch detail failed'
        #continue
    try:
        numPic = str(len(content['pic_ids']))
    except:
        numPic = ''
        print 'fetch detail failed'
        #continue
    try:
        lat = str(content['geo']['coordinates'][0])
    except:
        lat = ''
        print 'fetch detail failed'
        #continue
    try:
        lon = str(content['geo']['coordinates'][1])
    except:
        lon = ''
        print 'fetch detail failed'
        #continue
    try:
        uid = content['user']['id']
    except:
        uid = ''
        print 'fetch detail failed'
        #continue
    try:
        province = content['user']['province']
    except:
        province = ''
        print 'fetch detail failed'
        #continue
    try:
        city = content['user']['city']
    except:
        city = ''
        print 'fetch detail failed'
        #continue
    try:
        location = content['user']['location']
    except:
        location = ''
        print 'fetch detail failed'
        #continue
    try:
        gender = content['user']['gender']
    except:
        gender = ''
        print 'fetch detail failed'
        #continue
    try:
        followers_count = content['user']['followers_count']
    except:
        followers_count = ''
        print 'fetch detail failed'
        #continue
    try:
        friends_count = content['user']['friends_count']
    except:
        friends_count = ''
        print 'fetch detail failed'
        #continue
    try:
        statuses_count = content['user']['statuses_count']
    except:
        statuses_count = ''
        print 'fetch detail failed'
        #continue
    try:
        created_at = content['user']['created_at']
    except:
        created_at = ''
        print 'fetch detail failed'
        #continue
    try:
        verified = content['user']['verified']
    except:
        verified = ''
        print 'fetch detail failed'
        #continue
    try:
        credit_score = content['user']['credit_score']
    except:
        credit_score = ''
        print 'fetch detail failed'
        #continue
    try:
        #result = (checkinTime+';'+str(weiboID)+';'+text+';'+numPic+';'+str(lat)+';'+str(lon)+';'+str(uid)+';'+str(province)+';'+str(city)+';'+location+';'+gender+';'+str(followers_count)+';'+str(friends_count)+';'+str(statuses_count)+';'+created_at+';'+str(verified)+';'+str(credit_score)+'\n')
        result = (checkinTime, str(weiboID), text, numPic, str(lat), str(lon), 
        str(uid), str(province), str(city),location, gender, str(friends_count),
        str(followers_count), str(statuses_count), created_at, str(verified), str(credit_score))
        # print checkinTime + '; ' + text
        return result
    except:
        print 'this piece is bad'
        result = None
        return result
        #continue


# Fetch loop and write
def worker(_param):
    mssql = MSSQL(host, user, pw, database)
    (cur, conn) = mssql.connect()
    accessToken = _param[0]
    coordList = _param[1]

    for coord in coordList:

        # locator = coord[0]
        # print 'this is ' + str(locator) + ' of ' + str(len(coordList))
        lat = coord[1]
        lon = coord[2]
        urlForPage = genURL(accessToken, lat, lon, 1)
        parseData1 = fetch(urlForPage)
        numPages = getTotal(parseData1)
        print '(' + str(lat) + ';' + str(lon) + '); ' + str(1) + ' of ' + str(numPages)
        if numPages != 0:

            contents = parseData1['statuses']
            if contents is not None:
                for content in contents:
                    result = extractInfo(content)
                    if result is not None:
                        sql = 'insert into Table_Beijing values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                        cur.execute(sql, result)
                        conn.commit()
            # Loop api from page 2
            for i in range(1, numPages + 1):
                print '(' + str(lat) + ';' + str(lon) + '); ' + str(i) + ' of ' + str(numPages)
                url = genURL(accessToken, lat, lon, i)
                parsedData = fetch(url)
                try:
                    contents = parsedData['statuses']
                    if contents is not None:
                        for content in contents:
                            result = extractInfo(content)
                            if result is not None:
                                sql = 'insert into Table_Beijing values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
                                cur.execute(sql, result)
                                conn.commit()
                except:
                    continue

# Delete repeat inputs
def deleteRep():
    mssql = MSSQL(host, user, pw, database)
    (cur, conn) = mssql.connect()
    query = "SELECT DISTINCT * INTO [Weibo Checkin].[dbo].[Temp] FROM [Weibo Checkin].[dbo].[Table_Beijing] \
    WHERE convert(int,cast(lat as float)) > 20 AND convert(int,cast(lat as float)) < 60 AND lat NOT Like '' \
    DROP TABLE [Weibo Checkin].[dbo].[Table_Beijing] \
    SELECT DISTINCT * INTO [Weibo Checkin].[dbo].[Table_Beijing] FROM [Weibo Checkin].[dbo].[Temp]\
    DROP TABLE [Weibo Checkin].[dbo].[Temp]"
    cur.execute(query)
    conn.commit()


# Multiprocessing
def startWorker():
    
    eve = int(math.ceil(len(AcoordList) / float(len(tokenList))))
    paramList = []

    for i in range(len(tokenList)):
        templist = []
        templist.append(tokenList[i])
        templist.append(AcoordList[i * eve:(i + 1) * eve])
        paramList.append(templist)

    pool = Pool(processes=len(tokenList))
    pool.map(worker, paramList)


# Export data
AcoordList = genCoord()
tokenList = genToken()

if __name__ == '__main__':
    t0 = time.time()
    startWorker()
    t1 = time.time()
    deleteRep()
    t = t1-t0
    print 'Have completed in %s seconds' % t
