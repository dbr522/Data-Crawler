#coding=utf-8
import requests
from bs4 import BeautifulSoup
import time
import datetime
import pandas as pd

def get_AQI():
    url = 'http://www.pm25.in/rank/'
    feedback = requests.get(url)
    feedback = feedback.text

    soup = BeautifulSoup(feedback, 'lxml')

    ranktable = soup.find('table', 'table table-striped table-bordered table-condensed')
    info_time = soup.find('div','time').p.text.encode('utf-8').replace('数据更新时间：','')

    contents = ranktable.tbody.findAll('tr')
    output_table = []

    for content in contents:
        result = []
        for t in content.findAll('td')[1:]:
            result.append(t.text)
        output_table.append(result)

    df = pd.DataFrame(output_table, columns=['City','AQI','Class','PrimeMatter','PM25','PM10','CO','NO2','O3','O3_8h','SO2'])
    df['Time'] = info_time

    return df

def main():
    while 1:
        try:
            cur_aqi = get_AQI()
            cur_time = cur_aqi['Time'][0]

            print time.asctime(), 'collecting:', cur_time

            cur_aqi.to_csv('F:\\Duan\\Crawler\\AQI\\data\\raw\\'+cur_time[2:10]+'-'+cur_time[11:13]+'.csv', index=False, encoding='gbk')
            print 'exporting ' + cur_time

            print 'waiting for one hour...'
            time.sleep(3599)

        except:
            print time.asctime(), 'fetch failed...'
            time.sleep(3599)

if __name__ == '__main__':
    main()
