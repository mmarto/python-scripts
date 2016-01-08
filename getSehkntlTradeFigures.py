# -*- coding: utf-8 -*-
"""
Created on Fri Jan  8 11:10:14 2016

@author: mhristov
Purpose: The script fetches the daily trading buy and sell figures of SEHKNTL for the previous week and calculates the total weekly trading (buy + sell)
url http://www.hkex.com.hk/eng/csm/dailystat/d{YYYYMMDD}e.htm
"""

import os, sys
import requests
from bs4 import BeautifulSoup
import datetime
import time
import logging
import traceback

logging.basicConfig(filename=os.path.join(os.path.dirname(__file__), 'log', 'log.txt'), 
                    filemode='w', level=logging.DEBUG, format='%(asctime)s %(message)s', datefmt='%Y%m%d %I:%M:%S %p')

def main():
    sys.excepthook = log_except_hook
    
    # settings
    cellId = 3842
    cellName = 'Total trade from SEHKNTL'
    outFile = os.path.join(os.path.dirname(__file__), 'sehkntlData', 'load_rep_sehkntl.{:%Y%m%d}.{:%Y%m%d}.lst')
    today = datetime.date.today()
    lastMon = today - datetime.timedelta(days=today.weekday(), weeks=1)
    lastSun = lastMon - datetime.timedelta(days=1)
    lastSat = lastMon + datetime.timedelta(days=5)

    logging.info('Started getting data for the week of {}'.format(lastMon))
    # get all working days Mon - Fri for the previous week 
    lastWeekDays = ['{:%Y%m%d}'.format(lastMon + datetime.timedelta(days=i)) for i in range(5)]

    totalTrades = 0
    for day in lastWeekDays:
        logging.info('Parsing day {}'.format(day))
        url = 'http://www.hkex.com.hk/eng/csm/dailystat/d{}e.htm'.format(day)
        # Get daily total trades
        trades = getDaylyFigure(url)
        print(trades, day)
        # Compute the weekly num of trades trades
        totalTrades += trades
        #print day, trades, totalTrades
        time.sleep(2)
    textFile = outFile.format(lastSun,lastSat)
    logging.info('Out file: {}'.format(textFile))

    with open(textFile, 'w') as o:
        print('{}|{:.0f}|{:%Y%m%d}|{:%Y%m%d}|{}'.format(cellId, totalTrades,lastSun,lastSat,cellName), file=o) 

def getDaylyFigure(url): # find nordbound table by the div id (NBTitle) above it
    dataDict = dict()
    r  = requests.get(url, timeout=180)
    data = r.text
    
    bs = BeautifulSoup(data, 'lxml')
    
    el = bs.find(id="NBTitle").parent.parent.findNext('tr')
    table = el.find('table')
    table2 = table.find('table')
    for row in table2.find_all('tr'):
        tds = row.find_all('td')
        key = tds[0].find(text=True)
        value = tds[1].find(text=True)
        if value == '-':
            logging.info("No figure found in {}".format(url))
            continue
        value = float(value.replace(',',''))
        dataDict[key.strip()] = value
        #print key, value    
    try:      
        #print(dataDict)
        return dataDict['No. of Buy Trades'] + dataDict['No. of Sell Trades']
    except KeyError as e:
        return 0
    
def log_except_hook(*exc_info):
    text = "".join(traceback.format_exception(*exc_info))
    logging.error("Unhandled exception: %s", text)


    
if __name__ == '__main__':
    main()