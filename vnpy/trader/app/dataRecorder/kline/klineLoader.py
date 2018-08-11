import time
from datetime import timedelta, datetime

import pandas as pd
from vnpy.trader.utils.HttpMD5Util import *

TABLE_HEADERS = ["Date", "Time", "Open", "High", "Low", "Close", "TotalVolume"]


#########################################################################
class klineLoader(object):
    # ----------------------------------------------------------------------
    def __init__(self, exchange, symbol, timeFrame, daysAgo, contractType="SPOT", fileName=None):
        self.timeFrame = timeFrame
        self.contractType = contractType
        self.symbol = symbol
        self.exchange = exchange
        self.fileName = fileName
        self.daysAgo = daysAgo

        self.spotKlineRequestDict = {}
        self.futureKlineRequestDict = {}
        self.klineFormatingDict = {}
        self.initDicts()

    # ----------------------------------------------------------------------
    def initDicts(self):
        self.spotKlineRequestDict["bitfinex"] = self.getBfinexSpotKline
        self.klineFormatingDict["bitfinex"] = self.formatBfinextKline

    # ----------------------------------------------------------------------
    def getBfinexSpotKline(self, endTs):
        return httpGet("api.bitfinex.com", "/v2/candles/trade:%s:%s/hist" % (self.timeFrame, self.symbol),
                       'end=' + str(endTs * 1000) + '&limit=1000')

    # ----------------------------------------------------------------------
    def formatBfinextKline(self, candle):
        ts = candle[0] / 1000
        dt = datetime.fromtimestamp(ts)
        return ts, dt.strftime("%Y-%m-%d"), dt.strftime("%H:%M:%S"), candle[1], candle[2], candle[3], candle[4], candle[5]

    # ----------------------------------------------------------------------
    def isFuture(self):
        return self.contractType.upper() == "SPOT"

    # ----------------------------------------------------------------------
    def getOutPutFileName(self):
        if not self.fileName:
            self.fileName = self.exchange + "_" + self.timeFrame + "_" + self.symbol + "_" + self.contractType + ".csv"
        return self.fileName

    # ----------------------------------------------------------------------
    def loadKline(self):
        print('Loading kline in exchange-%s, symbol-%s, contractType-%s and timeframe-%s from %s days ago...' %
              (self.exchange, self.symbol, self.contractType, self.timeFrame, self.daysAgo))
        outputFileName = self.getOutPutFileName()
        historyStart = int(time.mktime((datetime.today() - timedelta(days=self.daysAgo)).timetuple()))
        historyEnd = None
        kline = None
        try:
            kline = pd.read_csv(outputFileName)
            historyEndDate = kline.iloc[-1].Date
            historyEndTime = kline.iloc[-1].Time
            historyEnd = int(time.mktime((datetime.strptime("%s %s" %  (historyEndDate, historyEndTime),
                                                            "%Y-%m-%d %H:%M:%S") + timedelta(minutes=1)).timetuple()))
            print("Found %s in directory, with %s entires, last one at %s. History starts %s"
                  % (outputFileName, kline.shape[0],
                     datetime.fromtimestamp(historyEnd), datetime.fromtimestamp(historyStart)))
        except Exception:
            kline = pd.DataFrame(columns = TABLE_HEADERS)
            historyEnd = time.time()
            print("Creating new %s, history start sets at %s and end sets at %s"
                  % (outputFileName, datetime.fromtimestamp(historyStart), datetime.fromtimestamp(historyEnd)))

        while historyEnd > historyStart:
            print("Current kline data size: %s" % kline.shape[0])
            if (self.contractType == "SPOT"):
                res = self.spotKlineRequestDict[self.exchange](historyEnd)
            else:
                res = self.futureKlineRequestDict[self.exchange](historyEnd)
            for candle in res:
                elements = self.formatBfinextKline(candle)
                historyEnd = elements[0]
                kline = kline.append({'Date': elements[1], 'Time': elements[2], 'Open': elements[3], 'Close': elements[4],
                               'High': elements[5], 'Low': elements[6], 'TotalVolume': elements[7]}, ignore_index=True)
            kline.to_csv(outputFileName, index=False)
            print("Uploaded untilL: %s" % datetime.fromtimestamp(historyEnd))

if __name__ == '__main__':
    # Change your input here:
    klineLoader("bitfinex", "tXRPUSD", "1m", 300).loadKline()
