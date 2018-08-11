# encoding: UTF-8

import datetime as dt
import json

import requests

from vnpy.trader.app.dataRecorder.drBase import *
from vnpy.trader.vtLogger import *

# OKEX has 1min/3min/5min/15min/30min/1day/3day/1week/1hour/2hour/4hour/6hour/12hour
periodToDbNameMap = {"1min": MINUTE_DB_NAME, "1day": DAILY_DB_NAME}
timeDeltaMap = {"1min": dt.timedelta(minutes=1), "1day": dt.timedelta(days=1)}

OKEX_HISTORICAL_FUTURE_KLINE = "https://www.okex.com/api/v1/future_kline.do?symbol=%s&type=%s&contract_type=%s"
DEFAULT_HISTORY_DELTA = dt.timedelta(weeks=4)


#########################################################################
class mongoDataChecker(object):

    # ----------------------------------------------------------------------
    def __init__(self, client, logger, settingFilePath):
        """Constructor"""
        self.client = client
        self.logger = logger
        self.settingFilePath = settingFilePath

        self.barSettingDict = {}
        self.activeSettingDict = {}
        self.period = None
        self.targetDbName = None
        self.timeDelta = None
        self.fillData = False
        self.removeDuplicates = False
        self.fixDifference = False
        self.now = dt.datetime.now().replace(second=0, microsecond=0) - dt.timedelta(minutes=2)
        self.loadSettings()

        self.historyMaxDeltaDict = self.initMaxDeltDict()
        self.missingDataDict = {}
        self.duplicateDataDict = {}
        self.remoteBarData = {}

    # ----------------------------------------------------------------------
    def loadSettings(self):
        with open(self.settingFilePath) as file:
            drcSetting = json.load(file)
        self.period = drcSetting['period']
        self.targetDbName = periodToDbNameMap[self.period]
        self.timeDelta = timeDeltaMap[self.period]

        if 'fillData' in drcSetting:
            self.fillData = drcSetting['fillData']

        if 'removeDuplicates' in drcSetting:
            self.removeDuplicates = drcSetting['removeDuplicates']

        if 'fixDifference' in drcSetting:
            self.fixDifference = drcSetting['fixDifference']

        if 'bar' in drcSetting:
            barSettings = drcSetting['bar']

            for barSetting in barSettings:
                vtSymbol = barSetting[0]
                gateway = barSetting[1]
                isSpot = barSetting[2]
                if vtSymbol not in self.barSettingDict:
                    self.barSettingDict[vtSymbol] = {'symbol': vtSymbol, 'gateway': gateway, 'isSpot': isSpot}

        if 'active' in drcSetting:
            activeSetting = drcSetting['active']
            self.activeSettingDict = {vtSymbol: activeSymbol for activeSymbol, vtSymbol in activeSetting.items()}

    # ----------------------------------------------------------------------
    def requestRemoteData(self, vtSymbol, isSpot):
        if isSpot:
            return self.requestRemoteSpotData(vtSymbol)
        else:
            return self.requestRemoteFutureData(vtSymbol)

    # ----------------------------------------------------------------------
    def bestEffortRun(self):
        until = self.now
        historyMaxDelta = self.historyMaxDeltaDict[self.period]
        if historyMaxDelta:
            since = self.now - historyMaxDelta
        else:
            since = self.now - DEFAULT_HISTORY_DELTA
        return self.runCheckBetween(since, until)

    # ----------------------------------------------------------------------
    def runCheckBetween(self, since, until):
        if since >= until:
            self.logger.error("Requested start time " + since.strftime("%Y%m%d %H:%M:%S")
                              + " is greater than now" + until.strftime("%Y%m%d %H:%M:%S") + ".", 0)
            return
        else:
            for collection in self.barSettingDict:
                dbData = self.client.dbQuery(self.targetDbName, collection, self.mongoQuery(since), 'datetime')

                duplicatesInSingleCollection = self.checkDuplicatesForCollection(dbData, collection, since, until)
                missingInSingleCollection = self.checkMissingForCollection(dbData, collection, since, until)

                if missingInSingleCollection:
                    self.missingDataDict[collection] = missingInSingleCollection
                if duplicatesInSingleCollection:
                    self.duplicateDataDict[collection] = duplicatesInSingleCollection

        if self.removeDuplicates and self.duplicateDataDict:
            for vtSymbol, duplicateBarData in self.duplicateDataDict.items():
                self.logger.info(
                    "Start removing duplicates for %s from %s" % (vtSymbol, since.strftime("%Y%m%d %H:%M:%S")))
                self.removeDuplicateData(vtSymbol, self.getRemoteBarData(vtSymbol), duplicateBarData)
        else:
            self.logger.info("Remove duplicate data flag=false, or no duplicate needs to be removed.")

        if self.fillData and self.missingDataDict:
            for vtSymbol, missedBarData in self.missingDataDict.items():
                self.logger.info(
                    "Start backfilling miss data for %s from %s" % (vtSymbol, since.strftime("%Y%m%d %H:%M:%S")))
                self.persistMissingData(vtSymbol, self.getRemoteBarData(vtSymbol), missedBarData)
        else:
            self.logger.info("Fill data flag=false, or no data needs to be filled.")

        if self.fixDifference:
            for vtSymbol in self.barSettingDict:
                dbData = self.client.dbQuery(self.targetDbName, vtSymbol, self.mongoQuery(since), 'datetime')
                self.logger.info(
                    "Start checking data consistency for %s from %s" % (vtSymbol, since.strftime("%Y%m%d %H:%M:%S")))
                self.fixDifferentData(dbData, vtSymbol)
        else:
            self.logger.info("Fix difference flag=false.")

    # ----------------------------------------------------------------------
    def getRemoteBarData(self, vtSymbol):
        if not self.remoteBarData.has_key(vtSymbol):
            isSpot = self.barSettingDict[vtSymbol]['isSpot']
            self.remoteBarData[vtSymbol] = self.toBars(vtSymbol, self.requestRemoteData(vtSymbol, isSpot))
            self.logger.info(
                "%s bar data in total are retrieved from remote destination" % len(self.remoteBarData[vtSymbol]))
        return self.remoteBarData[vtSymbol]

    # ----------------------------------------------------------------------
    def checkMissingForCollection(self, dbData, collection, sinceRaw, until):
        since = sinceRaw.replace(second=0, microsecond=0)
        self.logger.info("start checking missing points of " + collection + " since "
                         + since.strftime("%Y%m%d %H:%M:%S") + " until "
                         + until.strftime("%Y%m%d %H:%M:%S"))
        persistedTimeList = [barData["datetime"] for barData in dbData]
        missedTimeList = []
        totalCheckCounter = 0
        missingPointsCounter = 0
        while since <= until:
            if since not in persistedTimeList:
                missedTimeList.append(since)
                missingPointsCounter = missingPointsCounter + 1
            totalCheckCounter = totalCheckCounter + 1
            since = since + self.timeDelta
        self.logger.info("%s out of %s points are missing" % (missingPointsCounter, totalCheckCounter))
        self.logger.info("Finish checking the missing points in " + collection + ".")
        return missedTimeList

    # ----------------------------------------------------------------------
    def checkDuplicatesForCollection(self, dbData, collection, sinceRaw, until):
        since = sinceRaw.replace(second=0, microsecond=0)
        self.logger.info("start checking duplicate points of " + collection + " since "
                         + since.strftime("%Y%m%d %H:%M:%S") + " until "
                         + until.strftime("%Y%m%d %H:%M:%S"))
        fullTimeList = {}
        duplicateTimeList = {}
        counter = 0
        for barData in dbData:
            ct = barData["datetime"]
            if ct in fullTimeList.keys():
                counter = counter + 1
                if duplicateTimeList.has_key(ct):
                    duplicateTimeList[ct].append(barData)
                else:
                    duplicateTimeList[ct] = [barData, fullTimeList[ct]]
            else:
                fullTimeList[ct] = barData
        duplicatelengh = len(duplicateTimeList)
        totalLength = duplicatelengh + len(fullTimeList)
        self.logger.info("%s out of %s points in time are duplicates" % (duplicatelengh, totalLength))
        self.logger.info("There are %s duplicate points among those time." % counter)
        self.logger.info("Finish checking the duplicate points in " + collection + ".")
        return duplicateTimeList

    # ----------------------------------------------------------------------
    def persistMissingData(self, collectionName, remoteBarData, missedBarData):
        activeCollectionName = self.activeSettingDict[collectionName]
        counter = 0
        for barData in remoteBarData:
            if barData.datetime in missedBarData:
                self.client.dbInsert(self.targetDbName, collectionName, barData.__dict__)
                self.logger.info(
                    "persisting bar data on %s into %s@%s" % (barData.datetime, self.targetDbName, collectionName))
                counter = counter + 1
                if activeCollectionName:
                    self.client.dbInsert(self.targetDbName, activeCollectionName, barData.__dict__)
                    self.logger.info("persisting bar data on %s into %s@%s" % (
                        barData.datetime, self.targetDbName, activeCollectionName))
        self.logger.info("%s out of %s missing data are filled in %s" % (counter, len(missedBarData), collectionName))

    # ----------------------------------------------------------------------
    def removeDuplicateData(self, collectionName, remoteBarData, duplciateBarData):
        activeCollectionName = self.activeSettingDict[collectionName]
        counter = 0
        for barData in remoteBarData:
            if barData.datetime in duplciateBarData.keys():
                for dup in duplciateBarData[barData.datetime]:
                    if dup['volume'] != barData.volume:
                        self.client.dbDelete(self.targetDbName, collectionName,
                                             {'volume': dup['volume'], 'datetime': dup['datetime']})
                        self.logger.info("Removed duplicate bar data on %s with volume=%s into %s@%s"
                                         % (barData.datetime, dup['volume'], self.targetDbName, collectionName))
                        counter = counter + 1
                        if activeCollectionName:
                            self.client.dbDelete(self.targetDbName, activeCollectionName,
                                                 {'volume': dup['volume'], 'datetime': dup['datetime']})
                            self.logger.info("Removed dupilcate bar data on %s with volume=%s into %s@%s"
                                             % (
                                                 barData.datetime, dup['volume'], self.targetDbName,
                                                 activeCollectionName))

        self.logger.info(
            "%s out of %s duplicate data are removed in %s" % (counter, len(duplciateBarData), collectionName))

    # ----------------------------------------------------------------------
    def fixDifferentData(self, dbData, vtSymbol):
        dbDataDict = {barData["datetime"]: barData for barData in dbData}
        activeVtSymbol = self.activeSettingDict[vtSymbol]
        totalCount = 0
        discrepencyCount = 0
        for remoteData in self.getRemoteBarData(vtSymbol):
            time = remoteData.datetime
            localData = dbDataDict.get(time)
            if localData:
                totalCount = totalCount + 1
                if (remoteData.volume != localData['volume'] or remoteData.close != localData['close']):
                    discrepencyCount = discrepencyCount + 1
                    self.client.dbDelete(self.targetDbName, vtSymbol,
                                         {'volume': localData['volume'], 'close': localData['close'], 'datetime': time})
                    self.logger.info("Removed different bar data on %s with volume=%s, close=%s into %s@%s"
                                     % (time, localData['volume'], localData['close'], self.targetDbName, vtSymbol))
                    self.client.dbInsert(self.targetDbName, vtSymbol, remoteData.__dict__)
                    self.logger.info("Replace with remote bar data on %s with volume=%s, close=%s into %s@%s"
                                     % (time, remoteData.volume, remoteData.close, self.targetDbName, vtSymbol))
                    if activeVtSymbol:
                        self.client.dbDelete(self.targetDbName, activeVtSymbol,
                                             {'volume': localData['volume'], 'close': localData['close'],
                                              'datetime': time})
                        self.logger.info("Removed different bar data on %s with volume=%s, close=%s into %s@%s"
                                         % (time, localData['volume'], localData['close'], self.targetDbName,
                                            activeVtSymbol))
                        self.client.dbInsert(self.targetDbName, activeVtSymbol, remoteData.__dict__)
                        self.logger.info("Replace with remote bar data on %s with volume=%s, close=%s into %s@%s"
                                         % (time, remoteData.volume, remoteData.close, self.targetDbName,
                                            activeVtSymbol))
        self.logger.info("%s out of %s different points are fixed for %s" % (discrepencyCount, totalCount, vtSymbol))

    # ----------------------------------------------------------------------
    def runCheckUntilNow(self, since):
        return self.runCheckBetween(since, self.now)

    # ----------------------------------------------------------------------
    def mongoQuery(self, since):
        return {"datetime": {"$gte": since}}

    # ----------------------------------------------------------------------
    def requestRemoteSpotData(self, vtSymbol):
        raise NotImplementedError("To be implement so it knows how to fetch data from the right place.")

    # ----------------------------------------------------------------------
    def requestRemoteFutureData(self, vtSymbol):
        raise NotImplementedError("To be implement so it knows how to fetch data from the right place.")

    # ----------------------------------------------------------------------
    def toBars(self, vtSymbol, rawData):
        raise NotImplementedError("To be implement so it knows how to fetch data from the right place.")

    # ----------------------------------------------------------------------
    def initMaxDeltDict(self):
        raise NotImplementedError("To be implement so it knows how to fetch data from the right place.")


#########################################################################
class okexMongoDataChecker(mongoDataChecker):

    # ----------------------------------------------------------------------
    def __init__(self, client, logger, settingFilePath):
        """Constructor"""
        super(okexMongoDataChecker, self).__init__(client, logger, settingFilePath)

    # ----------------------------------------------------------------------
    def requestRemoteFutureData(self, vtSymbol):
        try:
            symbol, contractType = self.resolveOkexFutureVtSymbol(vtSymbol)
            targetUrl = OKEX_HISTORICAL_FUTURE_KLINE % (symbol, self.period, contractType)
            self.logger.info("Requesting data from url=%s" % targetUrl)
            response = requests.get(targetUrl)
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.info("unsuccessful request code: %s" % response.status_code)
                return []
        except Exception as e:
            self.logger.info("httpGet failed, detail is:%s" % e)
            return []

    # ----------------------------------------------------------------------
    def resolveOkexFutureVtSymbol(self, vtSymbol):
        symbol, contractType, rest = vtSymbol.split(':')
        return symbol, contractType

    # ----------------------------------------------------------------------
    def toBars(self, vtSymbol, rawData):
        result = []
        for bar in rawData:
            barData = VtBarData()
            barData.symbol = vtSymbol
            barData.vtSymbol = vtSymbol
            barData.exchange = "OKEX"
            barData.gatewayName = self.barSettingDict[vtSymbol]['gateway'] + "_BackFilled"
            barTime = dt.datetime.fromtimestamp(float(bar[0]) / 1e3)

            barData.open = float(bar[1])
            barData.high = float(bar[2])
            barData.low = float(bar[3])
            barData.close = float(bar[4])
            barData.volume = float(bar[5])

            barData.date = barTime.strftime('%Y%m%d')
            barData.time = barTime.strftime('%H:%M:%S.%f')
            barData.datetime = dt.datetime.strptime(' '.join([barData.date, barData.time]), '%Y%m%d %H:%M:%S.%f')
            result.append(barData)
        return result

    # ----------------------------------------------------------------------
    def initMaxDeltDict(self):
        return {'1min': dt.timedelta(days=2)}
