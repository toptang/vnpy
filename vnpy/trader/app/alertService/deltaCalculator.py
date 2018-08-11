import json

from vnpy.trader.app.ctaStrategy.ctaBase import POSITION_DB_NAME
########################################################################
from vnpy.trader.language.english.constant import DIRECTION_LONG
from vnpy.trader.vtObject import VtDeltaCheckResult


class DeltaCalculator(object):
    # ----------------------------------------------------------------------
    def __init__(self, mainEngine, settingFilePath, logger):
        self.mainEngine = mainEngine
        self.exchangeBalance = {}
        self.exchangeMargin = {}
        self.exchangePositions = {}
        self.exchangeAccountVsHoldingDict = {}
        self.livePrices = {}
        self.localPositions = {}
        self.logger = logger

        self.settingFilePath = settingFilePath
        self.strategies = {}
        self.strategiesRemainingAlertNumbs = {}
        self.warmingSeconds = 120
        self.freqInSeconds = 10
        self.timeRecorder = 0
        self.isReady = False
        self.loadSettings()

    # ----------------------------------------------------------------------
    def loadSettings(self):
        with open(self.settingFilePath) as file:
            dcSetting = json.load(file)
        self.strategies = dcSetting['strategies']
        self.warmingSeconds = dcSetting['warmingSeconds'] if dcSetting['warmingSeconds'] else self.warmingSeconds
        self.freqInSeconds = dcSetting['freqInSeconds'] if dcSetting['freqInSeconds'] else self.freqInSeconds
        for strategy in self.strategies.keys():
            self.strategiesRemainingAlertNumbs[strategy] = self.strategies[strategy]['maxAlertTimes']

    # ----------------------------------------------------------------------
    def updateExchangeAccountInfo(self, userInfo):
        account_id = userInfo.vtAccountID
        self.updateExchangeBalance(account_id, userInfo)
        self.updateExchangeMargin(account_id, userInfo)

    # ----------------------------------------------------------------------
    def updateExchangePositionInfo(self, pos):
        positionId = pos.vtPositionName
        try:
            self.exchangePositions[positionId] = pos

            account_id = pos.symbol + '.future.' + pos.exchange
            if not self.exchangeAccountVsHoldingDict.has_key(account_id):
                self.exchangeAccountVsHoldingDict[account_id] = set()
            self.exchangeAccountVsHoldingDict[account_id].add(positionId)
        except Exception, ex:
            self.logger.error("Error while updating Position details for %s, error: %s" % (positionId, ex))

    # ----------------------------------------------------------------------
    def updatePrices(self, tick):
        self.livePrices[tick.vtSymbol] = tick.lastPrice

    # ----------------------------------------------------------------------
    def updateExchangeMargin(self, account_id, userInfo):
        try:
            self.logger.info("Updating margin for %s, from %s to %s" %
                             (account_id,
                              self.exchangeMargin[account_id] if self.exchangeMargin.has_key(account_id) else "0",
                              userInfo.margin))
            self.exchangeMargin[account_id] = userInfo.margin
        except Exception, ex:
            self.logger.error("Error while updating account margin for %s, error: %s" % (account_id, ex))

    # ----------------------------------------------------------------------
    def updateExchangeBalance(self, account_id, userInfo):
        try:
            self.logger.info("Updating balance for %s, from %s to %s" %
                             (account_id,
                              self.exchangeBalance[account_id] if self.exchangeBalance.has_key(account_id) else "0",
                              userInfo.balance))
            self.exchangeBalance[account_id] = userInfo.balance
        except Exception, ex:
            self.logger.error("Error while updating account balanace for %s, error: %s" % (account_id, ex))

    # ----------------------------------------------------------------------
    def needPrice(self, tick):
        for positionedContract in self.exchangePositions:
            if str.startswith(str(positionedContract), tick.vtSymbol):
                return True
        for accountName in self.exchangeBalance.keys():
            if self.toSpotVtSymbol(accountName) == tick.vtSymbol:
                return True
        return False

    # ----------------------------------------------------------------------
    def toSpotVtSymbol(self, accountName):
        return str.split(str(accountName), '.')[0] + "." + str.split(str(accountName), '.')[2]

    # ----------------------------------------------------------------------
    def timeToRunDeltaCheck(self):
        self.timeRecorder = self.timeRecorder + 1
        if self.warmingSeconds + self.freqInSeconds == self.timeRecorder:
            self.timeRecorder = self.warmingSeconds
            return True
        return False

    # ----------------------------------------------------------------------
    def areYouReady(self):
        if not self.isReady:
            for stratgy in self.strategies.values():
                ## Strategy could find its account
                if not self.exchangeBalance.has_key(stratgy['account']):
                    return {"isReady": False, "msg": "No account found."}
            for account in self.exchangeBalance.keys():
                ## Account has spot price to calculate the holding positions
                if not self.livePrices.has_key(self.toSpotVtSymbol(account)):
                    return {"isReady": False, "msg": "No Spot price to calculate the holding pos."}
            self.isReady = True
        return {"isReady": True}

    # ----------------------------------------------------------------------
    def runDeltaCheck(self):
        results = []
        for strategy in self.strategies.keys():
            account = self.strategies[strategy]['account']
            strategyTable = self.strategies[strategy]['table']
            multiplier = int(self.strategies[strategy]['multiplier'])
            strategyVtSymbol = self.strategies[strategy]['vtSymbol']
            threshold = self.strategies[strategy]['threshold']
            remainingAlertNumbs = self.strategiesRemainingAlertNumbs[strategy]

            localPos = self.getPersistedLocalPosition(strategy, strategyVtSymbol, strategyTable)
            localDelta = round(localPos * multiplier, 2)

            exchangeSpotPos = self.exchangeBalance[account]
            spotPrice = self.livePrices[self.toSpotVtSymbol(account)]

            exchangeFuturePos = 0
            for holdingSymbol in self.exchangeAccountVsHoldingDict[account]:
                holding = self.exchangePositions[holdingSymbol]
                if holding.direction == DIRECTION_LONG:
                    exchangeFuturePos = exchangeFuturePos + holding.position
                else:
                    exchangeFuturePos = exchangeFuturePos - holding.position
            exchangeDelta = round(exchangeFuturePos * multiplier + exchangeSpotPos * spotPrice, 2)

            absDiff = abs(localDelta - exchangeDelta)
            percentDiff = round(absDiff / exchangeDelta, 2)

            deltaCheckresult = VtDeltaCheckResult()
            deltaCheckresult.strategy = strategy
            deltaCheckresult.localPos = localPos
            deltaCheckresult.localDelta = localDelta
            deltaCheckresult.exchangeSpotPos = exchangeSpotPos
            deltaCheckresult.spotPrice = spotPrice
            deltaCheckresult.exchangeNetFuturePos = exchangeFuturePos
            deltaCheckresult.exchangeDelta = exchangeDelta
            deltaCheckresult.absDiff = absDiff
            deltaCheckresult.percentDiff = percentDiff

            if abs(percentDiff) > threshold:
                if remainingAlertNumbs > 0:
                    deltaCheckresult.sendAlert = True
                    self.strategiesRemainingAlertNumbs[strategy] = remainingAlertNumbs - 1
                    self.logger.info("Going to send out notification for big delta difference on " + strategy)
                else:
                    self.logger.info("Not sending alert due to reaching alert number limit on" + strategy)
                    deltaCheckresult.sendAlert = False
            else:
                deltaCheckresult.sendAlert = False
            msg = "%s: local %s vs exchange %s, absDiff is %s, percentDiff is %s" \
                  % (strategy, localDelta, exchangeDelta, absDiff, percentDiff * 100) + "%."
            self.logger.info(msg)
            results.append(deltaCheckresult)
        return results


    # ----------------------------------------------------------------------
    def getPersistedLocalPosition(self, strategy, strategyVtSymbol, table):
        "Expect to be the unique result"
        return self.mainEngine.dbQuery(POSITION_DB_NAME, table,
                                       {"name": strategy, "vtSymbol": strategyVtSymbol})[0]['pos']
