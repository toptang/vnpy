import json
import os

from vnpy.trader.language.english.constant import STATUS_NOTTRADED, STATUS_CANCELLED, STATUS_ALLTRADED
from vnpy.trader.vtEvent import EVENT_POSITION, EVENT_ORDER, EVENT_ACCOUNT, EVENT_TICK, EVENT_TIMER
from vnpy.trader.vtFunction import getJsonPath
from vnpy.trader.vtLogger import VtLogger
from .alerter import aliyun
from .deltaCalculator import DeltaCalculator


########################################################################
class AlertEngine(object):

    # ----------------------------------------------------------------------
    def __init__(self, mainEngine, eventEngine):
        """Constructor"""
        self.mainEngine = mainEngine
        self.eventEngine = eventEngine
        self.alerterList = []
        self.registerEvent()
        self.applyAlerters()

        self.deltaCalculatorSetting = 'DC_setting.json'
        self.settingFilePath = getJsonPath(self.deltaCalculatorSetting, __file__)
        self.logger = VtLogger(os.path.dirname(os.path.abspath(__file__)) + "/alertEngine.log", open_md='ab')
        self.deltaCalculator = DeltaCalculator(mainEngine, self.settingFilePath, self.logger)

        self.startTimeTicking = False
        self.deltaCalculatorNotRunningAlertSent = False

    # ----------------------------------------------------------------------
    def registerEvent(self):
        self.eventEngine.register(EVENT_ORDER, self.processOrderEvent)
        self.eventEngine.register(EVENT_POSITION, self.processPositionEvent)
        self.eventEngine.register(EVENT_ACCOUNT, self.processAccountEvent)
        self.eventEngine.register(EVENT_TICK, self.processTickEvent)

    # ----------------------------------------------------------------------
    def applyAlerters(self):
        self.alerterList.append(aliyun.alerterInstance)

    # ----------------------------------------------------------------------
    def processOrderEvent(self, event):
        order = event.dict_['data']
        orderStatus = order.status
        # if (orderStatus == STATUS_NOTTRADED or orderStatus == STATUS_CANCELLED or orderStatus == STATUS_ALLTRADED):
        if (orderStatus == STATUS_ALLTRADED):
            self.logger.info("Sending notifications on order event:" + json.dumps(event.__dict__))
            for alerter in self.alerterList:
                alerter.sendOrderFillNotify(order)

    # ----------------------------------------------------------------------
    def processPositionEvent(self, event):
        pos = event.dict_['data']
        self.runTradeCompleteMonitor(pos)

    # ----------------------------------------------------------------------
    def processAccountEvent(self, event):
        userInfo = event.dict_['data']
        if userInfo.balance > 0:
            self.logger.info("Updating delta calculator on user info event:" + json.dumps(userInfo.__dict__))
            self.deltaCalculator.updateExchangeAccountInfo(userInfo)
            if not self.startTimeTicking:
                self.logger.info("Recieved account info, start listening to timer event. ")
                self.eventEngine.register(EVENT_TIMER, self.processTimerEvent)
                self.startTimeTicking = True

    # ----------------------------------------------------------------------
    def processTickEvent(self, event):
        tick = event.dict_['data']
        if self.deltaCalculator.needPrice(tick):
            self.deltaCalculator.updatePrices(tick)

    # ----------------------------------------------------------------------
    def processTimerEvent(self, event):
        self.runDeltaMonitor()

    # ----------------------------------------------------------------------
    def runTradeCompleteMonitor(self, pos):
        # Filtering out the spot on positions
        if pos.position > 0 and pos.price > 0:
            self.logger.info("Updating delta calculator on position event:" + json.dumps(pos.__dict__))
            self.deltaCalculator.updateExchangePositionInfo(pos)

    # ----------------------------------------------------------------------
    def runDeltaMonitor(self):
        if self.deltaCalculator.timeToRunDeltaCheck():
            answer = self.deltaCalculator.areYouReady()
            if answer['isReady']:
                deltaCheckResults = self.deltaCalculator.runDeltaCheck()
                for deltaCheckResult in deltaCheckResults:
                    if deltaCheckResult.sendAlert:
                        ## TODO DEBUG here
                        self.logger.info("Sending alerts on delta diffs:" + json.dumps(deltaCheckResult.__dict__))
                        for alerter in self.alerterList:
                            alerter.sendDeltaHedgeNotify(deltaCheckResult)
            else:
                msg = "Data not available in DC - %s" % answer['msg']
                self.logger.error(msg, 0)
                if not self.deltaCalculatorNotRunningAlertSent:
                    ## TODO implement this to send out alert
                    self.deltaCalculatorNotRunningAlertSent = True
