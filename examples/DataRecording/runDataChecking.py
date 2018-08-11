# encoding: UTF-8

from datetime import time as tm
from time import sleep

from vnpy.trader.app.dataRecorder.drChecker import okexMongoDataChecker
from vnpy.db import *
from vnpy.trader.vtFunction import getJsonPath
from vnpy.trader.vtLogger import *


# ----------------------------------------------------------------------
def runChildProcess():
    """子进程运行函数"""
    # Initialize Logger
    logger = VtLogger("./temp/%s-%s.log" % ("RecordedDataChecker", datetime.now().strftime("%Y%m%dT%H:%M:%S")))
    timedPrint("Warming up RDC...")

    # Initialize MongoDb Client
    client = mongoDbClient(logger)
    client.dbConnect()

    # Initialize the configuration file
    settingFileName = '1min_DRC_setting.json'
    settingFilePath = getJsonPath(settingFileName, __file__)
    logger.info("Setting file name is %s" % settingFileName)

    # Initialize the checker
    checker = okexMongoDataChecker(client, logger, settingFilePath)
    timedPrint("RDC starts running...")
    checker.bestEffortRun()
    timedPrint("RDC finishes running...")


# ----------------------------------------------------------------------
def runParentProcess():
    """父进程运行函数"""
    # 创建日志引擎
    print("Starting RDC scheduler...")

    ONE_MIN_CHECK_START_FIRST = tm(02, 00)
    ONE_MIN_CHECK_END_FIRST = tm(02, 01)
    ONE_MIN_CHECK_START_SECOND = tm(14, 00)
    ONE_MIN_CHECK_END_SECOND = tm(14, 01)
    SLEEPING_LENGTH = 60

    timedPrint("The scheduled time is between %s and %s, as well as %s and %s, check every %s seconds." %
               (ONE_MIN_CHECK_START_FIRST.strftime("%H:%M:%S"), ONE_MIN_CHECK_END_FIRST.strftime("%H:%M:%S"),
                ONE_MIN_CHECK_START_SECOND.strftime("%H:%M:%S"), ONE_MIN_CHECK_END_SECOND.strftime("%H:%M:%S"),
                SLEEPING_LENGTH))

    while True:
        currentTime = datetime.now().time()
        timedPrint("Im good.")
        # 判断当前处于的时间段
        if ((currentTime >= ONE_MIN_CHECK_START_FIRST and currentTime < ONE_MIN_CHECK_END_FIRST) or
                (currentTime >= ONE_MIN_CHECK_START_SECOND and currentTime < ONE_MIN_CHECK_END_SECOND)):
            timedPrint("Now is the time (%s), kicking off the RDC." % currentTime.strftime("%H:%M:%S"))
            runChildProcess()

        sleep(SLEEPING_LENGTH)


# ----------------------------------------------------------------------
def timedPrint(msg):
    print(datetime.now().strftime("%Y%m%d %H:%M:%S.%f") + ": " + msg)


if __name__ == '__main__':
    runParentProcess()
    # runChildProcess()
