# encoding: UTF-8

from pymongo import MongoClient, ASCENDING
from pymongo.errors import ConnectionFailure, OperationFailure
from vnpy.trader.language.english.constant import LOG_DB_NAME

from vnpy.trader.language import text
from vnpy.trader.vtGlobal import globalSetting


#########################################################################
class mongoDbClient(object):

    # ----------------------------------------------------------------------
    def __init__(self, logger):
        """Constructor"""
        self.logger = logger
        self.dbClient = None

    # ----------------------------------------------------------------------
    def dbConnect(self):
        """连接MongoDB数据库"""
        if not self.dbClient:
            # 读取MongoDB的设置
            try:
                # 设置MongoDB操作的超时时间为0.5秒
                self.dbClient = MongoClient(globalSetting['mongoHost'], globalSetting['mongoPort'],
                                            connectTimeoutMS=500)
                db_auth = self.dbClient.admin
                try:
                    db_auth.authenticate(globalSetting['mongoUser'], globalSetting['mongoPass'])
                except OperationFailure:
                    self.logger.info(u'Authentication failed, try not doing any auth.')
                # 调用server_info查询服务器状态，防止服务器异常并未连接成功
                self.dbClient.server_info()

                self.logger.info(text.DATABASE_CONNECTING_COMPLETED)

            except ConnectionFailure:
                self.logger.error(text.DATABASE_CONNECTING_FAILED)

    # ----------------------------------------------------------------------
    def dbInsert(self, dbName, collectionName, d):
        """向MongoDB中插入数据，d是具体数据"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.insert_one(d)
        else:
            self.logger.error(text.DATA_INSERT_FAILED)

    # ----------------------------------------------------------------------
    def dbDelete(self, dbName, collectionName, d):
        """向MongoDB中delete数据，"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.delete_one(d)
        else:
            self.logger.error(text.DATA_DELETE_FAILED)

    # ----------------------------------------------------------------------
    def dbQuery(self, dbName, collectionName, d, sortKey='', sortDirection=ASCENDING):
        """从MongoDB中读取数据，d是查询要求，返回的是数据库查询的指针"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]

            if sortKey:
                cursor = collection.find(d).sort(sortKey, sortDirection)  # 对查询出来的数据进行排序
            else:
                cursor = collection.find(d)

            if cursor:
                return list(cursor)
            else:
                return []
        else:
            self.logger.error(text.DATA_QUERY_FAILED)
            return []

    # ----------------------------------------------------------------------
    def dbUpdate(self, dbName, collectionName, d, flt, upsert=False):
        """向MongoDB中更新数据，d是具体数据，flt是过滤条件，upsert代表若无是否要插入"""
        if self.dbClient:
            db = self.dbClient[dbName]
            collection = db[collectionName]
            collection.replace_one(flt, d, upsert)
        else:
            self.logger.error(text.DATA_UPDATE_FAILED)

    # ----------------------------------------------------------------------

    def dbLogging(self, event):
        """向MongoDB中插入日志"""
        log = event.dict_['data']
        d = {
            'content': log.logContent,
            'time': log.logTime,
            'gateway': log.gatewayName
        }
        self.dbInsert(LOG_DB_NAME, self.todayDate, d)
