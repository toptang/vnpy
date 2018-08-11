import os

from vnpy.trader.language.english.constant import STATUS_NOTTRADED, STATUS_CANCELLED

__author__ = 'xx'

import base64
import hmac
import math
import random
import string
from hashlib import sha1
from urllib import quote

from tornado.httpclient import HTTPError

from alysettings import (ALIYUN_API_VERSION, ALIYUN_SMS_DOMAIN_V2, ALIYUN_SMS_DOMAIN, SMS_TEMPLATE_CODE_SINGLE_PRICE_UP,
                         SMS_TEMPLATE_CODE_SINGLE_PRICE_DOWN, REC_TEL_NO, SMS_TEMPLATE_CODE_MARKET_DATA_ERROR,
                         SMS_SIGN_NAME, SMS_HOSTNAME, SMS_TEMPLATE_CODE_ORDER_OPEN_CLOSE,
                         SMS_TEMPLATE_CODE_DELTA_HEDGE_REQUIRED)
from exhange_rest import ExchangeClient
from vnpy.trader.app.alertService.vtAlerter import VtAlerter
from vnpy.trader.vtLogger import *

log = VtLogger(os.path.dirname(os.path.abspath(__file__)) + "/aliYunAlerter.log", open_md='ab')


########################################################################
class AliyunClient(ExchangeClient):

    # ----------------------------------------------------------------------
    def __init__(self, root_url, access_key, access_secret):
        super(AliyunClient, self).__init__(root_url, access_key, access_secret)

    # ----------------------------------------------------------------------
    def _buildMySign(self, params, method='GET'):
        string_to_sign = ''
        for key in sorted(params.keys()):
            seg = key + '=' + quote(str(params[key]), safe='')
            string_to_sign += '&' + seg

        string_to_sign = quote(string_to_sign[1:], safe='')
        string_to_sign = method + '&%2F&' + string_to_sign
        hmac_sign_key = self._secret_key + '&'

        hmac_crypto = hmac.new(bytes(hmac_sign_key).encode('utf8'), string_to_sign.encode("utf-8"), sha1)
        sign_res = base64.b64encode(hmac_crypto.digest())
        sign_res = str(sign_res).encode("utf-8")
        return sign_res

    # ----------------------------------------------------------------------
    def getFullParamsV1(self, template_code, number, param_value):
        params = {
            # common parameters
            "RegionId": 'cn-hongkong',  # optional
            "AccessKeyId": self._api_key,
            "Format": "JSON",
            "SignatureMethod": "HMAC-SHA1",
            "SignatureVersion": "1.0",
            "SignatureNonce": self.uniqid(),
            "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Version": "2016-09-27",
            # SMS parameters
            "Action": "SingleSendSms",
            "SignName": SMS_SIGN_NAME,
            "TemplateCode": template_code,
            "RecNum": number,
            # custom parameters
            "ParamString": param_value
        }
        return params

    # ----------------------------------------------------------------------
    def getFullParamsV2(self, template_code, number, param_value):
        params = {
            # common parameters
            "RegionId": 'cn-hongkong',  # optional
            "AccessKeyId": self._api_key,
            "Format": "JSON",
            "SignatureMethod": "HMAC-SHA1",
            "SignatureVersion": "1.0",
            "SignatureNonce": self.uniqid(),
            "Timestamp": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
            "Version": "2017-05-25",  # v2
            "Action": "SendSms",  # v2
            "SignName": SMS_SIGN_NAME,
            "TemplateCode": template_code,
            "PhoneNumbers": number,  # v2
            # custom parameters
            "TemplateParam": param_value  # v2
        }
        return params

    # ----------------------------------------------------------------------
    def uniqid(cls, prefix='', more_entropy=False):
        m = time.time()
        uniqid = '%8x%05x' % (math.floor(m), int((m - math.floor(m)) * 1000000))
        if more_entropy:
            valid_chars = list(set(string.hexdigits.lower()))
            entropy_string = ''
            for i in range(0, 10, 1):
                entropy_string += random.choice(valid_chars)
            uniqid = uniqid + entropy_string
        uniqid = prefix + uniqid
        return uniqid


########################################################################
class SMSClient(AliyunClient, VtAlerter):

    # ----------------------------------------------------------------------
    def __init__(self, access_key, access_secret):
        if ALIYUN_API_VERSION == 'V2':
            super(SMSClient, self).__init__(ALIYUN_SMS_DOMAIN_V2, access_key, access_secret)
        else:
            super(SMSClient, self).__init__(ALIYUN_SMS_DOMAIN, access_key, access_secret)

    # ----------------------------------------------------------------------
    def sendOrderFillNotify(self, order):
        orderId = '#%s' % order.orderID
        quantity = round(float(order.tradedVolume), 2)
        totalQuantity = round(float(order.totalVolume), 2)
        price = round(float(order.price), 4)
        keyInfo = string.split(order.vtSymbol, ':')
        type = keyInfo[0] + "_" + keyInfo[1][:2].upper() + "_" + order.direction[:1].upper()
        exchange = string.split(keyInfo[2], '.')[1]
        if order.status == STATUS_NOTTRADED:
            status = "Opn"
        elif order.status == STATUS_CANCELLED:
            status = "Esc"
        else:
            status = "Done"

        log.info("Sending order fill notify for order: %s" % (str(order)))

        self._sendOrderFillNotify(orderId, quantity, price, totalQuantity, exchange, type, status)

    # ----------------------------------------------------------------------
    def sendDeltaHedgeNotify(self, deltaCheckResult):
        log.info("Sending delta hedge notification for: %s" % (str(deltaCheckResult)))
        type = deltaCheckResult.strategy
        actual = str(deltaCheckResult.exchangeDelta)
        plan = str(deltaCheckResult.localDelta)
        percentDiff = str(deltaCheckResult.percentDiff * 100) + "%"
        self._sendDeltaHedgeNotify(type, actual, plan, percentDiff)

    # ------------------------------------------------------------------------
    # including execution report: full fill and cancelled
    # def sendStrategyCompleteNotify(self, order):
    #     quantity = int(order.fillQuantity)
    #     price = round(float(order.averagePrice), 4)
    #     order_type_info = getTypeInfo(order)
    #     if order.status == ORDER_FILLED:
    #         comment = "Fully filled"
    #     else:
    #         comment = "NO|" + order.comment
    #
    #     # aliyun don't support field too long
    #     comment = comment[0:12]
    #
    #     log.info("Sending order completed notify for order: %s" % (str(id)))
    #
    #     self._sendStrategyCompleteNotify(id, quantity, price, order_type_info, comment)

    # ----------------------------------------------------------------------
    def sendMarketDataErrorNotify(self, security):
        log.info("Sending Market data error notify: %s" % (str(security)))
        self._sendMarketDataErrorNotify(security)

    # ----------------------------------------------------------------------
    # return True if succeeds
    def sendPriceNotify(self, price_type, operator, threshold):

        template_code = ''

        if operator == 'GE':
            template_code = SMS_TEMPLATE_CODE_SINGLE_PRICE_UP
        elif operator == 'LE':
            template_code = SMS_TEMPLATE_CODE_SINGLE_PRICE_DOWN

        params = {
            "type": price_type,
            "price": str(threshold)
        }
        param_value = self._generateParamString(params)

        log.info("Sending single mode price alert: %s %s %s" % (price_type, operator, str(threshold)))

        return self._sendSMS(template_code, param_value)

    # # ----------------------------------------------------------------------
    # def sendRiskRateLowNotify(self, account, coin, low_risk_rate):
    #     type = '%s-%s' % (account, coin.upper())
    #     risk_rate_str = '%.2f%%' % (low_risk_rate * 100)
    #     params = {
    #         "type": type,
    #         "rate": risk_rate_str
    #     }
    #
    #     param_value = self._generateParamString(params)
    #
    #     log.info("Sending risk rate low notify, account: %s, coin: %s, risk_rate: %s" % (account, coin, risk_rate_str))
    #     return self._sendSMS(SMS_TEMPLATE_CODE_RISK_RATE_LOW, param_value)

    # ----------------------------------------------------------------------
    # def sendComponentRedNotify(self, component_name, SMS_TEMPLATE_CODE_COMPONENT_RED=None):
    #
    #     params = {
    #         "component": component_name[0:15],
    #     }
    #
    #     param_value = self._generateParamString(params)
    #
    #     log.info("Sending component red notify %s" % (component_name))
    #
    #     return self._sendSMS(SMS_TEMPLATE_CODE_COMPONENT_RED, param_value)

    # ----------------------------------------------------------------------
    # def sendAccountWsRedNotify(self, account):
    #     params = {
    #         "account": str(account)[0:15],
    #     }
    #
    #     param_value = self._generateParamString(params)
    #
    #     log.info("Sending account ws red notify %s" % (account))
    #
    #     return self._sendSMS(SMS_TEMPLATE_CODE_ACCOUNT_WS_RED, param_value)

    # ----------------------------------------------------------------------
    def _sendOrderFillNotify(self, order_id, quantity, price, totalQuantity, exchange, type, status):
        params = {
            "prod_id": str(order_id),
            "quantity": str(quantity) + "_" + status,
            "price": str(price),
            "from": exchange,
            "total": str(totalQuantity),
            "type": type
        }

        param_value = self._generateParamString(params)

        log.info("Sending order notify SMS for order:%s" % (str(order_id)))

        return self._sendSMS(SMS_TEMPLATE_CODE_ORDER_OPEN_CLOSE, param_value)

    # ----------------------------------------------------------------------
    def _sendDeltaHedgeNotify(self, type, actual, plan, percentDiff):
        params = {
            "type": type,
            "actual": actual,
            "plan": plan,
            "percentDiff": percentDiff,
        }

        param_value = self._generateParamString(params)

        log.info("Sending delta hedge notify SMS for strategy:%s" % type)

        return self._sendSMS(SMS_TEMPLATE_CODE_DELTA_HEDGE_REQUIRED, param_value)

    # ----------------------------------------------------------------------
    # def _sendStrategyCompleteNotify(self, order_id, quantity, price, order_type_info, comment):
    #
    #     params = {
    #         "prod_id": str(order_id),
    #         "quantity": str(quantity),
    #         "price": str(price),
    #         "type": order_type_info,
    #         "comment": comment
    #     }
    #
    #     param_value = self._generateParamString(params)
    #
    #     log.info("Sending order complete notify SMS for order:%s" % (str(order_id)))
    #
    #     return self._sendSMS(SMS_TEMPLATE_CODE_STRATEGY_COMPLETED, param_value)

    # ----------------------------------------------------------------------
    def _sendMarketDataErrorNotify(self, security):

        params = {
            "type": str(security),
        }

        param_value = self._generateParamString(params)

        log.info("Sending market data error SMS for:%s" % (str(security)))

        return self._sendSMS(SMS_TEMPLATE_CODE_MARKET_DATA_ERROR, param_value)

    # ----------------------------------------------------------------------
    def _generateParamString(self, param_value):
        """
        :param  param_value is a dict of parameter-value to be used in the template. Make sure that the key and value
                shall both be string and the length must NOT greater than 15 chars
        """
        string_value = "{"
        for key, value in param_value.items():
            string_value += "\"%s\":\"%s\"," % (key, value)
        # add host name
        string_value += "\"host\":\"%s\"" % (SMS_HOSTNAME[0:15])
        string_value += "}"
        return string_value

    # ----------------------------------------------------------------------
    def _sendSMS(self, template_code, param_value):
        for number in REC_TEL_NO:
            if ALIYUN_API_VERSION == 'V2':
                params = self.getFullParamsV2(template_code, number, param_value)
            else:
                params = self.getFullParamsV1(template_code, number, param_value)

            params["Signature"] = self._buildMySign(params, 'GET')
            try:
                self._get(path='/', params=params)
            except HTTPError as e:
                log.error("HTTP Error %d:%s" % (e.code, e.message), 66)



def main():
    # client = SMSClient(ALIYUN_ACCESS_KEY, ALIYUN_ACCESS_SECRET)
    # client.sendMarketDataErrorNotify("spot")
    # client.sendStrategyCompleteNotify("82")
    # client.sendStrategyCompleteNotify("115")
    pass


if __name__ == '__main__':
    main()
