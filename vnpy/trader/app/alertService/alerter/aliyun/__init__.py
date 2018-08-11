from aliyun_clients import SMSClient
from alysettings import ALIYUN_ACCESS_KEY, ALIYUN_ACCESS_SECRET

alerterName = 'Aliyun SMS Alerter'
alerterInstance = SMSClient(ALIYUN_ACCESS_KEY, ALIYUN_ACCESS_SECRET)
