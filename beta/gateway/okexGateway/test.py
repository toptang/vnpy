from beta.gateway.okexGateway import Api_contract, okexGateway

if __name__ == '__main__':
    gate = okexGateway(None, 'OKEX')

    okapi = Api_contract(gate)

    okapi.connect_Contract(apiKey='864f871b-726d-4cc3-a96e-da1ff121fa00',
                           secretKey= '70D993EEEB74C494556ABB33F5B956F5'
                           )
    okapi.login()
    while True:
        import time
        time.sleep(10)

    # okapi.subscribeFuturePositions()
    # okapi.subscribeFutureUserInfo()



