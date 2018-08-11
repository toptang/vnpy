from __future__ import division

import json

from tornado.httpclient import AsyncHTTPClient, HTTPClient, HTTPRequest
from urllib import urlencode

class ExchangeRestError(Exception):
    pass


class ExchangeRestClientError(ExchangeRestError):
    pass


class ExchangeRestInvalidNonceError(ExchangeRestClientError):
    pass


class ExchangeClient(object):

    def __init__(self, root=None, api_key=None, secret_key=None):
        self._root = root
        credentials = [api_key, secret_key]
        assert all(credentials)
        self.set_auth(*credentials)
        # self._requests = []

    def set_auth(self, api_key, secret_key):
        self._api_key = api_key
        self._secret_key = secret_key

    def _get(self, path, params=None, parser_fun=None, callback=None, ):
        if params:
            path += '?' + urlencode(params)

        return self._request('GET',
                             path=path,
                             callback=callback,
                             parser_fun=parser_fun)

    def _post(self, path, body=None, head=None, callback=None, parser_fun=None):
        return self._request('POST',
                             path=path,
                             body=body,
                             head=head,
                             parser_fun=parser_fun,
                             callback=callback,
                             )

    def _request(self, method, path, body=None, head=None, callback=None, parser_fun=None):
        client_class = AsyncHTTPClient if callback else HTTPClient

        request = HTTPRequest(
            url=self._root + path,
            method=method,
            body=body,
            headers=head,
        )
        client = client_class()
        if callback:
            return client.fetch(
                request=request,
                callback=lambda resp: callback(self._process_response(
                    resp, parser_fun))
            )
        else:
            return self._process_response(client.fetch(request), parser_fun)

    def _process_response(self, response, parser_fun=None):
        """
        :type response: tornado.httpclient.HTTPResponse
        """
        response.rethrow()

        parser_fun = parser_fun or (lambda x: x)

        # log.debug('< %s %s', response.headers, response.body)

        # content_type = response.headers['Content-Type']
        # if 'json' not in content_type:
        #    raise ExchangeRestError(
        #        'not JSON response (%s)' % content_type,
        #        response.headers,
        #        response.body
        #    )

        try:
            data = json.loads(response.body.decode('utf8'))
        except (ValueError, UnicodeError) as e:
            raise ExchangeRestError(
                'could not decode response json',
                response.headers,
                response.body, e)

        self._validate_response(data)
        data = parser_fun(data)
        return data

    def _validate_response(self, response):
        """
        :param response:
        :return: True,False
        """
        if 'error' in response:
            if response['error'] == 'Invalid nonce':
                raise ExchangeRestInvalidNonceError
            raise ExchangeRestClientError(response)
