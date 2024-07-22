import hmac
import hashlib
import os
import time
import json
import requests
from time import gmtime, strftime


class CoupangAPI:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key
        self.domain = "https://api-gateway.coupang.com"

    def generateHmac(self, method, url):
        path, *query = url.split('?')
        os.environ['TZ'] = 'GMT+0'
        dt_datetime = strftime('%y%m%d', gmtime()) + 'T' + strftime('%H%M%S', gmtime()) + 'Z'  # GMT+0
        msg = dt_datetime + method + path + (query[0] if query else '')
        signature = hmac.new(bytes(self.secret_key, 'utf-8'), msg.encode('utf-8'), hashlib.sha256).hexdigest()

        return 'CEA algorithm=HmacSHA256, access-key={}, signed-date={}, signature={}'.format(self.access_key,
                                                                                              dt_datetime, signature)

    def get_cps_d_agg(self, start_date, end_date):
        REQUEST_METHOD = "GET"
        URL = f"/v2/providers/affiliate_open_api/apis/openapi/v1/reports/commission?startDate={start_date}&endDate={end_date}"
        authorization = self.generateHmac(REQUEST_METHOD, URL)
        url = "{}{}".format(self.domain, URL)
        response = requests.request(method=REQUEST_METHOD, url=url,
                                    headers={
                                        "Authorization": authorization,
                                        "Content-Type": "application/json"
                                    })
        pretty_json = json.dumps(response.json(), indent=2, ensure_ascii=False)
        print(pretty_json)
        return response.json().get('data')