from httpx import Client
from datetime import datetime
from . import const as c
from . import utils as u


import urllib.parse
import logging
import json
import hmac
import hashlib
import base64
import time

logger = logging.getLogger(__name__)


class KsClient(Client):
    def __init__(self, app_key, app_secret, sign_secret, sign_method, version, base_url=c.BASE_URL, proxy=None):
        super().__init__(base_url=base_url, http2=True, proxy=proxy)
        self.app_key = app_key
        self.app_secret = app_secret
        self.sign_secret = sign_secret
        self.sign_method = sign_method
        self.version = version
        
    def generate_signature(self, params, access_token, api_name, timestamp):
        sign_string = {
            'access_token': access_token,
            'appkey': self.app_key,
            'method': api_name,
            'param': json.dumps(params),
            'signMethod': self.sign_method,
            'timestamp': timestamp,
            'version': self.version,
            'signSecret': self.sign_secret
        }
        sign_string = [f'{key}={value}' for key, value in sign_string.items() if value != 'null']
        sign_string = '&'.join(sign_string)
        signature = hmac.new(self.sign_secret.encode(), sign_string.encode(), hashlib.sha256).digest()
        signature = base64.b64encode(signature).decode()
        return signature
    
    def get_access_token(self, refresh_token: str):
        logger.info(f'TOKEN过期，更新TOKEN')
        url = 'https://openapi.kwaixiaodian.com/oauth2/refresh_token'
        body = {
            'grant_type': 'refresh_token',
            'refresh_token': refresh_token,
            'app_id': self.app_key,
            'app_secret': self.app_secret
        }
        response = self.post(url, data=body).json()

        if response['result'] == 1:
            new_refresh_token = response['refresh_token']
            new_access_token = response['access_token']
            logger.info(f'新TOKEN获取成功. access_token: {new_access_token}, refresh_token: {new_refresh_token}')
            return {
                'refresh_token': new_refresh_token,
                'access_token': new_access_token
            }
        else:
            logger.info('TOKEN无效，需要重新授权')
            raise
    
    def _request(self, method, api_name, access_token, params=None):
        timestamp = u.timestamp_now()
        signature = self.generate_signature(params=params, access_token=access_token, api_name=api_name, timestamp=timestamp)
        sys_params = urllib.parse.urlencode({
            'appkey': self.app_key,
            'timestamp': timestamp,
            'access_token': access_token,
            'version': self.version,
            'method': api_name,
            'sign': signature,
            'signMethod': self.sign_method
        })
        request_path = api_name.replace('.', '/') + f'?{sys_params}'
        
        if method == c.GET:
            if params:
                code_params = urllib.parse.quote_plus(json.dumps(params))
                request_path = request_path + f'&param={code_params}'
            response = self.get(request_path)
        elif method == c.POST:
            response = self.post(request_path, data=json.dumps(params))
        else:
            raise ValueError('method只能为GET或POST')

        return response.json()
    
    def get_cps_orders(self, access_token, begin_time: datetime, end_time: datetime, cps_order_status: int = 0, 
                       sort_type: int = 2, query_type: int = 2, page_size: int = 100, pcursor: str = ''):

        api_name = 'open.distribution.cps.distributor.order.cursor.list'

        begin_time = int(begin_time.timestamp() * 1000)
        end_time = int(end_time.timestamp() * 1000)

        params = {
            'sortType': sort_type,
            'queryType': query_type,
            'beginTime': begin_time,
            'endTime': end_time,
            'cpsOrderStatus': cps_order_status,
            'pageSize': page_size,
            'pcursor': pcursor
        }
        logger.info(params)

        result = []
        while params['pcursor'] != 'nomore':
            response = self._request(
                method='GET', access_token=access_token, api_name=api_name, params=params
            )
            if response['msg'] == 'success':
                result = result + response['data']['orderView']
                params['pcursor'] = response['data']['pcursor']
                continue
            else:
                break

        return result
    
    def get_leader_orders(self, access_token, begin_time: datetime, end_time: datetime, cps_order_status: int = 0,
                           sort_type: int = 2, query_type: int = 2, distributor_id: int = 0, fund_type: int = 1, 
                           page_size: int = 100, pcursor: str = ''):
        api_name = 'open.distribution.cps.leader.order.cursor.list'

        begin_time = int(begin_time.timestamp() * 1000)
        end_time = int(end_time.timestamp() * 1000)

        params = {
            'sortType': sort_type,
            'queryType': query_type,
            'beginTime': begin_time,
            'endTime': end_time,
            'cpsOrderStatus': cps_order_status,
            'pageSize': page_size,
            'pcursor': pcursor,
            'fundType': fund_type,
            'distributorId': distributor_id
        }
        logger.info(params)

        result = []
        while params['pcursor'] != 'nomore':
            response = self._request(
                method='GET', access_token=access_token, api_name=api_name, params=params
            )
            if response['msg'] == 'success':
                result = result + response['data']['orderView']
                params['pcursor'] = response['data']['pcursor']
                continue
            else:
                break

        return result

    def get_activity_info(self, access_token, limit: int = 500, offset: int = 0):
        api_name = 'open.distribution.investment.activity.open.list'

        params = {
            'limit': limit,
            'offset': offset
        }

        result = []

        response = self._request(
            method='GET', api_name=api_name, access_token=access_token, params=params
        )
        if response['msg'] == 'success':
            total = response.get('data').get('total')
            result = result + response['data']['result']
            # 数据量大于limit，需要循环获取
            if total > limit:
                params['offset'] = params['offset'] + limit
                while params['offset'] < total:
                    response = self._request(
                        method='GET', api_name=api_name, access_token=access_token, params=params
                    )
                    if response['msg'] == 'success':
                        result = result + response['data']['result']
                        params['offset'] = params['offset'] + limit
                        continue
                    else:
                        break
        else:
            return None

        return result
    
    def get_activity_item_list(self, access_token, activity_id, limit: int = 200, offset: int = 0):
        api_name = 'open.distribution.investment.activity.open.item.list'

        params = {
            'activity_id': activity_id,
            'limit': limit,
            'offset': offset
        }

        result = []

        response = self._request(
            method='GET', api_name=api_name, access_token=access_token, params=params
        )
        if response['msg'] == 'success':
            total = response.get('data').get('total')
            result = result + response['data']['activityItemDataList']
            # 数据量大于limit，需要循环获取
            if total > limit:
                params['offset'] = params['offset'] + limit
                while params['offset'] < total:
                    response = self._request(
                        method='GET', api_name=api_name, access_token=access_token, params=params
                    )
                    if response['msg'] == 'success':
                        result = result + response['data']['activityItemDataList']
                        params['offset'] = params['offset'] + limit
                        continue
                    else:
                        break
        else:
            return None

        return result