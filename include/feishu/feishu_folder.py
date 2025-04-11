from include.feishu.feishu_client import FeishuClient
from lark_oapi.api.sheets.v3 import *
import lark_oapi as lark


class FeishuFolder(FeishuClient):
    def __init__(self, app_id=None, app_secret=None):
        FeishuClient.__init__(self, app_id, app_secret)

    def list_file(self, folder_token, direction='DESC', order_by='EditedTime', limit=200, page_token=None):
        uri = f"https://open.feishu.cn/open-apis/drive/v1/files?direction={direction}&folder_token={folder_token}'&order_by={order_by}&page_size={limit}"
        if page_token:
            uri = uri + f'&page_token={page_token}'
        request = BaseRequest.builder().http_method(lark.HttpMethod.GET) \
            .uri(uri) \
            .token_types({lark.AccessTokenType.TENANT}) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'list_file failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, 'dict')['data']
    
    def delete_file(self, file_token, type):
        uri = f'https://open.feishu.cn/open-apis/drive/v1/files/{file_token}?type={type}'
        request = BaseRequest.builder().http_method(lark.HttpMethod.DELETE) \
            .uri(uri) \
            .token_types({lark.AccessTokenType.TENANT}) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'delete_file failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, 'dict')['data']
    
    def create_folder(self, name, folder_token):
        body = {
            'name': name,
            'folder_token': folder_token
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.POST) \
            .uri('https://open.feishu.cn/open-apis/drive/v1/files/create_folder') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'create_folder failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, 'dict')['data']