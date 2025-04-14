from include.feishu.feishu_client import FeishuClient
from lark_oapi.api.sheets.v3 import *
import lark_oapi as lark


class FeishuAppRobot(FeishuClient):
    def __init__(self, app_id=None, app_secret=None, name=None):
        FeishuClient.__init__(self, app_id, app_secret)
        self.name = name

    def get_robot_name(self):
        return self.name

    def send_msg(self, receive_id, msg_type, content, receive_id_type='user_id'):
        # See https://open.feishu.cn/document/server-docs/im-v1/message/create for more infomation
        body = {
            'receive_id': receive_id,
            'msg_type': msg_type,
            'content': lark.JSON.marshal(content)
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.POST) \
            .uri('/open-apis/im/v1/messages') \
            .queries([('receive_id_type', f'{receive_id_type}')]) \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'send message failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def send_interactive_card(self, receive_id, template_id, template_version_name, template_variable, receive_id_type='user_id'):
        content = {
            'type': 'template',
            'data': {
                'template_id': template_id,
                'template_version_name': template_version_name,
                'template_variable': template_variable
            }
        }
        self.send_msg(receive_id, 'interactive', content, receive_id_type)


    def update_interactive_card(self, token, template_id, template_version_name, template_variable):
        body = {
            'token': token,
            'card': {
                'type': 'template',
                'data': {
                    'template_id': template_id,
                    'template_version_name': template_version_name,
                    'template_variable': template_variable
                }
            }
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.POST) \
            .uri('/open-apis/interactive/v1/card/update') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'update card failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['msg']