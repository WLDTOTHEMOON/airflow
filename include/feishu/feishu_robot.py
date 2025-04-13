import json
import requests


class FeishuRobot():
    def __init__(self, robot_url):
        self.robot_url = robot_url

    def send_msg_card(self, data, card_id, version_name):
        body = json.dumps({
            'msg_type': 'interactive',
            'card': {
                'type': 'template',
                'data': {
                    'template_id': card_id,
                    'template_version_name': version_name,
                    'template_variable': data
                }
            }
        })
        headers = {'Content-Type': 'application/json'}
        requests.post(url=self.robot_url, data=body, headers=headers)