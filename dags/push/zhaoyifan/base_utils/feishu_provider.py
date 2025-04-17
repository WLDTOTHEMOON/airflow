from typing import Dict, Any

import pandas as pd
from airflow.models import Variable
from include.feishu.feishu_sheet import FeishuSheet
from include.feishu.feishu_robot import FeishuRobot


class FeishuSheetManager:
    """封装飞书表格操作"""

    def __init__(self):
        config = Variable.get('feishu', deserialize_json=True)
        self.app_id = config.get('app_id')
        self.app_secret = config.get('app_secret')
        self.feishu = FeishuSheet(self.app_id, self.app_secret)

    def get_workbook_params(self, workbook_name: str, folder_token: str):
        """创建工作簿"""
        spreadsheet = self.feishu.create_spreadsheet(workbook_name, folder_token)
        spreadsheet_token = spreadsheet['spreadsheet']['spreadsheet_token']
        workbook_url = spreadsheet['spreadsheet']['url']
        return workbook_url, spreadsheet_token

    def get_sheet_params(self, sheet_name, spreadsheet_token):
        sheet = self.feishu.create_sheet(sheet_name, spreadsheet_token)
        sheet_id = sheet['replies'][0]['addSheet']['properties']['sheetId']
        return sheet_id


class FeishuCardSender:
    """封装卡片发送操作"""

    def __init__(self, robot_url_var: str):
        self.robot_url = Variable.get(robot_url_var)

    def send_card(self, card_data: Dict[str, Any], card_id: str, version: str = '1.0.0'):
        """发送消息卡片"""

        robot = FeishuRobot(robot_url=self.robot_url)
        robot.send_msg_card(
            data=card_data,
            card_id=card_id,
            version_name=version
        )


