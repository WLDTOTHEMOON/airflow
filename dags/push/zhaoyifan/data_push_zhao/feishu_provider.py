from typing import Dict, Any

import pandas as pd
from airflow.models import Variable
from include.feishu.feishu_sheet import FeishuSheet
import lark_oapi as lark
from include.feishu.feishu_robot import FeishuRobot


class FeishuSheetManager():
    def __init__(self):
        config = Variable.get('feishu', deserialize_json=True)
        self.app_id = config.get('app_id')
        self.app_secret = config.get('app_secret')
        self.feishu = FeishuSheet(self.app_id, self.app_secret)

    def get_workbook_params(self, workbook_name: str, folder_token: str):
        spreadsheet = self.feishu.create_spreadsheet(workbook_name, folder_token)
        print(f"Type of sheet: {type(spreadsheet)}")
        spreadsheet_token = spreadsheet['spreadsheet']['spreadsheet_token']
        workbook_url = spreadsheet['spreadsheet']['url']
        return workbook_url, spreadsheet_token

    def get_sheet_params(self, spreadsheet_token, sheet_name: str,):
        sheet_id = self.feishu.create_sheet(spreadsheet_token, sheet_name)
        print(f"Content of sheet: {sheet_id}")  # 查看原始内容
        print(f"Type of sheet: {type(sheet_id)}")  # 关键调试信息
        sheet_id = sheet_id['replies'][0]['addSheet']['properties']['sheetId']
        return sheet_id



