from include.feishu.feishu_client import FeishuClient
from lark_oapi.api.sheets.v3 import *
import lark_oapi as lark
import string


class FeishuSheet(FeishuClient):
    def __init__(self, app_id=None, app_secret=None):
        FeishuClient.__init__(self, app_id, app_secret)

    def list_sheet(self, spreadsheet_token):
        request = BaseRequest.builder().http_method(lark.HttpMethod.GET) \
            .uri(f'/open-apis/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/query') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'list_sheet failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def create_spreadsheet(self, title, folder_token):
        body = {
            'title': title,
            'folder_token': folder_token
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.POST) \
            .uri('/open-apis/sheets/v3/spreadsheets') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'create spreadsheet failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def create_sheet(self, spreadsheet_token, title, index=0):
        body = {
            'requests': [
                {
                    'addSheet': {
                        'properties': {
                            'title': title,
                            'index': index
                        }
                    }
                }
            ]
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.POST) \
            .uri(f'/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/sheets_batch_update') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'create sheet failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def delete_row_col(self, spreadsheet_token, sheet_id, major_dim, start_index, end_index):
        body = {
            'dimension': {
                'sheetId': sheet_id,
                'majorDimension': major_dim,
                'startIndex': start_index,
                'endIndex': end_index
            }
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.DELETE) \
            .uri(f'/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'delete row col failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def add_row_col(self, spreadsheet_token, sheet_id, major_dim, length):
        body = {
            'dimension': {
                'sheetId': sheet_id,
                'majorDimension': major_dim,
                'length': length
            }
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.POST) \
            .uri(f'/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/dimension_range') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'add row col failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def read_cells(self, spreadsheet_token, sheet_id, ranges):
        request = BaseRequest.builder().http_method(lark.HttpMethod.GET) \
            .uri(f'/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_get') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .queries([('ranges', f'{sheet_id}!{ranges}')]) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'read cells failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def write_cells(self, spreadsheet_token, sheet_id, ranges, values):
        body = {
            'valueRanges': [{
                'range': f'{sheet_id}!{ranges}',
                'values': values
            }]
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.POST) \
            .uri(f'/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'write dat failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']

    def style_cells(self, spreadsheet_token, sheet_id, ranges, styles):
        body = {
            'appendStyle': {
                'range': f'{sheet_id}!{ranges}',
                'style': styles
            }
        }
        request = BaseRequest.builder().http_method(lark.HttpMethod.PUT) \
            .uri(f'/open-apis/sheets/v2/spreadsheets/{spreadsheet_token}/style') \
            .token_types({lark.AccessTokenType.TENANT}) \
            .body(body) \
            .build()
        response = self.client.request(request)
        if not response.success():
            lark.logger.error(
                f'style cells failed, '
                f'code: {response.code}, '
                f'msg: {response.msg}, '
                f'log_id: {response.get_log_id()}'
            )
            return response
        return lark.JSON.unmarshal(response.raw.content, dict)['data']
    
    def fetch_dat(self, spreadsheet_token, sheet_id):
        first_row = self.read_cells(spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, ranges='A1:AZ1')
        col_num = next(i for i, x in enumerate(first_row['valueRanges'][0]['values'][0]) if x is None)

        first_col = self.read_cells(spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, ranges='A1:A99999')
        row_num = next(i for i, x in enumerate(first_col['valueRanges'][0]['values']) if x == [None])

        alphabeta = string.ascii_uppercase[:26]
        if col_num <= 26:
            col_num = alphabeta[col_num % 26 - 1]
        else:
            col_num = alphabeta[col_num // 26 - 1] + alphabeta[col_num % 26 - 1]

        dat = self.read_cells(spreadsheet_token=spreadsheet_token, sheet_id=sheet_id, ranges=f'A1:{col_num}{row_num}')

        return dat