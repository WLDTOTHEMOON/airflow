from lark_oapi import Client


class FeishuClient(Client):
    def __init__(self, app_id, app_secret):
        super().__init__()
        self.client = FeishuClient.builder()\
            .app_id(app_id)\
            .app_secret(app_secret)\
            .build()