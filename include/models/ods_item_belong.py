from sqlalchemy import Column, String, Date, TIMESTAMP
from sqlalchemy.dialects.mysql import VARCHAR
from include.database.mysql import Base
from sqlalchemy.sql import func

class OdsItemBelong(Base):
    __tablename__ = 'ods_item_belong'

    order_date = Column(Date, nullable=True)
    item_id = Column(VARCHAR(255), primary_key=True, default='', comment='商品ID')
    item_title = Column(VARCHAR(255), default='', comment='快手商品名称')
    bd_name = Column(VARCHAR(255), default='', comment='商务姓名')
    bd_name_vice = Column(VARCHAR(255), default='')
    update_at = Column(TIMESTAMP, server_default=func.current_timestamp(), onupdate=func.current_timestamp())

