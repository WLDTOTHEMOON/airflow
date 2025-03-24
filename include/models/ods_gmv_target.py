from sqlalchemy import Column, String, DECIMAL, TIMESTAMP
from include.database.mysql import Base


class OdsGmvTarget(Base):
    __tablename__ = 'ods_gmv_target'
    
    month = Column(String(32), primary_key=True, default='', comment='月份')
    anchor = Column(String(32), primary_key=True, default='', comment='主播')
    target = Column(DECIMAL(32, 4), nullable=True, comment='目标值')
    target_final = Column(DECIMAL(32, 0), nullable=True, comment='最终目标')
    update_at = Column(TIMESTAMP, nullable=True, comment='更新时间')
    