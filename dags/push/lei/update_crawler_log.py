import logging

import pendulum
from airflow.decorators import dag, task
from include.database.mysql import engine
from include.service.message import task_failure_callback

logger = logging.getLogger(__name__)


@dag(
    dag_id='update_crawler_log',
    schedule='0 21,3 * * *',
    default_args=
    {
        'owner': 'Lei Jiangling',
        'start_date': pendulum.datetime(2025, 5, 12),
        'on_failure_callback': task_failure_callback
    },
    tags=['update', 'crawler_log']
)
def update_crawler_log():
    @task
    def insert_log(**kwargs):
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')

        task_id = ['18541124', '3054930335', '3892258892', '146458792']

        if start_time.strftime('%H') == '05':
            # 二创订单
            begin_time = start_time.subtract(days=1).strftime('%Y-%m-%d 04:00:00')
            end_time = start_time.strftime('%Y-%m-%d 04:00:00')
            for i in task_id:
                sql = f'''
                        insert into tmp.tmp_crawler_log(task_id,start_time,end_time,status,update_time,remark)
                            values('{i}','{begin_time}','{end_time}',4,null,null)
                        '''
                engine.connect().execute(sql)

            # 团长佣金收入订单
            begin_time = start_time.subtract(days=32).strftime('%Y-%m-01 00:00:00')
            end_time = start_time.strftime('%Y-%m-%d %H:%M:%S')
            sql = f'''
                    insert into tmp.tmp_crawler_log(task_id,start_time,end_time,status,update_time,remark)
                        values('86765902','{begin_time}','{end_time}',4,null,null)
                        '''
            engine.connect().execute(sql)

            # MCN订单
            sql = f'''
                    insert into tmp.tmp_crawler_log(task_id,start_time,end_time,status,update_time,remark)
                        values('MCN','{begin_time}','{end_time}',4,null,null)
                                '''
            engine.connect().execute(sql)

        if start_time.strftime('%H') == '11':
            # 直播记录
            begin_time = start_time.subtract(days=7).strftime('%Y-%m-%d 00:00:00')
            end_time = start_time.subtract(days=1).strftime('%Y-%m-%d 00:00:00')
            sql = f'''
                    insert into tmp.tmp_crawler_log(task_id,start_time,end_time,status,update_time,remark)
                        values('live_record','{begin_time}','{end_time}',4,null,null)
                        '''
            engine.connect().execute(sql)

    @task
    def month_update_log(**kwargs):
        start_time = kwargs['data_interval_end'].in_tz('Asia/Shanghai')

        task_id = ['3054930335', '3892258892', '146458792']

        begin_time = start_time.subtract(days=11).strftime('%Y-%m-01')
        end_time = start_time.strftime('%Y-%m-01')
        if start_time.strftime('%m') == '10' and start_time.strftime('%H') == '05':
            for i in task_id:
                sql = f'''
                            insert into tmp.tmp_crawler_log(task_id,start_time,end_time,status,update_time,remark)
                            SELECT
                                '{i}' task_id
                                ,DATE_FORMAT(date_col, '%Y-%m-%d 00:00:00') start_time
                                ,DATE_FORMAT(DATE_ADD(date_col, INTERVAL 1 DAY), '%Y-%m-%d 00:00:00') end_time
                                ,0 status
                                ,null update_time
                                ,null remark
                            FROM
                                (
                                    SELECT
                                        DATE_ADD('{begin_time}', INTERVAL (t4.a*1000 + t3.a*100 + t2.a*10 + t1.a) DAY) AS date_col
                                    FROM
                                        (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS t1,
                                        (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS t2,
                                        (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9) AS t3,
                                        (SELECT 0 AS a UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3) AS t4
                                    order by	
                                        date_col asc
                                ) AS dates
                            WHERE date_col < '{end_time}'
                            order by
                                start_time asc
                        '''
                engine.connect().execute(sql)

    insert_log()
    month_update_log()

update_crawler_log()
