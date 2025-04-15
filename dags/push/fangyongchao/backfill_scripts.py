from airflow.decorators import task, dag
from airflow.models import Param
import logging
import pendulum


logger = logging.getLogger(__name__)
dag_ids_with_src_tag = [
    'ods_crawler_mcn_order',
    'ods_crawler_leader_commission_income',
    'ods_crawler_recreation',
    'ods_fs_slice_account',
    'ods_fs_gmv_target',
    'ods_fs_links',
    'ods_ks_activity_info',
    'ods_ks_cps_order',
    'ods_ks_leader_order',
    'ods_pf_summary',
    'ods_cps_order_fake',
    'ods_special_allocation',
    'ods_item_category'
]
params = {
    'Dag Id': Param(dag_ids_with_src_tag, type='array', examples=dag_ids_with_src_tag),
    'End Date': Param(pendulum.now().strftime('%Y-%m-%d %H:%M:%S'), type='string')
}

@dag(schedule_interval=None, params=params)
def backfill_scripts():
    @task
    def generate_backfill_cmds(params: dict) -> list[str]:
        dag_ids = params.get('Dag Id', [])
        end_date = params.get('End Date', '')
        cmds = [
            f'airflow dags backfill {dag_id} -e "{end_date}" --reset-dagruns -y'
            for dag_id in dag_ids
        ]
        logger.info(f'生成命令: {cmds}')
        return cmds

    cmds = generate_backfill_cmds()

    from airflow.operators.bash import BashOperator
    rerun_tasks = BashOperator.partial(
        task_id='rerun_src_dag',
        do_xcom_push=False
    ).expand(bash_command=cmds)

backfill_scripts()