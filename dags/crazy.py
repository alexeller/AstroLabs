import random

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from pendulum import datetime
from airflow.operators.python import BranchPythonOperator

def choose_branch() -> str:
    # Returns the task_id of the path to follow
    i = random.randint(1, 10)
    return f"dataset{i}_task"

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2},
    tags=["example"],
)
def mapping_dag():

    branch_task = BranchPythonOperator(
        task_id="branch_task",
        python_callable=choose_branch
    )
    
    sampler_task = EmptyOperator(
        task_id="sampler_task",
        trigger_rule="none_failed_min_one_success" 
    )

    dataset1_task = EmptyOperator(task_id='DataSource1')
    dataset2_task = EmptyOperator(task_id='DataSource2')
    dataset3_task = EmptyOperator(task_id='DataSource3')
    dataset4_task = EmptyOperator(task_id='DataSource4')
    dataset5_task = EmptyOperator(task_id='DataSource5')
    dataset6_task = EmptyOperator(task_id='DataSource6')
    dataset7_task = EmptyOperator(task_id='DataSource7')
    dataset8_task = EmptyOperator(task_id='DataSource8')
    dataset9_task = EmptyOperator(task_id='DataSource9')
    dataset10_task = EmptyOperator(task_id='DataSource10')

    cleaner1_task = EmptyOperator(task_id='Cleaner1')
    cleaner2_task = EmptyOperator(task_id='Cleaner2')
    cleaner3_task = EmptyOperator(task_id='Cleaner3')
    cleaner4_task = EmptyOperator(task_id='Cleaner4')
    cleaner5_task = EmptyOperator(task_id='Cleaner5')

    fraud1_task = EmptyOperator(task_id='FraudDetection1')
    fraud2_task = EmptyOperator(task_id='FraudDetection2')
    fraud3_task = EmptyOperator(task_id='FraudDetection3')
    fraud4_task = EmptyOperator(task_id='FraudDetection4')
    fraud5_task = EmptyOperator(task_id='FraudDetection5')

    report1_task = EmptyOperator(task_id='Report1')
    report2_task = EmptyOperator(task_id='Report2')

    monitor_task = EmptyOperator(task_id='Monitor')
    dataStore_task = EmptyOperator(task_id='DataStore')
    cleanerStore_task = EmptyOperator(task_id='CleanerStore')
    fraudStore_task = EmptyOperator(task_id='FraudStore')
    reportStore_task = EmptyOperator(task_id='ReportStore')

    fork_task = EmptyOperator(task_id='Fork')
    join_task = EmptyOperator(task_id='ReplicationJoin')


    [dataset1_task, dataset2_task] >> cleaner1_task >> fraud1_task
    [dataset3_task, dataset4_task] >> cleaner2_task >> fraud2_task
    [dataset5_task, dataset6_task] >> cleaner3_task >> fraud3_task
    [dataset7_task, dataset8_task] >> cleaner4_task >> fraud4_task
    [dataset9_task, dataset10_task] >> cleaner5_task >> fraud5_task

    [dataset1_task, dataset2_task, dataset3_task, dataset4_task, dataset5_task, dataset6_task, dataset7_task, dataset8_task, dataset9_task, dataset10_task] >> report1_task
    [dataset1_task, dataset2_task, dataset3_task, dataset4_task, dataset5_task, dataset6_task, dataset7_task, dataset8_task, dataset9_task, dataset10_task] >> report2_task
    [report1_task, report2_task] << fraud1_task
    [report1_task, report2_task] << fraud2_task
    [report1_task, report2_task] << fraud3_task
    [report1_task, report2_task] << fraud4_task
    [report1_task, report2_task] << fraud5_task

    [dataset1_task, dataset2_task, dataset3_task, dataset4_task, dataset5_task, dataset6_task, dataset7_task, dataset8_task, dataset9_task, dataset10_task] >> monitor_task
    [cleaner1_task, cleaner2_task, cleaner3_task, cleaner4_task, cleaner5_task] >> monitor_task
    [fraud1_task, fraud2_task, fraud3_task, fraud4_task, fraud5_task] >> monitor_task
    [report1_task, report2_task] >> monitor_task

    [dataset1_task, dataset2_task, dataset3_task, dataset4_task, dataset5_task, dataset6_task, dataset7_task, dataset8_task, dataset9_task, dataset10_task,
    cleaner1_task, cleaner2_task, cleaner3_task, cleaner4_task, cleaner5_task,
    fraud1_task, fraud2_task, fraud3_task, fraud4_task, fraud5_task,report1_task, report2_task] >> monitor_task

    [dataset1_task, dataset2_task, dataset3_task, dataset4_task, dataset5_task, dataset6_task, dataset7_task, dataset8_task, dataset9_task, dataset10_task] >> dataStore_task
    [cleaner1_task, cleaner2_task, cleaner3_task, cleaner4_task, cleaner5_task] >> cleanerStore_task
    [fraud1_task, fraud2_task, fraud3_task, fraud4_task, fraud5_task] >> fraudStore_task
    [report1_task, report2_task] >> reportStore_task

    fork_task >> [dataset1_task, dataset2_task, dataset3_task, dataset4_task, dataset5_task, dataset6_task, dataset7_task, dataset8_task, dataset9_task, dataset10_task] >> join_task
    fork_task >> [cleaner1_task, cleaner2_task, cleaner3_task, cleaner4_task, cleaner5_task] >> join_task
    fork_task >> [fraud1_task, fraud2_task, fraud3_task, fraud4_task, fraud5_task] >> join_task
    fork_task >> [report1_task, report2_task] >> join_task
    branch_task >> [dataset1_task, dataset2_task, dataset3_task, dataset4_task, dataset5_task, dataset6_task, dataset7_task, dataset8_task, dataset9_task, dataset10_task] >> sampler_task

mapping_dag()
