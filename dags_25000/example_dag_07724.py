from airflow.models.dag import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.utils.helpers import chain
with DAG('example_dag_07724_1',
         max_active_runs=3,
         start_date=days_ago(2),
         schedule_interval="@hourly",
         concurrency=16) as dag_1:
    tasks = [
        BashOperator(
            task_id="__".join(["tasks", "{}_of_{}".format(i, '10')]), bash_command='echo test'
        )
        for i in range(1, 10 + 1)
    ]
    for i in range(1, len(tasks)):
        tasks[i].set_upstream(tasks[(i - 1) // 2])
