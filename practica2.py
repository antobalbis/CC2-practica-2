from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'run_as_user': 'antonio',
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG (
    'flujo_practica2',
    default_args=default_args,
    description='Grafo de tareas de la práctica 2',
    schedule_interval=timedelta(days=1),
)

MakeDir = BashOperator(
    task_id = 'create_dir',
    bash_command = 'mkdir -p /tmp/datos',
    dag = dag,
)

DownloadData1 = BashOperator(
    task_id = 'descarga1',
    bash_command = 'wget -O /tmp/datos/humidity.csv.zip https://github.com/manuparra/MaterialCC2020/blob/master/humidity.csv.zip?raw=true',
    dag = dag,
)

DownloadData2 = BashOperator(
    task_id = 'descarga2',
    bash_command = 'wget -O /tmp/datos/temperatura.csv.zip https://github.com/manuparra/MaterialCC2020/blob/master/temperature.csv.zip?raw=true',
    dag = dag,
)

UnzipData1 = BashOperator(
    task_id = 'unzip_data1',
    bash_command = 'unzip -o /tmp/datos/humidity.csv.zip -d /tmp/datos/',
    dag = dag,
)

UnzipData2 = BashOperator(
    task_id = 'unzip_data2',
    bash_command = 'unzip -o /tmp/datos/temperatura.csv.zip -d /tmp/datos/',
    dag = dag,
)

LaunchServices = BashOperator(
    task_id = 'launch_containers',
    bash_command = 'cd ~/Documentos/CC2/CC2-practica2/db && \
    docker-compose up -d',
    dag = dag,
)

JoinDatos = BashOperator(
    task_id = 'pre_process_data',
    bash_command = 'cd ~/Documentos/CC2/CC2-practica2/ && \
    python3 clean_data.py',
    dag = dag,
)



#Execution secuence 1
MakeDir >> DownloadData1 >> DownloadData2 >> UnzipData1 >> UnzipData2 >> LaunchServices >>JoinDatos
