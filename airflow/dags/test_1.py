from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import time
import random

# Giả lập một hàm xử lý logic
def mock_processing_logic(task_name, **kwargs):
    print(f"--- Đang bắt đầu task: {task_name} ---")
    
    # Giả lập thời gian xử lý từ 2 đến 5 giây
    processing_time = random.randint(2, 5)
    time.sleep(processing_time)
    
    # Giả lập việc đẩy dữ liệu qua XCom
    return f"Kết quả từ {task_name} đã sẵn sàng sau {processing_time}s"

default_args = {
    'owner': 'gemini_mock',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mock_data_pipeline_v1',
    default_args=default_args,
    description='Một DAG giả lập để test UI và Flow',
    schedule_interval='@daily',
    catchup=False,
    tags=['testing', 'mock'],
) as dag:

    # 1. Start Node
    start = EmptyOperator(task_id='start')

    # 2. Mock Extract Task
    extract = PythonOperator(
        task_id='mock_extract',
        python_callable=mock_processing_logic,
        op_kwargs={'task_name': 'Extract_Source_A'}
    )

    # 3. Mock Transform Task (Chạy song song)
    transform_1 = PythonOperator(
        task_id='mock_transform_part_1',
        python_callable=mock_processing_logic,
        op_kwargs={'task_name': 'Transform_Heavy_Logic'}
    )

    transform_2 = PythonOperator(
        task_id='mock_transform_part_2',
        python_callable=mock_processing_logic,
        op_kwargs={'task_name': 'Transform_Light_Logic'}
    )

    # 4. Mock Load Task
    load = PythonOperator(
        task_id='mock_load_to_warehouse',
        python_callable=mock_processing_logic,
        op_kwargs={'task_name': 'Load_Final_Table'}
    )

    # 5. End Node
    end = EmptyOperator(task_id='end')

    # Định nghĩa luồng chạy (Dependency)
    start >> extract >> [transform_1, transform_2] >> load >> end