from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from datetime import datetime
import pandas as pd
import numpy as np
import io


DAG_ID = "hr_datamart_pipeline"
dw_conn_id = "dw_postgres"

# Failure callback
def log_task_failure(context):
    try:
        ti = context["task_instance"]
        dag_run = context["dag_run"]
        hook = PostgresHook(postgres_conn_id=dw_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        
        error_msg = str(context.get("exception", "Unknown error"))
        
        cur.execute("""
            INSERT INTO etl_task_failures (
                dag_id,
                task_id,
                run_id,
                error_message,
                failure_time
            )
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            ti.dag_id,
            ti.task_id,
            dag_run.run_id,
            error_msg
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"CRITICAL: Error logging task failure to DB: {e}")
        import traceback
        traceback.print_exc()

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026,1,1),
    schedule="0 * * * *",
    catchup=False,
    tags=["data mart","human resources"],
    default_args={
        "on_failure_callback": log_task_failure
    }
)

def hr_datamart_pipeline():

    @task
    def generate_hr_data_mart():
        try:
            warehouse = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = warehouse.get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO mart_hr (
                    hr_fact_id, file_no, department,location,sex, leave_status, rank, sgl, years_in_service
                )
                SELECT h.hr_fact_id, h.file_no, d.department_name, h.location, h.sex, h.leave_status, h.rank, h.sgl, h.years_in_service 
                FROM core_hr_fact h
                JOIN public.master_departments d on d.department_id = h.department
                ON CONFLICT (hr_fact_id)
                DO UPDATE SET
                    file_no = EXCLUDED.file_no,
                    department = EXCLUDED.department,
                    location = EXCLUDED.location,
                    sex = EXCLUDED.sex, 
                    leave_status = EXCLUDED.leave_status, 
                    rank = EXCLUDED.rank, 
                    sgl = EXCLUDED.sgl,
                    years_in_service = EXCLUDED.years_in_service;
                """
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Error in generating HR data mart from the data warehouse: {str(e)}") from e
   


    data_mart = generate_hr_data_mart()


hr_datamart_pipeline()