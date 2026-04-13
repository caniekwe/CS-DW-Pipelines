from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from datetime import datetime
import pandas as pd
import numpy as np
import io


DAG_ID = "csm_sitrep_datamart_pipeline"
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
    tags=["data mart","sitrep", "CSM"],
    default_args={
        "on_failure_callback": log_task_failure
    }
)

def sitrep_datamart_pipeline():

    @task
    def generate_sitrep_data_mart():
        try:
            warehouse = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = warehouse.get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO mart_sitrep (
                    record_id, disease, epi_year, epi_week, state, lga, age_years, sex, result, outcome
                )
                SELECT
                    c.case_fact_id,
                    d.disease_name,
                    c.epi_year,
                    c.epi_week,
                    s.state_name,
                    l.lga_name,
                    c.age,
                    c.sex,
                    e.result_interpretation,
                    c.outcome                       
                          
                FROM core_case_fact c
                JOIN master_disease d ON c.disease_id = d.disease_id
                LEFT JOIN master_state s ON c.state_id = s.state_id
                LEFT JOIN master_lga l ON c.lga_id = l.lga_id
                LEFT JOIN ext_csm_case e ON c.case_fact_id = e.case_id
                where d.disease_name = 'Cerebrospinal meningitis (CSM)'
                ON CONFLICT (record_id)
                DO UPDATE SET
                    age_years = EXCLUDED.age_years,
                    sex = EXCLUDED.sex,
                    result = EXCLUDED.result,
                    outcome = EXCLUDED.outcome
                """
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Error in generating cholera sitrep data mart from the data warehouse: {str(e)}") from e
   


    data_mart = generate_sitrep_data_mart()




sitrep_datamart_pipeline()