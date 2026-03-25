from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from datetime import datetime
import pandas as pd
import numpy as np
import io


DAG_ID = "wer_datamart_pipeline"
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
    tags=["data mart","WER"],
    default_args={
        "on_failure_callback": log_task_failure
    }
)

def wer_datamart_pipeline():

    @task
    def generate_wer_data_mart():
        try:
            warehouse = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = warehouse.get_conn()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO mart_wer (
                    disease,epi_year,epi_week,state,number_of_lgas,suspected, confirmed,deaths
                )
                SELECT
                    d.disease_name AS disease,
                    EXTRACT(ISOYEAR FROM c.onset_date) AS epi_year,
                    EXTRACT(WEEK FROM c.onset_date) AS epi_week,
                    s.state_name AS state,
                    COUNT(DISTINCT c.lga_id) AS number_of_lgas,
                    COUNT(*) AS suspected,
                    SUM(
                        CASE 
                            WHEN LOWER(c.case_classification) = 'confirmed' THEN 1 
                            ELSE 0 
                        END
                    ) AS confirmed,
                    SUM(
                        CASE 
                            WHEN c.outcome = 'Dead' THEN 1 
                            ELSE 0 
                        END
                    ) AS deaths
                FROM core_case_fact c
                JOIN master_disease d ON c.disease_id = d.disease_id
                LEFT JOIN master_state s ON c.state_id = s.state_id
                WHERE c.onset_date IS NOT NULL
                GROUP BY
                    d.disease_name,
                    EXTRACT(ISOYEAR FROM c.onset_date),
                    EXTRACT(WEEK FROM c.onset_date),
                    s.state_name
                ON CONFLICT (disease, epi_year, epi_week, state)
                DO UPDATE SET
                    number_of_lgas = EXCLUDED.number_of_lgas,
                    suspected = EXCLUDED.suspected,
                    confirmed = EXCLUDED.confirmed,
                    deaths = EXCLUDED.deaths;
                """
            )
            conn.commit()
            cur.close()
            conn.close()
        except Exception as e:
            raise Exception(f"Error in generating WER data mart from the data warehouse: {str(e)}") from e
   


    data_mart = generate_wer_data_mart()


wer_datamart_pipeline()