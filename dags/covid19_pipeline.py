from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from datetime import datetime
import pandas as pd
import numpy as np
import io
import os


DAG_ID = "covid19_linelist_pipeline"
staging_conn_id = "staging_postgres_db"
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

# Return XCom safe data by converting datetimes to strings and ensuring all data is JSON serializable
def make_xcom_safe(df):
    df = df.copy()
    for col in df.columns:
        # Convert datetime/date columns to string
        if "datetime" in str(df[col].dtype) or df[col].dtype == "object":
            df[col] = df[col].apply(
                lambda x: x.isoformat() if hasattr(x, "isoformat") else x
            )

    # Replace NaN / NaT with None
    df = df.replace({np.nan: None})

    return df

@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026,1,1),
    schedule="0 * * * *",
    catchup=False,
    tags=["line list","COVID-19"],
)

def covid19_pipeline():

    # -------------------------
    # Run Logging - Start
    # -------------------------
    @task
    def start_run(**context):
        dag_run = context["dag_run"]
        status = dag_run.state
        hook = PostgresHook(postgres_conn_id=dw_conn_id)

        conn = hook.get_conn()
        cur = conn.cursor()

        cur.execute("""
        INSERT INTO etl_run_log
        (dag_id, start_time, status)
        VALUES (%s, NOW(), %s)
        RETURNING run_id
        """,
        (
            DAG_ID,
            status
        ))

        etl_run_id = cur.fetchone()[0]

        conn.commit()

        return etl_run_id
 
    @task
    def extract_data(started, **context):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        try:
            if not started:
                return []
            staging = PostgresHook(postgres_conn_id=staging_conn_id)
            sql = "SELECT stored_path FROM file_uploads WHERE primary_disease ='covid19' AND file_type='disease_line_list' ORDER BY uploaded_at DESC LIMIT 1"
            df = staging.get_pandas_df(sql)
            if df is None or df.empty:
                return []
            else:
                stored_path = df.iloc[0]["stored_path"]
                if not stored_path:
                    return []                
                
                hook = PostgresHook(postgres_conn_id=dw_conn_id)
                conn = hook.get_conn()
                #check if file has been processed before
                file_name = os.path.basename(stored_path)

                sql_check = "SELECT count(*) FROM etl_run_log WHERE file_name = %s and status = 'SUCCESS'"
                check = hook.get_pandas_df(sql_check, parameters= (file_name,))
                if check is None or check.empty:
                    return []
                else:
                    count = check.iloc[0][0]
                    if count > 0:
                        return []
                    else:
                        full_path = os.path.join(BASE_DIR, stored_path)
                        root, file_ext = os.path.splitext(file_name)
                        if file_ext == '.csv':
                            line_list_data = pd.read_csv(full_path)
                        elif file_ext in ['.xlsx', '.xls']:
                            line_list_data = pd.read_excel(full_path)
                        
                        if line_list_data is None or line_list_data.empty:
                            return []
                        else:
                            cur = conn.cursor()
                            cur.execute("""
                                    UPDATE etl_run_log
                                    SET file_name = %s        
                                    WHERE run_id=%s
                                """, (file_name, started))
                            
                            conn.commit()
                            return make_xcom_safe(line_list_data).to_dict("records")
        except Exception as e:
            raise Exception(f"Error in extract data from the staging db: {str(e)}") from e

    @task
    def clean_data(records):
        try:
            if not records:
                return []
            df = pd.DataFrame(records)
            df.replace("null", pd.NA, inplace=True)

            df.columns = df.columns.str.lower()
            # Outcome, IgM Result
            if "labresults_rdt" in df.columns:
                df["labresults_rdt"] = df["labresults_rdt"].str.strip().str.lower()
                df["labresults_rdt"] = df["labresults_rdt"].map({"not done":"not_done"}).fillna(df["labresults_rdt"])
            if "labresults_cul" in df.columns:
                df["labresults_cul"] = df["labresults_cul"].str.strip().str.lower()
                df["labresults_cul"] = df["labresults_cul"].map({"not done":"not_done"}).fillna(df["labresults_cul"])

            if "labresults_rdt" in df.columns and "labresults_cul" in df.columns:
                df["case_classification"] = np.where(
                        (df["labresults_rdt"].str.lower() == "positive") | (df["labresults_cul"].str.lower() == "positive"), "confirmed",
                    np.where(
                        df["labresults_rdt"].isna() & df["labresults_cul"].isna(), "missing",
                        "suspected"
                    )
                )
            
            return df.to_dict("records")
        except Exception as e:
            raise Exception(f"Error in clean_data: {str(e)}") from e

    @task
    def resolve_dimensions(records):
        try:
            if not records:
                print("WARNING: resolve_dimensions received empty list, returning empty")
                return []
            
            df = pd.DataFrame(records)
            dw = PostgresHook(postgres_conn_id=dw_conn_id)

            states = dw.get_pandas_df("SELECT state_id,state_name FROM master_state")
            if states is None or states.empty:
                raise Exception("No states found in master_state table")

            lgas = dw.get_pandas_df("SELECT lga_id,lga_name,state_id FROM master_lga")
            if lgas is None or lgas.empty:
                raise Exception("No LGAs found in master_lga table")

            if "state" in df.columns:
                df["state"] = df["state"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df["state"] = df["state"].map({"fct": "federalcapitalterritory"}).fillna(df["state"])
                states["state_name"] = states["state_name"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df = df.merge(states, left_on="state", right_on="state_name", how="left")
            else:
                raise Exception("Missing 'state' column in data")

            if "lga" in df.columns and "state_id" in df.columns:
                df["lga"] = df["lga"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df["lga"] = df["lga"].map({"yenegoa": "yenagoa","aiyekiregbonyin":"gbonyin","munya":"moya","abujamunicipal":"municipalareacouncil"}).fillna(df["lga"])
                lgas["lga_name"] = lgas["lga_name"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df = df.merge(lgas, left_on=["lga", "state_id"], right_on=["lga_name", "state_id"], how="left")
            else:
                raise Exception("Missing 'lga' or 'state_id' column in data")

            disease = dw.get_pandas_df("SELECT disease_id FROM master_disease WHERE disease_name='covid19'")
            if disease is None or disease.empty:
                raise Exception("No disease found with name 'covid19'")

            df["disease_id"] = disease.iloc[0]["disease_id"]

            for col in ["lga_id", "state_id"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

                   
            return make_xcom_safe(df).to_dict("records")
        except Exception as e:
            print(f"CRITICAL ERROR in resolve_dimensions: {str(e)}")
            raise Exception(f"Error in resolve_dimensions: {str(e)}") from e

    @task
    def load_covid19_data(records): #check where to get the epi_year value from
        try:
            if not records:
                print("WARNING: load_covid19_data received empty list, returning empty DataFrame")
                return []
            
            df = pd.DataFrame(records) 

            for col in ["lga_id", "state_id","epi_week","epi_year"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            required_cols = ["disease_id", "lga_id", "state_id","case_classification","epi_week","epi_year","outcome"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns: {missing_cols}")
            
            fact_df = df[required_cols].copy()
            fact_df.columns = ["disease_id", "lga_id", "state_id", "case_classification","epi_week","epi_year","outcome"]
            
            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()

            cur.execute("""
                        CREATE TEMP TABLE tmp_core_surveillance_fact (
                            disease_id INT,
                            lga_id INT, 
                            state_id INT,
                            case_classification VARCHAR(50),
                            epi_week INT,
                            epi_year INT,
                            outcome VARCHAR(20)
                        ) ON COMMIT DROP
                        """)
                        
            buffer = io.StringIO()
            fact_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.copy_expert("""
            COPY tmp_core_surveillance_fact
            (disease_id,lga_id,state_id,case_classification,epi_week,epi_year,outcome)
            FROM STDIN WITH CSV
            """, buffer)

            cur.execute("""
                INSERT INTO core_surveillance_fact (
                    disease,epi_week,epi_year,state,lga,suspected,confirmed,deaths
                )
                SELECT
                    d.disease_name AS disease,
                    c.epi_week,
                    c.epi_year,
                    s.state_name AS state,
                    l.lga_name AS lga,
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
                FROM tmp_core_surveillance_fact c
                JOIN master_disease d ON c.disease_id = d.disease_id
                LEFT JOIN master_state s ON c.state_id = s.state_id
                LEFT JOIN master_lga l ON c.lga_id = l.lga_id
                WHERE c.onset_date IS NOT NULL
                GROUP BY
                    d.disease_name,
                    c.epi_week,
                    c.epi_year,
                    s.state_name,
                    l.lga_id
                ON CONFLICT (disease, epi_year, epi_week, state, lga)
                DO UPDATE SET
                    suspected = EXCLUDED.suspected,
                    confirmed = EXCLUDED.confirmed,
                    deaths = EXCLUDED.deaths;
                """
            )
            conn.commit()
            cur.close()
            conn.close()
            return True
        except Exception as e:
            raise Exception(f"Error in aggregating covid19 surveillance data from the data warehouse: {str(e)}") from e

        finally:
            if conn:
                try:
                    if cur:
                        cur.close()
                    conn.close()
                except:
                    pass

    # # -------------------------
    # Run Logging - End
    # -------------------------
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def end_run(records, loaded_data, etl_run_id, **context):
        conn = None
        cur = None
        try:
            extracted_count = len(records) if records else 0
            if loaded_data:

                hook = PostgresHook(postgres_conn_id=dw_conn_id)
                conn = hook.get_conn()
                cur = conn.cursor()

                try:
                    cur.execute("""
                        SELECT COUNT(*)
                        FROM etl_task_failures
                        WHERE run_id = %s
                    """, (context.get("run_id", etl_run_id),))
                    
                    result = cur.fetchone()
                    failure_count = result[0] if result else 0
                except Exception as query_error:
                    print(f"Warning: Could not query task failures: {str(query_error)}")
                    failure_count = 0

                status = "FAILED" if failure_count > 0 else "SUCCESS"
                
                cur.execute("""
                    UPDATE etl_run_log
                    SET end_time = NOW(),
                        status=%s,
                        records_extracted = %s          
                    WHERE run_id=%s
                """, (status, extracted_count, etl_run_id))

                conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            raise Exception(f"Error in end_run: {str(e)}") from e
        finally:
            if conn:
                try:
                    if cur:
                        cur.close()
                    conn.close()
                except:
                    pass


    # DAG dependency flow

    run = start_run()

    records = extract_data(run)
    
    cleaned_data = clean_data(records)

    resolved = resolve_dimensions(cleaned_data)

    loaded_data =load_covid19_data(resolved)

    end_run(records, loaded_data, run )


covid19_pipeline()