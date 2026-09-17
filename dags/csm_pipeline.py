import os
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.task.trigger_rule import TriggerRule
from airflow.models import DagRun
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import pandas as pd
import numpy as np
import io
import json
import re 
from difflib import SequenceMatcher
import tempfile

DAG_ID = "csm_linelist_pipeline"
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

DAG_DIR = os.path.dirname(os.path.abspath(__file__))
COLUMN_MAPPING_FILE = os.path.join(DAG_DIR, "column_mappings.json")


def normalize_column_name(col_name):
    if col_name is None:
        return ""

    col_name = str(col_name).lower().strip()
    col_name = col_name.replace("_", " ")
    col_name = col_name.replace("-", " ")
    col_name = re.sub(r"[^a-z0-9\s]", " ", col_name)
    col_name = re.sub(r"\s+", " ", col_name).strip()

    return col_name


def load_column_mapping():
    if not os.path.exists(COLUMN_MAPPING_FILE):
        raise FileNotFoundError(
            f"Column mapping file not found: {COLUMN_MAPPING_FILE}"
        )

    with open(COLUMN_MAPPING_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def build_synonym_lookup(column_mapping):
    synonym_lookup = {}

    for standard_name, synonyms in column_mapping.items():
        # Also allow the standard name itself to match
        synonym_lookup[normalize_column_name(standard_name)] = standard_name

        for synonym in synonyms:
            synonym_lookup[normalize_column_name(synonym)] = standard_name

    return synonym_lookup


def fuzzy_match_column(normalized_col, synonym_lookup, threshold=85):
    possible_names = list(synonym_lookup.keys())

    if not possible_names:
        return None, 0

    best_name = None
    best_score = 0

    for possible_name in possible_names:
        score = SequenceMatcher(None, normalized_col, possible_name).ratio() * 100

        if score > best_score:
            best_score = score
            best_name = possible_name

    if best_score >= threshold:
        return synonym_lookup[best_name], best_score

    return None, best_score


def apply_column_mapping(df, required_columns=None, fuzzy_threshold=85):
    
    df = df.copy()

    column_mapping = load_column_mapping()
    synonym_lookup = build_synonym_lookup(column_mapping)

    rename_map = {}
    mapped_standard_columns = set()

    for original_col in df.columns:
        normalized_col = normalize_column_name(original_col)

        if normalized_col in synonym_lookup:
            standard_name = synonym_lookup[normalized_col]

            if standard_name not in mapped_standard_columns:
                rename_map[original_col] = standard_name
                mapped_standard_columns.add(standard_name)

            else:
                print(
                    f"WARNING: Column '{original_col}' also maps to '{standard_name}', "
                    "but that standard column was already mapped. Skipping duplicate."
                )

        else:
            standard_name, score = fuzzy_match_column(
                normalized_col,
                synonym_lookup,
                threshold=fuzzy_threshold
            )

            if standard_name and standard_name not in mapped_standard_columns:
                rename_map[original_col] = standard_name
                mapped_standard_columns.add(standard_name)

            else:
                print(
                    f"WARNING: No column mapping found for '{original_col}'. "
                    f"Best fuzzy score was {score}"
                )

    df = df.rename(columns=rename_map)

    return df



@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026,1,1),
    schedule="0 * * * *",
    catchup=False,
    tags=["line list","csm"],
    default_args={
        "on_failure_callback": log_task_failure
    }
)

def csm_pipeline():

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

    # -------------------------
    # Extract
    # -------------------------
    @task
    def extract_data(started):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        try:
            if not started:
                return []
            staging = PostgresHook(postgres_conn_id=staging_conn_id)
            sql = "SELECT stored_path FROM file_uploads WHERE primary_disease ='csm' AND file_type='disease_line_list' ORDER BY uploaded_at DESC LIMIT 1"
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
                        cur = conn.cursor()
                        cur.execute("""
                                UPDATE etl_run_log
                                SET end_time = NOW(),
                                    file_name = %s,
                                    status=%s,
                                    records_extracted = %s,
                                    records_loaded = %s       
                                WHERE run_id=%s
                            """, (file_name,'SKIPPED', 0, 0, started))
                        
                        conn.commit()  
                        raise AirflowSkipException(f"File {file_name} has already been processed")
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
                                    SET file_name = %s,
                                      records_extracted = %s            
                                    WHERE run_id=%s
                                """, (file_name, len(line_list_data),started))
                            
                            conn.commit()
                            tmp_file = tempfile.NamedTemporaryFile(
                                suffix=".parquet",
                                prefix=f"csm_{started}_",
                                delete=False
                            )
                            object_cols = line_list_data.select_dtypes(include=["object"]).columns

                            for col in object_cols:
                                line_list_data[col] = line_list_data[col].astype(str)
                           
                            line_list_data.to_parquet(tmp_file.name, index=False)

                            return {
                                "run_id": started,
                                "path": tmp_file.name,
                                "rows": len(line_list_data)
                            }
        except AirflowSkipException:
            raise
        except Exception as e:
            raise Exception(f"Error in extract data from the staging db: {str(e)}") from e

    @task
    def clean_data(records):
        try:
            if not records:
                return []
            df = pd.read_parquet(records["path"])

            required_columns = ["State", "LGA", "Case classification", "Outcome", "Date of symptom onset (dd/MM/yyyy)"]
            df = apply_column_mapping(df, required_columns=required_columns, fuzzy_threshold=85)

            df.replace("null", pd.NA, inplace=True)
            df.columns = df.columns.str.lower()
            
            if "age" in df.columns:
                df["age"] = np.ceil(pd.to_numeric(df["age"], errors="coerce")).astype("Int64")
                        
            for col in ["onset_date"]:
                if col in df.columns and df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
                    df[col] = pd.to_datetime(df[col], dayfirst=True, format="mixed", errors="coerce").dt.date
                    df[col] = df[col].replace({pd.NaT: None})
            if "outcome" in df.columns:
                df["outcome"] = df["outcome"].str.strip().fillna("missing")

            if "result_positive_negative" in df.columns:
                df["result_positive_negative"] = df["result_positive_negative"].str.strip().str.lower()
                df["result_positive_negative"] = df["result_positive_negative"].map({"awaiting": "pending","NA": "not applicable"}).fillna(df["result_positive_negative"])          

                df["case_classification"] = np.where(df["result_positive_negative"] == "positive", "confirmed",
                                            np.where(df["result_positive_negative"].isna(), "missing", "suspected"))

            df.to_parquet(records["path"], index=False)
            return records
        except Exception as e:
            raise Exception(f"Error in clean_data: {str(e)}") from e

    @task
    def resolve_dimensions(records):
        try:
            if not records:
                print("WARNING: resolve_dimensions received empty list, returning empty")
                return []
            
            df = pd.read_parquet(records["path"])
            dw = PostgresHook(postgres_conn_id=dw_conn_id)

            states = dw.get_pandas_df("SELECT state_id,state_name FROM master_state")
            if states is None or states.empty:
                raise Exception("No states found in master_state table")

            lgas = dw.get_pandas_df("SELECT lga_id,lga_name,state_id FROM master_lga")
            if lgas is None or lgas.empty:
                raise Exception("No LGAs found in master_lga table")

            if "state" in df.columns:
                df["state"] = df["state"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df["state"] = df["state"].map({"fct": "federal capital territory"}).fillna(df["state"])
                states["state_name"] = states["state_name"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df = df.merge(states, left_on="state", right_on="state_name", how="left")
            else:
                raise Exception("Missing 'state' column in data")

            if "lga" in df.columns and "state_id" in df.columns:
                df["lga"] = df["lga"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df["lga"] = df["lga"].map({"kirikasamma": "kirikasama","wamakko":"wamako","nassarawa":"nasarawa","birninmagaji":"birninmagajikiyaw","ileshaeast":"ilesaeast","ileshawest":"ilesawest"}).fillna(df["lga"])
                lgas["lga_name"] = lgas["lga_name"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df = df.merge(lgas, left_on=["lga", "state_id"], right_on=["lga_name", "state_id"], how="left")
            else:
                raise Exception("Missing 'lga' or 'state_id' column in data")

            disease = dw.get_pandas_df("SELECT disease_id FROM master_disease WHERE disease_name='Cerebrospinal meningitis (CSM)'")
            if disease is None or disease.empty:
                raise Exception("No disease found with name 'CSM'")

            # source = dw.get_pandas_df("SELECT system_id FROM master_source_systems WHERE system_name='disease line list'")
            # if source is None or source.empty:
            #     raise Exception("No source system found with name 'disease line list'")

            df["disease_id"] = disease.iloc[0]["disease_id"]
            # df["source_system"] = source.iloc[0]["system_id"]
            for col in ["lga_id", "state_id"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

                   
            df.to_parquet(records["path"], index=False)       
            return records
        except Exception as e:
            print(f"CRITICAL ERROR in resolve_dimensions: {str(e)}")
            raise Exception(f"Error in resolve_dimensions: {str(e)}") from e


    @task
    def load_csm_data(records):
        conn = None
        cur = None
        try:
            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()

            if not records:
                print("WARNING: load_csm_data received empty list, returning empty DataFrame")
                return []
            df = pd.read_parquet(records["path"])

            for col in ["lga_id", "state_id"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            required_cols = ["disease_id", "onset_date", "lga_id", "state_id","case_classification","outcome"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns: {missing_cols}")
            
            fact_df = df[required_cols].copy()
            fact_df.columns = ["disease_id", "onset_date", "lga_id", "state_id", "case_classification", "outcome"]
            
            cur.execute("""
                        CREATE TEMP TABLE tmp_core_surveillance_fact (
                            disease_id INT,
                            onset_date DATE,
                            lga_id INT, 
                            state_id INT,
                            case_classification VARCHAR(50),
                            outcome VARCHAR(50)
                        ) ON COMMIT DROP
                        """)
            
            buffer = io.StringIO()
            fact_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.copy_expert("""
            COPY tmp_core_surveillance_fact
            (disease_id,onset_date,lga_id,state_id,case_classification,outcome)
            FROM STDIN WITH CSV
            """, buffer)

            cur.execute("""
                INSERT INTO core_surveillance_fact (
                    disease,epi_year,epi_week,state,lga,suspected,confirmed,deaths
                )
                SELECT
                    d.disease_name AS disease,
                    EXTRACT(ISOYEAR FROM c.onset_date) AS epi_year,
                    EXTRACT(WEEK FROM c.onset_date) AS epi_week,
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
                    EXTRACT(ISOYEAR FROM c.onset_date),
                    EXTRACT(WEEK FROM c.onset_date),
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
            raise Exception(f"Error in aggregating csm surveillance data from the staging data: {str(e)}") from e
        finally:
            if conn:
                try:
                    if cur:
                        cur.close()
                    conn.close()
                except:
                    pass

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup_temp(records):

        try:
            if records and os.path.exists(records["path"]):
                os.remove(records["path"])
        except Exception as e:
            print(f"Cleanup warning: {e}")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def end_run(loaded_cases, etl_run_id, **context):
        conn = None
        cur = None
        try:
            loaded_count = len(loaded_cases) if loaded_cases else 0

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
                        records_loaded = %s
                    WHERE run_id=%s and status = 'running'
                """, (status, loaded_count, etl_run_id))

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
    cleaned = clean_data(records)
    resolved = resolve_dimensions(cleaned)
    loaded_data =load_csm_data(resolved)
    cleaned = cleanup_temp(resolved)
    loaded_data >> cleaned
    end = end_run(loaded_data, run )
    cleaned >> end

csm_pipeline()