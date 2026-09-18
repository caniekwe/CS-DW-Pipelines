import os
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.task.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import pandas as pd
import numpy as np
import io
import json
import re 
from difflib import SequenceMatcher
import tempfile


DAG_ID = "afp_linelist_pipeline"
staging_conn_id = "staging_postgres_db"
dw_conn_id = "dw_postgres"

# Failure callback
def log_task_failure(context):
    try:
        ti = context["task_instance"]
        hook = PostgresHook(postgres_conn_id=dw_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        error_msg = str(context.get("exception", "Unknown error"))


        etl_run_id = ti.xcom_pull(task_ids="start_run", key="return_value")

        cur.execute("""
            INSERT INTO etl_task_failures (
                dag_id,
                task_id,
                etl_run_id,
                error_message,
                failure_time
            )
            VALUES (%s, %s, %s, %s, NOW())
        """, (
            ti.dag_id,
            ti.task_id,
            etl_run_id,
            error_msg
        ))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        print(f"CRITICAL: Error logging task failure to DB: {e}")
        import traceback
        traceback.print_exc()


def mark_run_skipped(etl_run_id):
    hook = PostgresHook(postgres_conn_id=dw_conn_id)

    with hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE etl_run_log
                SET end_time = NOW(),
                    status = 'SKIPPED'
                WHERE run_id = %s
                  AND status = 'RUNNING'
                """,
                (etl_run_id,),
            )

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


def apply_column_mapping(df, fuzzy_threshold=85):
    
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
    tags=["line list","afp"],
    default_args={
        "on_failure_callback": log_task_failure
    }
)

def afp_pipeline():

    # -------------------------
    # Run Logging - Start
    # -------------------------
    @task
    def start_run(**context):
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
            "RUNNING"
        ))

        etl_run_id = cur.fetchone()[0]

        conn.commit()

        return etl_run_id
 
    @task
    def extract_data(started, **context):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        try:
            if not started:
                raise ValueError(f"Pipeline did not start properly, no etl_run_id found")
            staging = PostgresHook(postgres_conn_id=staging_conn_id)
            sql = "SELECT stored_path, id FROM file_uploads WHERE primary_disease ='afp' AND file_type='disease_line_list' ORDER BY uploaded_at DESC LIMIT 1"
            df = staging.get_pandas_df(sql)
            if df is None or df.empty:
                mark_run_skipped(started)
                raise AirflowSkipException(f"There is no upload record for AFP found in the staging database")
            else:
                stored_path = df.iloc[0]["stored_path"]
                file_id = int(df.iloc[0]["id"])
                if not stored_path:
                    raise ValueError(f"No file path recorded for this file")
                if not file_id:
                    raise ValueError(f"File ID is not recorded for this file")
                hook = PostgresHook(postgres_conn_id=dw_conn_id)
                conn = hook.get_conn()
                #check if file has been processed before
                file_name = os.path.basename(stored_path)

                sql_check = "SELECT count(*) FROM etl_run_log WHERE file_name = %s and file_upload_id = %s and status = 'SUCCESS'"
                check = hook.get_pandas_df(sql_check, parameters= (file_name, file_id))

                if check is not None and not check.empty:
                    count = check.iloc[0][0]
                    if count > 0:
                        cur = conn.cursor()
                        cur.execute("""
                                UPDATE etl_run_log
                                SET end_time = NOW(),
                                    file_name = %s,
                                    file_upload_id = %s,
                                    status=%s,
                                    records_extracted = %s
                                WHERE run_id=%s
                            """, (file_name, file_id, 'SKIPPED', 0, started))

                        conn.commit()
                        raise AirflowSkipException(f"File {file_name} has already been processed")
                    else:
                        full_path = os.path.join(BASE_DIR, stored_path)
                        file_ext = os.path.splitext(file_name)[1].lower()
                        if file_ext == '.csv':
                            line_list_data = pd.read_csv(full_path)
                        elif file_ext in ['.xlsx', '.xls']:
                            line_list_data = pd.read_excel(full_path)
                        else:
                            raise ValueError(f"Unsupported file extension '{file_ext}' for {file_name}")

                        if line_list_data is None or line_list_data.empty:
                            raise ValueError(f"{file_name} is empty or has no data")
                        else:
                            cur = conn.cursor()
                            cur.execute("""
                                    UPDATE etl_run_log
                                    SET file_name = %s,
                                        file_upload_id = %s,
                                        records_extracted = %s
                                    WHERE run_id=%s
                                """, (file_name, file_id, len(line_list_data), started))
                            
                            conn.commit()
                            tmp_file = tempfile.NamedTemporaryFile(
                                suffix=".parquet",
                                prefix=f"afp_{started}_",
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
                raise ValueError("clean_data received empty list")
            df = pd.read_parquet(records["path"])

            df = apply_column_mapping(df, fuzzy_threshold=85)

            df.replace("null", pd.NA, inplace=True)

            df.columns = df.columns.str.lower()
            
            if "case_classification" in df.columns:
                df["case_classification"] = df["case_classification"].str.strip().str.lower()

            if "outcome" in df.columns:
                df["outcome"] = df["outcome"].str.strip().str.lower()

            if "onset_date" in df.columns:
                df["onset_date"] = pd.to_datetime(df["onset_date"], errors="coerce")
            
            df.to_parquet(records["path"], index=False)
            return records
        except Exception as e:
            raise Exception(f"Error in clean_data: {str(e)}") from e

    @task
    def resolve_dimensions(records):
        try:
            if not records:
                print("WARNING: resolve_dimensions received empty list, returning empty")
                raise ValueError("resolve_dimensions received empty list")
            
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

            disease = dw.get_pandas_df("SELECT disease_id FROM master_disease WHERE disease_name='Acute flaccid paralysis (AFP)'")
            if disease is None or disease.empty:
                raise Exception("No disease found with name 'Acute flaccid paralysis (AFP)'")

            df["disease_id"] = disease.iloc[0]["disease_id"]

            for col in ["lga_id", "state_id"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

                   
            df.to_parquet(records["path"], index=False)       
            return records
        except Exception as e:
            print(f"CRITICAL ERROR in resolve_dimensions: {str(e)}")
            raise Exception(f"Error in resolve_dimensions: {str(e)}") from e

    @task
    def load_afp_data(records):
        conn = None
        cur = None
        try:
            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()
            if not records:
                print("WARNING: load_afp_data received empty list, returning empty DataFrame")
                raise ValueError("load_afp_data received empty list, cannot proceed with loading data")
            
            df = pd.read_parquet(records["path"])

            for col in ["lga_id", "state_id","epi_week","epi_year"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            required_cols = ["disease_id", "lga_id", "state_id","case_classification","onset_date", "outcome"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns: {missing_cols}")
            
            fact_df = df[required_cols].copy()
            fact_df.columns = ["disease_id", "lga_id", "state_id", "case_classification","onset_date", "outcome"]
            
            cur.execute("""
                        CREATE TEMP TABLE tmp_core_surveillance_fact (
                            disease_id INT,
                            lga_id INT, 
                            state_id INT,
                            case_classification VARCHAR(50),
                            onset_date DATE,
                            outcome VARCHAR(50)
                        ) ON COMMIT DROP
                        """)
                        
            buffer = io.StringIO()
            fact_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.copy_expert("""
            COPY tmp_core_surveillance_fact
            (disease_id,lga_id,state_id,case_classification,onset_date, outcome)
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
                            WHEN c.outcome = 'deceased' THEN 1 
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
            return True
        except Exception as e:
            raise Exception(f"Error in aggregating afp surveillance data from the data warehouse: {str(e)}") from e

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
    def end_run(etl_run_id, **context):
        conn = None
        cur = None
        try:
            #loaded_count = len(loaded_cases) if loaded_cases else 0

            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()


            cur.execute("""
                SELECT COUNT(*)
                FROM etl_task_failures
                WHERE etl_run_id = %s
            """, ( etl_run_id,))

            result = cur.fetchone()
            failure_count = result[0] if result else 0


            status = "FAILED" if failure_count > 0 else "SUCCESS"

            cur.execute("""
                UPDATE etl_run_log
                SET end_time = NOW(),
                    status=%s
                WHERE run_id=%s and status = 'RUNNING'
            """, (status, etl_run_id))

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
    loaded_data =load_afp_data(resolved)
    end = end_run(run)
    cleanup = cleanup_temp(records)

    loaded_data >> end >> cleanup

afp_pipeline()