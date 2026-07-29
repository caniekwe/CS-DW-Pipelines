from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from datetime import datetime
import pandas as pd
import numpy as np
import io
import re
import os
import json
from difflib import SequenceMatcher

# def clean_date_columns(df, date_cols, return_report=True):

#     def parse_excel_serial(x):
#         try:
#             val = float(x)
#             if val > 10000:  # heuristic threshold
#                 return pd.to_datetime(val, origin='1899-12-30', unit='D')
#         except:
#             pass
#         return None

#     def parse_mixed_date(x):
#         if pd.isna(x):
#             return pd.NaT

#         x = str(x).strip()

#         # Remove unwanted characters
#         x = re.sub(r'[^0-9/\-]', '', x)

#         # Try Excel serial
#         serial = parse_excel_serial(x)
#         if serial is not None:
#             return serial

#         # Heuristic for dd/mm vs mm/dd
#         try:
#             parts = re.split(r'[/-]', x)
#             if len(parts) == 3:
#                 p1, p2, _ = parts
#                 if int(p1) > 12:
#                     return pd.to_datetime(x, dayfirst=True, errors='coerce')
#                 elif int(p2) > 12:
#                     return pd.to_datetime(x, dayfirst=False, errors='coerce')
#         except:
#             pass

#         # Fallback
#         return pd.to_datetime(x, format='mixed', errors='coerce')

#     report = {}

#     for col in date_cols:
#         # Force to string to avoid mixed dtype issues
#         df[col] = df[col].astype(str)

#         cleaned_col = f"{col}_clean"

#         df[cleaned_col] = df[col].apply(parse_mixed_date)

#         # Data quality metrics
#         total = len(df)
#         nulls = df[cleaned_col].isna().sum()

#         report[col] = {
#             "total_rows": total,
#             "invalid_dates": int(nulls),
#             "valid_dates": int(total - nulls),
#             "invalid_pct": round(nulls / total * 100, 2)
#         }

#     if return_report:
#         return df, report

#     return df



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

            required_columns = ["State", "LGA", "Case classification", "Outcome", "Date of symptom onset (dd/MM/yyyy)"]
            df = apply_column_mapping(df, required_columns=required_columns, fuzzy_threshold=85)

            df.replace("null", pd.NA, inplace=True)
            
            if "age" in df.columns:
                df["age"] = np.ceil(pd.to_numeric(df["age"], errors="coerce")).astype("Int64")
            

            
            # df["epi_year"] = pd.to_datetime(df["date_of_symptom_onset"], errors="coerce").dt.isocalendar().year
            # df["epi_week_calculated"] =np.where(df['date_of_symptom_onset'].notnull(), pd.to_datetime(df["date_of_symptom_onset"], errors="coerce").dt.isocalendar().week, df['epi_week'].astype("Int64"))
            #df["epi_week_calculated"] = pd.to_datetime(df["date_of_symptom_onset_mm_dd_yyyy"], errors="coerce").dt.isocalendar().week

            # if "gender" in df.columns:
            #     df["gender"] = df["gender"].str.strip().str.lower()
            #     df["gender"] = df["gender"].map({"m": "male", "f": "female"}).fillna("missing")
            # if "vaccination" in df.columns:
            #     df["vaccination"] = df["vaccination"].map({"Unknown": "unknown", "Vaccinated": "vaccinated", "Not Vaccinated": "unvaccinated"}).fillna("missing")
            # if "vaccinated_men5doses" in df.columns:
            #     df["vaccinated_men5doses"] = df["vaccinated_men5doses"].map({"Vaccinated": "vaccinated", "Not Vaccinated": "unvaccinated", "Unvaccinated": "unvaccinated","Not applicable": "not applicable"}).fillna("missing")
            # if "sample_collected" in df.columns:
            #     df["sample_collected"] = df["sample_collected"].map({"Yes": True, "No": False}).astype("boolean").fillna(pd.NA)

                
                #df["outcome_of_case"] = df["outcome_of_case"].map({"alive":"Alive", "dead":"Dead"}).fillna("missing")
            
            # if "admitted_inpatient" in df.columns:
            #     df["admitted_inpatient"] = df["admitted_inpatient"].map({"In": "inpatient", "In patient": "inpatient","inpatient": "inpatient","outpatient": "outpatient","out-patient": "outpatient"}).fillna("missing")
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

            df = make_xcom_safe(df)
            return df.to_dict("records")
        except Exception as e:
            raise Exception(f"Error in clean_data: {str(e)}") from e
    # -------------------------
    # Validation
    # -------------------------
    # @task
    # def validate_data(records, **context):
    #     conn = None
    #     try:
    #         if not records:
    #             return []
            
    #         df = pd.DataFrame(records)
    #         dag_run = context["dag_run"]   
    #         ti = context["task_instance"]     
    #       #  valid_rows = []
    #         failures = []
            
    #         for idx, row in df.iterrows():
    #             try:
    #                 age = pd.to_numeric(row.get("age"), errors="coerce")

    #                 if pd.isna(row.get("state")) and pd.isna(row.get("lga")):
    #                     failures.append((int(idx),"Missing location",dag_run.run_id, ti.dag_id))

    #                 elif pd.isna(age) or age < 0 or age > 120: 
    #                     failures.append((int(idx),"Invalid age",dag_run.run_id, ti.dag_id))

    #                 elif pd.isna(row.get("epid_number")):
    #                     failures.append((int(idx),"Missing Epid number",dag_run.run_id, ti.dag_id))
    #                 else:
    #                     pass
    #                    # valid_rows.append(row)
    #             except Exception as row_error:
    #                 print(f"Row {idx} failed with error: {row_error}")
    #                 failures.append((int(idx),f"Row processing error: {str(row_error)}",dag_run.run_id, ti.dag_id))
    #         if failures:
    #             try:
    #                 hook = PostgresHook(postgres_conn_id=dw_conn_id)
    #                 conn = hook.get_conn()
    #                 cur = conn.cursor()
    #                 cur.executemany("""
    #                 INSERT INTO etl_validation_failures
    #                 (row_number,failure_reason, run_id, dag_id)
    #                 VALUES (%s,%s,%s,%s)
    #                 """, failures)
    #                 conn.commit()
    #             except Exception as db_error:
    #                 print(f"DB Error inserting validation failures: {str(db_error)}")
    #                 if conn:
    #                     conn.rollback()
    #                 raise Exception(f"Error inserting validation failures: {str(db_error)}") from db_error
    #             finally:
    #                 if conn:
    #                     try:
    #                         conn.close()
    #                     except:
    #                         pass

    #         df_valid = make_xcom_safe(df)
            
    #         return df_valid.to_dict("records")
            
            
    #     except Exception as e:
    #         print(f"CRITICAL ERROR in validate_data: {str(e)}")
    #         raise Exception(f"Error in validate_data: {str(e)}") from e


    # # -------------------------
    # # Deduplication
    # # -------------------------
    # @task
    # def deduplicate(records):
    #     try:
    #         if not records:
    #             print("WARNING: deduplicate received empty list, returning empty")
    #             return []
            
    #         df = pd.DataFrame(records)
            
    #         if "epid_number" in df.columns:
    #             df = df.drop_duplicates(subset=["epid_number"])
    #         else:
    #             df = df.drop_duplicates()

    #         return make_xcom_safe(df).to_dict("records")
    #     except Exception as e:
    #         print(f"CRITICAL ERROR in deduplicate: {str(e)}")
    #         raise Exception(f"Error in deduplicate: {str(e)}") from e


    # -------------------------
    # Dimension Resolution
    # -------------------------
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

                   
            return make_xcom_safe(df).to_dict("records")
        except Exception as e:
            print(f"CRITICAL ERROR in resolve_dimensions: {str(e)}")
            raise Exception(f"Error in resolve_dimensions: {str(e)}") from e


    # -------------------------
    # Case Versioning
    # -------------------------
    # @task
    # def apply_case_versioning(records):
    #     try:
    #         if not records:
    #             print("WARNING: apply_case_versioning received empty list, returning empty")
    #             return []
            
    #         df = pd.DataFrame(records)
    #         dw = PostgresHook(postgres_conn_id=dw_conn_id)

    #         try:
    #             existing = dw.get_pandas_df("SELECT epid_number, MAX(case_version) AS version FROM core_case_fact GROUP BY epid_number")
    #         except Exception as query_error:
    #             print(f"Warning: Could not query existing case versions: {str(query_error)}")
    #             existing = None
            
    #         if existing is None or existing.empty:
    #             existing = pd.DataFrame(columns=["epid_number", "version"])
            
    #         if "epid_number" in df.columns:
    #             df = df.merge(existing, on="epid_number", how="left")
            
    #         df["case_version"] = df["version"].fillna(0) + 1
    #         df = df.convert_dtypes()
    #         df = df.where(pd.notnull(df), None)

    #         return make_xcom_safe(df).to_dict("records")
    #     except Exception as e:
    #         print(f"CRITICAL ERROR in apply_case_versioning: {str(e)}")
    #         raise Exception(f"Error in apply_case_versioning: {str(e)}") from e

    # -------------------------
    # Bulk Load
    # -------------------------
    @task
    def load_csm_data(records):
        conn = None
        cur = None
        try:
            if not records:
                print("WARNING: load_csm_data received empty list, returning empty DataFrame")
                return []
            df = pd.DataFrame(records)

            for col in ["lga_id", "state_id"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            required_cols = ["disease_id", "onset_date", "lga_id", "state_id","case_classification","outcome"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns: {missing_cols}")
            
            fact_df = df[required_cols].copy()
            fact_df.columns = ["disease_id", "onset_date", "lga_id", "state_id", "case_classification", "outcome"]
            
            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()

            cur.execute("""
            CREATE TEMP TABLE tmp_core_case_fact (
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

#     @task
#     def load_csm_extension_table(records, inserted_cases_df):
#         conn = None
#         cur = None
#         try:
#             if not inserted_cases_df:
#                 return 0
            
#             df = pd.DataFrame(records)
#             inserted_cases = pd.DataFrame(inserted_cases_df)
            
#             if "epid_number" not in df.columns or "epid_number" not in inserted_cases.columns:
#                 raise Exception("Missing epid_number column for merge")
            
#             df = df.merge(inserted_cases, on="epid_number", how="inner")
            
#             if df.empty:
#                 raise Exception("No matching records after merge with inserted cases")
            
#             required_lassa_cols = [
#                 "case_fact_id", "ward", "vaccination", "vaccinatedmen5doses_0_1_2_3","samplecollected_yes_no","result_positive_negative", "date_of_report_dd_mm_yyyy"
#             ]
            
#             available_cols = [col for col in required_lassa_cols if col in df.columns]
#             if not available_cols:
#                 raise Exception(f"None of the required columns found in data")
            
#             lassa_df = df[available_cols].copy()
            
#             hook = PostgresHook(postgres_conn_id=dw_conn_id)
#             conn = hook.get_conn()
#             cur = conn.cursor()

#             buffer = io.StringIO()
#             lassa_df.to_csv(buffer, index=False, header=False)
#             buffer.seek(0)

#             cur.execute("""
#             CREATE TEMP TABLE tmp_ext_csm_case (
#                 case_id INT,                
#                 ward VARCHAR(50),
#                 vaccination_status VARCHAR(50),
#                 vaccinated_men5doses VARCHAR(50),
#                 sample_collected boolean,    
#                 result_interpretation VARCHAR(50),                    
#                 date_of_report date                      
#             ) ON COMMIT DROP
#             """)

#                 # facility VARCHAR(100),
#                 # case_classification VARCHAR(50),                
#                 # first_symptom VARCHAR(50),                
#                 # date_specimen_collected date,
#                 # date_specimen_received_at_lab date,
#                 # date_specimen_tested date,
#                 # sodc VARCHAR(50),
#                 # hpd VARCHAR(50),
#                 # lyta VARCHAR(50),
#                 # species VARCHAR(50),
#                 # nma VARCHAR(50),
#                 # nmb VARCHAR(50),
#                 # nmc VARCHAR(50),
#                 # nmw VARCHAR(50),
#                 # nmx VARCHAR(50),
#                 # nmy VARCHAR(50),
#                 # hib VARCHAR(50),
#                 # spn VARCHAR(50),
#                 # final_intepretation VARCHAR(50),
            
#             cur.copy_expert("""
#             COPY tmp_ext_csm_case (case_id, ward,vaccination_status,vaccinated_men5doses,
#                 sample_collected,result_interpretation, date_of_report)
#             FROM STDIN WITH CSV
#             """, buffer)
# # facility, case_classification,first_symptom,date_specimen_collected,date_specimen_received_at_lab,date_specimen_tested,sodc,hpd,
#                 #lyta,species,nma,nmb,nmc,nmw,nmx,nmy,hib,spn,final_intepretation,
#             cur.execute("""
#             INSERT INTO ext_csm_case (case_id, ward,vaccination_status,vaccinated_men5doses,
#                 sample_collected,result_interpretation,date_of_report)
#             SELECT case_id, ward,vaccination_status,vaccinated_men5doses,
#                 sample_collected,result_interpretation, date_of_report
#             FROM tmp_ext_csm_case
#             ON CONFLICT (case_id) DO UPDATE SET 
#                         date_of_report = EXCLUDED.date_of_report,
#                         ward = EXCLUDED.ward,
#                         vaccination_status = EXCLUDED.vaccination_status,
#                         vaccinated_men5doses = EXCLUDED.vaccinated_men5doses,
#                         sample_collected = EXCLUDED.sample_collected,
#                         result_interpretation = EXCLUDED.result_interpretation
#             """)
            
#             conn.commit()
#             return len(inserted_cases)
#         except Exception as e:
#             if conn:
#                 conn.rollback()
#             raise Exception(f"Error in load_csm_extension_table: {str(e)}") from e
#         finally:
#             if conn:
#                 try:
#                     if cur:
#                         cur.close()
#                     conn.close()
#                 except:
#                     pass

#     # -------------------------
    # Run Logging - End
    # -------------------------
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def end_run(records, loaded_cases, etl_run_id, **context):
        conn = None
        cur = None
        try:
            extracted_count = len(records) if records else 0
            if loaded_cases:

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

    cleaned = clean_data(records)

    # validated = validate_data(cleaned)

    # deduped = deduplicate(validated)

    resolved = resolve_dimensions(cleaned)

    # versioned = apply_case_versioning(resolved)

    # case_fact = load_core_fact_table(versioned)

    loaded_data =load_csm_data(resolved)

    end_run(records, loaded_data, run )


csm_pipeline()