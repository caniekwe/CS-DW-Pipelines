from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from datetime import datetime
import pandas as pd
import numpy as np
import io
import re



def clean_date_columns(df, date_cols, return_report=True):

    def parse_excel_serial(x):
        try:
            val = float(x)
            if val > 10000:  # heuristic threshold
                return pd.to_datetime(val, origin='1899-12-30', unit='D')
        except:
            pass
        return None

    def parse_mixed_date(x):
        if pd.isna(x):
            return pd.NaT

        x = str(x).strip()

        # Remove unwanted characters
        x = re.sub(r'[^0-9/\-]', '', x)

        # Try Excel serial
        serial = parse_excel_serial(x)
        if serial is not None:
            return serial

        # Heuristic for dd/mm vs mm/dd
        try:
            parts = re.split(r'[/-]', x)
            if len(parts) == 3:
                p1, p2, _ = parts
                if int(p1) > 12:
                    return pd.to_datetime(x, dayfirst=True, errors='coerce')
                elif int(p2) > 12:
                    return pd.to_datetime(x, dayfirst=False, errors='coerce')
        except:
            pass

        # Fallback
        return pd.to_datetime(x, format='mixed', errors='coerce')

    report = {}

    for col in date_cols:
        # Force to string to avoid mixed dtype issues
        df[col] = df[col].astype(str)

        cleaned_col = f"{col}_clean"

        df[cleaned_col] = df[col].apply(parse_mixed_date)

        # Data quality metrics
        total = len(df)
        nulls = df[cleaned_col].isna().sum()

        report[col] = {
            "total_rows": total,
            "invalid_dates": int(nulls),
            "valid_dates": int(total - nulls),
            "invalid_pct": round(nulls / total * 100, 2)
        }

    if return_report:
        return df, report

    return df



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
        try:
            if not started:
                return []
            staging = PostgresHook(postgres_conn_id=staging_conn_id)
            sql = "SELECT * FROM csm_cases"
            df = staging.get_pandas_df(sql)
            if df is None or df.empty:
                return []
            return df.to_dict("records")
        except Exception as e:
            raise Exception(f"Error in extract data from the staging db: {str(e)}") from e

    @task
    def clean_data(records):
        try:
            if not records:
                return []
            df = pd.DataFrame(records)
            df.replace("null", pd.NA, inplace=True)
            
            if "age" in df.columns:
                df["age"] = np.ceil(pd.to_numeric(df["age"], errors="coerce")).astype("Int64")
            
            for col in ["date_of_symptom_onset_mm_dd_yyyy", "date_of_report_dd_mm_yyyy"]:
                if col in df.columns and df[col].dtype == 'object':
                    df[col] = df[col].astype(str)
                    df[col] = pd.to_datetime(df[col], dayfirst=True, format="mixed", errors="coerce").dt.date
                    df[col] = df[col].replace({pd.NaT: None})
            
            df["epi_year"] = pd.to_datetime(df["date_of_symptom_onset_mm_dd_yyyy"], errors="coerce").dt.isocalendar().year
            df["epi_week_calculated"] =np.where(df['date_of_symptom_onset_mm_dd_yyyy'].notnull(), pd.to_datetime(df["date_of_symptom_onset_mm_dd_yyyy"], errors="coerce").dt.isocalendar().week, df['epi_week'].astype("Int64"))
            #df["epi_week_calculated"] = pd.to_datetime(df["date_of_symptom_onset_mm_dd_yyyy"], errors="coerce").dt.isocalendar().week

            if "gender" in df.columns:
                df["gender"] = df["gender"].str.strip().str.lower()
                df["gender"] = df["gender"].map({"m": "male", "f": "female"}).fillna("missing")
            if "vaccination" in df.columns:
                df["vaccination"] = df["vaccination"].map({"Unknown": "unknown", "Vaccinated": "vaccinated", "Not Vaccinated": "unvaccinated"}).fillna("missing")
            if "vaccinated_men5doses" in df.columns:
                df["vaccinated_men5doses"] = df["vaccinated_men5doses"].map({"Vaccinated": "vaccinated", "Not Vaccinated": "unvaccinated", "Unvaccinated": "unvaccinated","Not applicable": "not applicable"}).fillna("missing")
            if "sample_collected" in df.columns:
                df["sample_collected"] = df["sample_collected"].map({"Yes": True, "No": False}).astype("boolean").fillna(pd.NA)
            if "outcome_of_case" in df.columns:
                df["outcome_of_case"] = df["outcome_of_case"].str.strip().str.lower().fillna("missing")
                
                #df["outcome_of_case"] = df["outcome_of_case"].map({"alive":"Alive", "dead":"Dead"}).fillna("missing")
            
            if "admitted_inpatient" in df.columns:
                df["admitted_inpatient"] = df["admitted_inpatient"].map({"In": "inpatient", "In patient": "inpatient","inpatient": "inpatient","outpatient": "outpatient","out-patient": "outpatient"}).fillna("missing")
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
    @task
    def validate_data(records, **context):
        conn = None
        try:
            if not records:
                return []
            
            df = pd.DataFrame(records)
            dag_run = context["dag_run"]   
            ti = context["task_instance"]     
          #  valid_rows = []
            failures = []
            
            for idx, row in df.iterrows():
                try:
                    age = pd.to_numeric(row.get("age"), errors="coerce")

                    if pd.isna(row.get("state")) and pd.isna(row.get("lga")):
                        failures.append((int(idx),"Missing location",dag_run.run_id, ti.dag_id))

                    elif pd.isna(age) or age < 0 or age > 120: 
                        failures.append((int(idx),"Invalid age",dag_run.run_id, ti.dag_id))

                    elif pd.isna(row.get("epid_number")):
                        failures.append((int(idx),"Missing Epid number",dag_run.run_id, ti.dag_id))
                    else:
                        pass
                       # valid_rows.append(row)
                except Exception as row_error:
                    print(f"Row {idx} failed with error: {row_error}")
                    failures.append((int(idx),f"Row processing error: {str(row_error)}",dag_run.run_id, ti.dag_id))
            if failures:
                try:
                    hook = PostgresHook(postgres_conn_id=dw_conn_id)
                    conn = hook.get_conn()
                    cur = conn.cursor()
                    cur.executemany("""
                    INSERT INTO etl_validation_failures
                    (row_number,failure_reason, run_id, dag_id)
                    VALUES (%s,%s,%s,%s)
                    """, failures)
                    conn.commit()
                except Exception as db_error:
                    print(f"DB Error inserting validation failures: {str(db_error)}")
                    if conn:
                        conn.rollback()
                    raise Exception(f"Error inserting validation failures: {str(db_error)}") from db_error
                finally:
                    if conn:
                        try:
                            conn.close()
                        except:
                            pass

            df_valid = make_xcom_safe(df)
            
            return df_valid.to_dict("records")
            
            
        except Exception as e:
            print(f"CRITICAL ERROR in validate_data: {str(e)}")
            raise Exception(f"Error in validate_data: {str(e)}") from e


    # -------------------------
    # Deduplication
    # -------------------------
    @task
    def deduplicate(records):
        try:
            if not records:
                print("WARNING: deduplicate received empty list, returning empty")
                return []
            
            df = pd.DataFrame(records)
            
            if "epid_number" in df.columns:
                df = df.drop_duplicates(subset=["epid_number"])
            else:
                df = df.drop_duplicates()

            return make_xcom_safe(df).to_dict("records")
        except Exception as e:
            print(f"CRITICAL ERROR in deduplicate: {str(e)}")
            raise Exception(f"Error in deduplicate: {str(e)}") from e


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

            source = dw.get_pandas_df("SELECT system_id FROM master_source_systems WHERE system_name='disease line list'")
            if source is None or source.empty:
                raise Exception("No source system found with name 'disease line list'")

            df["disease_id"] = disease.iloc[0]["disease_id"]
            df["source_system"] = source.iloc[0]["system_id"]
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
    @task
    def apply_case_versioning(records):
        try:
            if not records:
                print("WARNING: apply_case_versioning received empty list, returning empty")
                return []
            
            df = pd.DataFrame(records)
            dw = PostgresHook(postgres_conn_id=dw_conn_id)

            try:
                existing = dw.get_pandas_df("SELECT epid_number, MAX(case_version) AS version FROM core_case_fact GROUP BY epid_number")
            except Exception as query_error:
                print(f"Warning: Could not query existing case versions: {str(query_error)}")
                existing = None
            
            if existing is None or existing.empty:
                existing = pd.DataFrame(columns=["epid_number", "version"])
            
            if "epid_number" in df.columns:
                df = df.merge(existing, on="epid_number", how="left")
            
            df["case_version"] = df["version"].fillna(0) + 1
            df = df.convert_dtypes()
            df = df.where(pd.notnull(df), None)

            return make_xcom_safe(df).to_dict("records")
        except Exception as e:
            print(f"CRITICAL ERROR in apply_case_versioning: {str(e)}")
            raise Exception(f"Error in apply_case_versioning: {str(e)}") from e

    # -------------------------
    # Bulk Load
    # -------------------------
    @task
    def load_core_fact_table(records):
        conn = None
        cur = None
        try:
            if not records:
                print("WARNING: load_core_fact_table received empty list, returning empty DataFrame")
                return pd.DataFrame(columns=["case_fact_id", "epid_number"])
            
            df = pd.DataFrame(records)

            for col in ["lga_id", "state_id","age","epi_week_calculated", "epi_year"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            required_cols = ["epid_number", "disease_id", "date_of_symptom_onset_mm_dd_yyyy", "gender", "age", "lga_id", "state_id","case_classification","admitted_as_inpatient", "source_system", "case_version", "epi_week_calculated","epi_year","outcome_of_case"]
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns: {missing_cols}")
            
            fact_df = df[required_cols].copy()
            fact_df.columns = ["epid_number", "disease_id", "onset_date", "sex", "age", "lga_id", "state_id", "case_classification", "hospitalisation_status", "source_system", "case_version", "epi_week_calculated","epi_year","outcome_of_case"]
            
            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()

            cur.execute("""
            CREATE TEMP TABLE tmp_core_case_fact (
                epid_number VARCHAR(50), disease_id INT, onset_date DATE, sex VARCHAR(10),
                age INT, lga_id INT, state_id INT, case_classification VARCHAR(50), hospitalisation_status VARCHAR(50), source_system INT, case_version INT,  epi_week INT, epi_year INT, outcome VARCHAR(50)
            ) ON COMMIT DROP
            """)
            
            buffer = io.StringIO()
            fact_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.copy_expert("""
            COPY tmp_core_case_fact (epid_number,disease_id,onset_date,sex,age,lga_id, state_id, case_classification,hospitalisation_status, source_system,case_version,epi_week, epi_year, outcome)
            FROM STDIN WITH CSV
            """, buffer)

            cur.execute("""
            INSERT INTO core_case_fact (epid_number,disease_id,onset_date,sex,age,lga_id,state_id,case_classification,hospitalisation_status,source_system,case_version,epi_week, epi_year, outcome)
            SELECT epid_number,disease_id,onset_date,sex,age,lga_id,state_id,case_classification,hospitalisation_status, source_system,case_version,epi_week, epi_year, outcome
            FROM tmp_core_case_fact
            ON CONFLICT (epid_number) DO UPDATE SET 
                        disease_id = EXCLUDED.disease_id, 
                        onset_date = EXCLUDED.onset_date, 
                        sex = EXCLUDED.sex, 
                        age = EXCLUDED.age, 
                        lga_id = EXCLUDED.lga_id, 
                        state_id = EXCLUDED.state_id, 
                        case_classification = EXCLUDED.case_classification,
                        hospitalisation_status = EXCLUDED.hospitalisation_status, 
                        source_system = EXCLUDED.source_system, 
                        case_version = EXCLUDED.case_version,
                        epi_week = EXCLUDED.epi_week, 
                        epi_year = EXCLUDED.epi_year,
                        outcome = EXCLUDED.outcome           
            RETURNING case_fact_id, epid_number
            """)

            inserted_rows = cur.fetchall()
            if not inserted_rows:
                raise Exception("No rows inserted into core_case_fact table")
            
            conn.commit()
            inserted_df = make_xcom_safe(pd.DataFrame(inserted_rows, columns=["case_fact_id", "epid_number"]))
            return inserted_df.to_dict("records")
        except Exception as e:
            print(f"CRITICAL ERROR in load_core_fact_table: {str(e)}")
            if conn:
                conn.rollback()
            raise Exception(f"Error in load_core_fact_table: {str(e)}") from e
        finally:
            if conn:
                try:
                    if cur:
                        cur.close()
                    conn.close()
                except:
                    pass

    @task
    def load_csm_extension_table(records, inserted_cases_df):
        conn = None
        cur = None
        try:
            if not inserted_cases_df:
                return 0
            
            df = pd.DataFrame(records)
            inserted_cases = pd.DataFrame(inserted_cases_df)
            
            if "epid_number" not in df.columns or "epid_number" not in inserted_cases.columns:
                raise Exception("Missing epid_number column for merge")
            
            df = df.merge(inserted_cases, on="epid_number", how="inner")
            
            if df.empty:
                raise Exception("No matching records after merge with inserted cases")
            
            required_lassa_cols = [
                "case_fact_id", "ward", "vaccination", "vaccinatedmen5doses_0_1_2_3","samplecollected_yes_no","result_positive_negative", "date_of_report_dd_mm_yyyy"
            ]
            
            available_cols = [col for col in required_lassa_cols if col in df.columns]
            if not available_cols:
                raise Exception(f"None of the required columns found in data")
            
            lassa_df = df[available_cols].copy()
            
            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()

            buffer = io.StringIO()
            lassa_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.execute("""
            CREATE TEMP TABLE tmp_ext_csm_case (
                case_id INT,                
                ward VARCHAR(50),
                vaccination_status VARCHAR(50),
                vaccinated_men5doses VARCHAR(50),
                sample_collected boolean,    
                result_interpretation VARCHAR(50),                    
                date_of_report date                      
            ) ON COMMIT DROP
            """)

                # facility VARCHAR(100),
                # case_classification VARCHAR(50),                
                # first_symptom VARCHAR(50),                
                # date_specimen_collected date,
                # date_specimen_received_at_lab date,
                # date_specimen_tested date,
                # sodc VARCHAR(50),
                # hpd VARCHAR(50),
                # lyta VARCHAR(50),
                # species VARCHAR(50),
                # nma VARCHAR(50),
                # nmb VARCHAR(50),
                # nmc VARCHAR(50),
                # nmw VARCHAR(50),
                # nmx VARCHAR(50),
                # nmy VARCHAR(50),
                # hib VARCHAR(50),
                # spn VARCHAR(50),
                # final_intepretation VARCHAR(50),
            
            cur.copy_expert("""
            COPY tmp_ext_csm_case (case_id, ward,vaccination_status,vaccinated_men5doses,
                sample_collected,result_interpretation, date_of_report)
            FROM STDIN WITH CSV
            """, buffer)
# facility, case_classification,first_symptom,date_specimen_collected,date_specimen_received_at_lab,date_specimen_tested,sodc,hpd,
                #lyta,species,nma,nmb,nmc,nmw,nmx,nmy,hib,spn,final_intepretation,
            cur.execute("""
            INSERT INTO ext_csm_case (case_id, ward,vaccination_status,vaccinated_men5doses,
                sample_collected,result_interpretation,date_of_report)
            SELECT case_id, ward,vaccination_status,vaccinated_men5doses,
                sample_collected,result_interpretation, date_of_report
            FROM tmp_ext_csm_case
            ON CONFLICT (case_id) DO UPDATE SET 
                        date_of_report = EXCLUDED.date_of_report,
                        ward = EXCLUDED.ward,
                        vaccination_status = EXCLUDED.vaccination_status,
                        vaccinated_men5doses = EXCLUDED.vaccinated_men5doses,
                        sample_collected = EXCLUDED.sample_collected,
                        result_interpretation = EXCLUDED.result_interpretation
            """)
            
            conn.commit()
            return len(inserted_cases)
        except Exception as e:
            if conn:
                conn.rollback()
            raise Exception(f"Error in load_csm_extension_table: {str(e)}") from e
        finally:
            if conn:
                try:
                    if cur:
                        cur.close()
                    conn.close()
                except:
                    pass

    # -------------------------
    # Run Logging - End
    # -------------------------
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def end_run(records, loaded_cases, etl_run_id, **context):
        conn = None
        cur = None
        try:
            extracted_count = len(records) if records else 0
            loaded_count = loaded_cases if loaded_cases else 0

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
                    records_extracted = %s,
                    records_loaded = %s           
                WHERE run_id=%s
            """, (status, extracted_count, loaded_count, etl_run_id))

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

    validated = validate_data(cleaned)

    deduped = deduplicate(validated)

    resolved = resolve_dimensions(deduped)

    versioned = apply_case_versioning(resolved)

    case_fact = load_core_fact_table(versioned)

    csm_extension =load_csm_extension_table(versioned, case_fact)

    end_run(records, csm_extension, run )


csm_pipeline()