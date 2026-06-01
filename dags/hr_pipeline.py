from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import DagRun
from datetime import datetime
import pandas as pd
import numpy as np
import io


DAG_ID = "hr_pipeline"
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
    tags=["human resources"]
)

def hr_pipeline():

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
    # Incremental Extract
    # -------------------------
    @task
    def extract_data(started):
        try:
            if not started:
                return []
            staging = PostgresHook(postgres_conn_id=staging_conn_id)
            sql = "SELECT * FROM hr_data"
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
                print("WARNING: clean_data received empty list, returning empty")
                return []
            df = pd.DataFrame(records)
            df.replace("null", pd.NA, inplace=True)
            
            dw = PostgresHook(postgres_conn_id=dw_conn_id)

            states = dw.get_pandas_df("SELECT state_id,state_name FROM master_state")
            if states is None or states.empty:
                raise Exception("No states found in master_state table")

            lgas = dw.get_pandas_df("SELECT lga_id,lga_name,state_id FROM master_lga")
            if lgas is None or lgas.empty:
                raise Exception("No LGAs found in master_lga table")
            
            dept = dw.get_pandas_df("SELECT department_id, department_name FROM master_department")
            if dept is None or dept.empty:
                raise Exception("No departments found in master_department table")

            if "state_of_origin" in df.columns:
                df["state"] = df["state_of_origin"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df["state"] = df["state_of_origin"].map({"fct": "federalcapitalterritory"}).fillna(df["state"])
                states["state_name"] = states["state_name"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df = df.merge(states, left_on="state", right_on="state_name", how="left")
            else:
                raise Exception("Missing 'state_of_origin' column in data")
            
            if "lga_of_origin" in df.columns and "state_id" in df.columns:
                df["lga"] = df["lga_of_origin"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df["lga"] = df["lga_of_origin"].map({"yenegoa": "yenagoa","aiyekiregbonyin":"gbonyin","munya":"moya","abujamunicipal":"municipalareacouncil"}).fillna(df["lga"])
                lgas["lga_name"] = lgas["lga_name"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df = df.merge(lgas, left_on=["lga", "state_id"], right_on=["lga_name", "state_id"], how="left")
            else:
                raise Exception("Missing 'lga_of_origin' or 'state_id' column in data")
            
            for col in ["lga_id", "state_id"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

            for col in ["date_of_birth", "date_of_1st_appt", "date_of_appt_conf", "date_of_pp_appt"]:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            if "department" in df.columns:
                df["department"] = df["department"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                dept["department_name"] = dept["department_name"].str.replace(r'[^a-zA-Z0-9]', '', regex=True).str.strip().str.lower()
                df = df.merge(dept, left_on="department", right_on="department_name", how="left")
            else:
                raise Exception("Missing 'department' column in data")
            
            return make_xcom_safe(df).to_dict("records")

        except Exception as e:
            raise Exception(f"Error in clean_data: {str(e)}") from e
    
    # -------------------------
    # Validation
    # -------------------------
    # @task
    # def validate_data(records, **context):
        # conn = None
        # try:
        #     if not records:
        #         return []
            
        #     df = pd.DataFrame(records)
        #     dag_run = context["dag_run"]   
        #     ti = context["task_instance"]     

        #    # valid_rows = []
        #     failures = []
            
        #     for idx, row in df.iterrows():
        #         try:
        #             age = pd.to_numeric(row.get("age"), errors="coerce")

        #             if pd.isna(row.get("state")) and pd.isna(row.get("lga")):
        #                 failures.append((int(idx),"Missing location",dag_run.run_id, ti.dag_id))

        #             elif pd.isna(age) or age < 0 or age > 120: 
        #                 failures.append((int(idx),"Invalid age",dag_run.run_id, ti.dag_id))

        #             elif pd.isna(row.get("epid_number")):
        #                 failures.append((int(idx),"Missing Epid number",dag_run.run_id, ti.dag_id))
        #             else:
        #                 # valid_rows.append(row)
        #                 pass
        #         except Exception as row_error:
        #             print(f"Row {idx} failed with error: {row_error}")
        #             failures.append((int(idx),f"Row processing error: {str(row_error)}",dag_run.run_id, ti.dag_id))
        #     if failures:
        #         try:
        #             hook = PostgresHook(postgres_conn_id=dw_conn_id)
        #             conn = hook.get_conn()
        #             cur = conn.cursor()
        #             cur.executemany("""
        #             INSERT INTO etl_validation_failures
        #             (row_number,failure_reason, run_id, dag_id)
        #             VALUES (%s,%s,%s,%s)
        #             """, failures)
        #             conn.commit()
        #         except Exception as db_error:
        #             print(f"DB Error inserting validation failures: {str(db_error)}")
        #             if conn:
        #                 conn.rollback()
        #             raise Exception(f"Error inserting validation failures: {str(db_error)}") from db_error
        #         finally:
        #             if conn:
        #                 try:
        #                     conn.close()
        #                 except:
        #                     pass

        #     df_valid = make_xcom_safe(df)
            
        #     return df_valid.to_dict("records")
            
            
        # except Exception as e:
        #     print(f"CRITICAL ERROR in validate_data: {str(e)}")
        #     raise Exception(f"Error in validate_data: {str(e)}") from e


        # -------------------------
   
    # -------------------------
    # Case Versioning
    # -------------------------
    @task
    def apply_record_versioning(records):

        try:
            if not records:
                print("WARNING: apply_record_versioning received empty list, returning empty")
                return []
            
            df = pd.DataFrame(records)
            dw = PostgresHook(postgres_conn_id=dw_conn_id)

            try:
                existing = dw.get_pandas_df("SELECT file_no, MAX(record_version) AS version FROM core_hr_fact GROUP BY file_no")
            except Exception as query_error:
                print(f"Warning: Could not query existing record versions: {str(query_error)}")
                existing = None
            
            if existing is None or existing.empty:
                existing = pd.DataFrame(columns=["file_no", "version"])
            
            if "file_no" in df.columns:
                df = df.merge(existing, on="file_no", how="left")
            
            df["record_version"] = df["version"].fillna(0) + 1
            df = df.convert_dtypes()
            df = df.where(pd.notnull(df), None)

            return make_xcom_safe(df).to_dict("records")
        except Exception as e:
            print(f"CRITICAL ERROR in apply_record_versioning: {str(e)}")
            raise Exception(f"Error in apply_record_versioning: {str(e)}") from e


    # -------------------------
    # Bulk Load
    # -------------------------
    @task
    def load_hr_fact_table(records):
        conn = None
        cur = None
        try:
            if not records:
                print("WARNING: load_hr_fact_table received empty list, returning empty DataFrame")
                return pd.DataFrame(columns=["case_fact_id", "epid_number"])
            
            df = pd.DataFrame(records)

            for col in ["lga_id", "state_id","age","wk","yr"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            required_cols = ["file_no", "ippis", "surname", "firstname", "othername", "rank", "sgl","department","primary_qualification","other_qualification", "date_of_birth", "date_of_1st_appt","date_of_appt_conf","lga_id","state_id","date_of_pp_appt","location","sex","years_in_service","record_version"]

            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns: {missing_cols}")
            
            fact_df = df[required_cols].copy()
            fact_df.columns = ["file_no", "ippis", "surname", "firstname", "othername", "rank", "sgl","department","primary_qualification","other_qualification", "date_of_birth", "date_of_1st_appt","date_of_appt_conf","lga_id","state_id","date_of_pp_appt","location","sex","years_in_service","record_version"]
            
            hook = PostgresHook(postgres_conn_id=dw_conn_id)
            conn = hook.get_conn()
            cur = conn.cursor()

            cur.execute("""
            CREATE TEMP TABLE tmp_core_hr_fact (
                file_no VARCHAR(50),
                ippis numeric,
                surname VARCHAR(100),
                firstname VARCHAR(100),
                othername VARCHAR(100),
                rank VARCHAR(100),
                sgl VARCHAR(100),
                department int,
                primary_qualification VARCHAR(100),
                other_qualification VARCHAR(100),
                date_of_birth DATE,
                date_of_1st_appt DATE,
                date_of_appt_conf DATE,
                lga_of_origin int,
                state_of_origin int,
                date_of_pp_appt DATE,                        
                location VARCHAR(100),
                sex VARCHAR(10),
                years_in_service int,
                record_version int
            ) ON COMMIT DROP
            """)
            
            buffer = io.StringIO()
            fact_df.to_csv(buffer, index=False, header=False)
            buffer.seek(0)

            cur.copy_expert("""
            COPY tmp_core_hr_fact
            (file_no,ippis,surname,firstname,othername,
            rank,sgl,department,primary_qualification,other_qualification,
            date_of_birth,date_of_1st_appt,date_of_appt_conf,lga_of_origin,state_of_origin,
            date_of_pp_appt,location,sex,years_in_service,record_version)
            FROM STDIN WITH CSV
            """, buffer)

            cur.execute("""
            INSERT INTO core_hr_fact
            (file_no,ippis,surname,firstname,othername,
            rank,sgl,department,primary_qualification,other_qualification,
            date_of_birth,date_of_1st_appt,date_of_appt_conf,lga_of_origin,state_of_origin,
            date_of_pp_appt,location,sex,years_in_service,record_version)
            SELECT file_no,ippis,surname,firstname,othername,
                rank,sgl,department,primary_qualification,other_qualification,
                date_of_birth,date_of_1st_appt,date_of_appt_conf,lga_of_origin,state_of_origin,
                date_of_pp_appt,location,sex,years_in_service,record_version
            FROM tmp_core_hr_fact
            ON CONFLICT (file_no) DO UPDATE
            SET
                surname = EXCLUDED.surname,
                firstname = EXCLUDED.firstname,
                othername = EXCLUDED.othername,
                rank = EXCLUDED.rank,
                sgl = EXCLUDED.sgl,
                department = EXCLUDED.department,
                primary_qualification = EXCLUDED.primary_qualification,
                other_qualification = EXCLUDED.other_qualification,
                date_of_birth = EXCLUDED.date_of_birth,
                date_of_1st_appt = EXCLUDED.date_of_1st_appt,
                date_of_appt_conf = EXCLUDED.date_of_appt_conf,
                lga_of_origin = EXCLUDED.lga_of_origin,
                state_of_origin = EXCLUDED.state_of_origin,
                date_of_pp_appt = EXCLUDED.date_of_pp_appt,
                location = EXCLUDED.location,
                sex = EXCLUDED.sex,
                years_in_service = EXCLUDED.years_in_service,
                record_version = EXCLUDED.record_version
            RETURNING file_no, ippis
            """)

            inserted_rows = cur.fetchall()
            if not inserted_rows:
                raise Exception("No rows inserted into core_hr_fact table")
            
            conn.commit()
            inserted_df = make_xcom_safe(pd.DataFrame(inserted_rows, columns=["file_no", "ippis"]))
            return inserted_df.to_dict("records")
        except Exception as e:
            print(f"CRITICAL ERROR in load_core_hr_fact_table: {str(e)}")
            if conn:
                conn.rollback()
            raise Exception(f"Error in load_core_hr_fact_table: {str(e)}") from e
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
    def end_run(records, loaded_records, etl_run_id, **context):
        conn = None
        cur = None
        try:
            extracted_count = len(pd.DataFrame(records)) if not pd.DataFrame(records).empty else 0
            loaded_count = len(pd.DataFrame(loaded_records)) if not pd.DataFrame(loaded_records).empty else 0

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
    
    standardized = clean_data(records)

    versioned = apply_record_versioning(standardized)

    hr_fact = load_hr_fact_table(versioned)

    end_run(records, hr_fact, run )


hr_pipeline()