import os
from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.task.trigger_rule import TriggerRule
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import pandas as pd
import io
import tempfile


DAG_ID = "hr_pipeline"
staging_conn_id = "staging_postgres_db"
dw_conn_id = "dw_postgres"

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
# Return XCom safe data by converting datetimes to strings and ensuring all data is JSON serializable


@dag(
    dag_id=DAG_ID,
    start_date=datetime(2026,1,1),
    schedule="0 * * * *",
    catchup=False,
    tags=["human resources"],
    default_args={
        "on_failure_callback": log_task_failure
    }
)

def hr_pipeline():

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


    # -------------------------
    # Incremental Extract
    # -------------------------
    @task
    def extract_data(started):
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        try:
            if not started:
                raise ValueError(f"Pipeline did not start properly, no etl_run_id found")
            staging = PostgresHook(postgres_conn_id=staging_conn_id)
            sql = "SELECT stored_path, id FROM file_uploads WHERE file_type='hr_data' ORDER BY uploaded_at DESC LIMIT 1"
            df = staging.get_pandas_df(sql)
            if df is None or df.empty:
                mark_run_skipped(started)
                raise AirflowSkipException(f"There is no upload record found in the staging database")
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
                            hr_data = pd.read_csv(full_path)
                        elif file_ext in ['.xlsx', '.xls']:
                            hr_data = pd.read_excel(full_path)
                        else:
                            raise ValueError(f"Unsupported file extension '{file_ext}' for {file_name}")

                        if hr_data is None or hr_data.empty:
                            raise ValueError(f"{file_name} is empty or has no data")
                        else:
                            cur = conn.cursor()
                            cur.execute("""
                                    UPDATE etl_run_log
                                    SET file_name = %s,
                                        file_upload_id = %s,
                                        records_extracted = %s
                                    WHERE run_id=%s
                                """, (file_name, file_id, len(hr_data), started))
                            
                            conn.commit()
                            tmp_file = tempfile.NamedTemporaryFile(
                                suffix=".parquet",
                                prefix=f"cholera_{started}_",
                                delete=False
                            )
                            object_cols = hr_data.select_dtypes(include=["object"]).columns

                            for col in object_cols:
                                hr_data[col] = hr_data[col].astype(str)
                           
                            hr_data.to_parquet(tmp_file.name, index=False)

                            return {
                                "run_id": started,
                                "path": tmp_file.name,
                                "rows": len(hr_data)
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
            df.replace("null", pd.NA, inplace=True)
            
            dw = PostgresHook(postgres_conn_id=dw_conn_id)

            states = dw.get_pandas_df("SELECT state_id,state_name FROM master_state")
            if states is None or states.empty:
                raise Exception("No states found in master_state table")

            lgas = dw.get_pandas_df("SELECT lga_id,lga_name,state_id FROM master_lga")
            if lgas is None or lgas.empty:
                raise Exception("No LGAs found in master_lga table")
            
            dept = dw.get_pandas_df("SELECT department_id, department_name FROM master_departments")
            if dept is None or dept.empty:
                raise Exception("No departments found in master_departments table")

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
            
            df.to_parquet(records["path"], index=False)
            return records

        except Exception as e:
            raise Exception(f"Error in clean_data: {str(e)}") from e
    
   
   
    @task
    def load_hr_fact_table(records):
        conn = None
        cur = None
        try:
            if not records:
                raise ValueError("load_hr_fact_table received empty list, cannot proceed with loading data")
            
            df = pd.read_parquet(records["path"])

            for col in ["ippis", "department_id","lga_id","state_id"]:
                if col in df.columns:
                    df[col] = df[col].astype("Int64")
            
            required_cols = ["file_no", "ippis", "surname", "firstname", "othername", "rank", "sgl","department_id","primary_qualification","other_qualification", "date_of_birth", "date_of_1st_appt","date_of_appt_conf","lga_id","state_id","date_of_pp_appt","location","sex","years_in_service","staff_on_leave"]

            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns: {missing_cols}")
            
            fact_df = df[required_cols].copy()
            fact_df.columns = ["file_no", "ippis", "surname", "firstname", "othername", "rank", "sgl","department_id","primary_qualification","other_qualification", "date_of_birth", "date_of_1st_appt","date_of_appt_conf","lga_id","state_id","date_of_pp_appt","location","sex","years_in_service","staff_on_leave"]
            
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
                leave_status VARCHAR(50)
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
            date_of_pp_appt,location,sex,years_in_service, leave_status)
            FROM STDIN WITH CSV
            """, buffer)

            cur.execute("""
            INSERT INTO core_hr_fact
            (file_no,ippis,surname,firstname,othername,
            rank,sgl,department,primary_qualification,other_qualification,
            date_of_birth,date_of_1st_appt,date_of_appt_conf,lga_of_origin,state_of_origin,
            date_of_pp_appt,location,sex,years_in_service,leave_status)
            SELECT file_no,ippis,surname,firstname,othername,
                rank,sgl,department,primary_qualification,other_qualification,
                date_of_birth,date_of_1st_appt,date_of_appt_conf,lga_of_origin,state_of_origin,
                date_of_pp_appt,location,sex,years_in_service,leave_status
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
                leave_status = EXCLUDED.leave_status
            RETURNING file_no, ippis
            """)

            conn.commit()
            return True
        except Exception as e:
            raise Exception(f"Error in load_core_hr_fact_table: {str(e)}") from e
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
    # -------------------------
    # Run Logging - End
    # -------------------------
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def end_run(etl_run_id, **context):
        conn = None
        cur = None
        try:
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
    hr_fact = load_hr_fact_table(cleaned_data)
    end = end_run(run )
    cleanup = cleanup_temp(records)

    hr_fact >> end >> cleanup


hr_pipeline()