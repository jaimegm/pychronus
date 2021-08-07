import logging
import os
import tempfile

from airflow.models import Variable
from google.cloud import bigquery

from pychronus.hooks.gcs import GCSHook
from pychronus.hooks.gsheet import GSheetHook


# Quickly delete a object on GCS
def delete_gcs_object(bucket_name, object_name):
    gcs_hook = GCSHook()
    try:
        gcs_hook.delete(bucket_name, object_name)
        logging.info("gcs object deleted")
    except:  # noqa: E722
        logging.info("No gcs object to delete")


# Pass a Pandas DataFrame to Google Cloud Storage
def df_to_gcs(df, bucket_name, object_name, mode="bool"):
    logging.info(
        f"Processing: bucket: {bucket_name} Object: {object_name} mode: {mode}"
    )
    if mode == "bool":
        object_name = f"{object_name}.csv" if ".csv" not in object_name else object_name
        logging.info("Standard Load Method")
    elif mode == "datalake":
        logging.info("Datalake Load Method")
        # object_name = build_datalake_gcs_path(object_name)
    else:
        raise ValueError("df_to_gcs load method Not Defined")
    logging.info("Trying to execute: {} :: {}".format(bucket_name, object_name))
    with tempfile.NamedTemporaryFile("w+", suffix=".csv") as tmp:
        if df.shape[0] == 0:
            logging.info("Empty Dataframe, Upload Canceled..")
            state = False
        else:
            logging.info("Uploading to GCS...")
            df.to_csv(tmp.name, header=True, index=False, encoding="utf-8")
            GCSHook().upload(
                bucket_name=bucket_name,
                object_name=object_name,
                filename=tmp.name,
                mime_type="text/csv",
            )
            logging.info(f"Upload Complete: gs://{bucket_name}/{object_name}")
            state = object_name
            if mode == "bool":
                state = True
    return state


# A Special method for uploading CSV files to big query with a defined Schema
def gcs_csv_to_bigquery(
    dataset_id, table_name, object_name, schema, mode="truncate", header=True, **kwargs
):
    # Experienced issues creating a table and defining the schema, this method will make this easier
    logging.info(
        f"GCP Dataset: {dataset_id} table: {table_name} gcs_source_object:{object_name}"
    )
    # Build API Client
    credential_path = Variable.get("CREDENTIALS_PATH")
    credential_file = os.path.join(credential_path, "airflow_sa.json")
    client = bigquery.Client.from_service_account_json(credential_file)
    # Get Object Path for Mode: Datalake
    if "xcom" in object_name:
        previous_task_name = object_name[5:]
        object_name = kwargs["ti"].xcom_pull(task_ids={previous_task_name})[0]
        # Datalake bucket is hardcoded, Xcom in object name is how datalake mode is triggered
        object_name = f"gs://infarm-datalake/{object_name}"
    # If dataset or table name include each other
    # Ex:backend.Delivery_Slips_Detailed_Report dataset_id = backend

    dataset_id = dataset_id.split(".")[0]
    table_name = table_name.split(".")[-1]
    logging.info(f"Parsed: Dataset: {dataset_id} Table: {table_name}")
    dataset_ref = client.dataset(dataset_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    if header:
        job_config.skip_leading_rows = 1
    # Table Schema definition
    job_config.schema = schema
    # CSV Upload options
    job_config.quote = '"'
    job_config.allow_jagged_rows = True
    job_config.encoding = "utf-8"
    job_config.allow_quoted_newlines = True
    job_config.autodetect = True
    if mode == "truncate":
        job_config.write_disposition = "WRITE_TRUNCATE"
        logging.info("write_disposition Truncate")
    else:
        job_config.write_disposition = "WRITE_APPEND"
        logging.info("write_disposition Append")
    load_job = client.load_table_from_uri(
        source_uris=object_name,
        destination=dataset_ref.table(table_name),
        job_config=job_config,
    )  # API request
    logging.info("Starting job {}".format(load_job.job_id))

    load_job.result()  # Waits for table load to complete.
    logging.info("Job finished.")

    destination_table = client.get_table(dataset_ref.table(table_name))
    logging.info("Loaded {} rows.".format(destination_table.num_rows))


def translate_dtype(dtype, name):
    if dtype == "int64":
        return "INTEGER"
    elif dtype == "float64":
        return "FLOAT64"
    elif dtype == "bool":
        return "BOOLEAN"
    elif dtype == "datetime64[ns]":
        return "DATE"
    else:
        return "STRING"


def make_bigquery_schema(df):
    schema = list()
    replacements = [
        (".", ""),
        ("&", ""),
        (" ", "_"),
        ("(", ""),
        ("ß", "ss"),
        ("Ä", "AE"),
        ("Ö", "OE"),
        ("Ü", "UE"),
        ("%", ""),
        ("É", "E"),
        ("È", "E"),
        ("ö", "OE"),
        (")", ""),
        ("#", "count"),
        ("!", ""),
        ("'", ""),
        ("/", ""),
        ("=", ""),
        ("-", "_"),
        ("_/t_", ""),
        ("/n", ""),
        ("\n", ""),
        ("?", ""),
        ("[", ""),
        ("]", ""),
        ("__", "_"),
    ]
    x = 1
    for name in df.columns:
        form = str(df.dtypes[name])
        cast = translate_dtype(form, name)
        if name is None or name == "":
            name = f"no_field_name{x}"
            x = x + 1
        for bad_char, good_char in replacements:
            name = name.replace(bad_char, good_char).strip()
        if name[0] in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9"]:
            name = "_" + name
        line = bigquery.SchemaField(name, cast, mode="NULLABLE")
        schema.append(line)
    return schema


def df_to_bq(
    df,
    dataset_name,
    table_name,
    mode="load",
    load_method="truncate",
    bucket_name="infarm-data",
    schema=None,
):
    logging.info(f"Processing df_to_bq: {table_name} mode : {mode}")
    # Push DataFrame to GCS
    temporary_gcs_bucket = "infarm-data"
    # Check for data in df, If Empty cancel Workflow
    state = True if df.shape[0] != 0 else False
    # Check if df was pushed
    if state:
        # Big Query Schema Handling
        # Provide Load method with a Schema
        if schema is not None:
            # Schema Passed to Function
            auto_schema = schema
            logging.info(f"Schema Provided: {auto_schema}")
        else:
            # Auto Schema Build
            auto_schema = make_bigquery_schema(df)
            logging.info(f"Built BQ Schema: {auto_schema}")
        # Load Methods
        if mode == "load":
            # Standard auto load method (Doesnt push to Datalake)
            state = df_to_gcs(df, temporary_gcs_bucket, table_name, mode="bool")
            # Remove File extension if present, auto added in gcs path
            if ".csv" in table_name:
                table_name = table_name.split(".")[0]
            # Push to Bigquery
            gcs_csv_to_bigquery(
                dataset_name,
                table_name,
                f"gs://{temporary_gcs_bucket}/{table_name}.csv",
                schema=auto_schema,
                mode=load_method,
                header=True,
            )
            # Remove Object
            delete_gcs_object(temporary_gcs_bucket, f"{table_name}.csv")
            return True
        elif mode == "datalake":
            # Auto load method for storing files in Datalake
            datalake_path = df_to_gcs(df, bucket_name, table_name, mode="datalake")
            # CRM HACK: Triage 2020-12-10
            if "crm" in table_name.lower():
                table_name = "CRM_" + table_name.split(".")[-1]

            # df_to_gcs mode: datalake, Builds gcs path, uploads file
            # Then Returns the GCS path
            gcs_csv_to_bigquery(
                dataset_id=dataset_name,
                table_name=table_name,
                object_name=f"gs://{bucket_name}/{datalake_path}",
                schema=auto_schema,
                mode=load_method,
                header=True,
            )
            # Manual Auto Schema builder allows gcs_csv_tobigquery to
            # operator without a schema, Utilizes BQ API Client
            return table_name
        else:
            # Return Error if invalid mode
            raise ValueError("df_to_bq Load Method Not Defined")
        # Method for checking uploaded Results for Duplcates: TBC
        if load_method == "append":
            logging.info("Checking Table For Duplicates...")
            # Run Duplication Check and Fix
            duplication = None
            if duplication:
                logging.info(f"Found Duplicates: Running {duplication} Workflow")
    else:
        logging.info("Table Load Cancelled")
        return False


# Callable Function: Pulls GSheet data
def pull_sheet(sheetid, sheet_range, bucket_name, object_name):
    hook = GSheetHook()
    sheet_data = hook.get_values_df(sheetid, sheet_range, shape_column=None)
    try:
        df_to_gcs(sheet_data, bucket_name, object_name)
        logging.info("Uploaded Gsheet {} to GCS, Woo!".format(object_name))
    except:  # noqa: E722
        logging.info("Failed to Upload Gsheet {} to GCS, Boo".format(object_name))


def gsheet_to_bq(sheetid, sheet_range, dataset_name, table_name):
    hook = GSheetHook()
    sheet_data = hook.get_values_df(sheetid, sheet_range, shape_column=None)
    df_to_bq(sheet_data, dataset_name, table_name)


def update_google_doc(sheet_id, sheet_range, data, start_cell="A1"):
    gsheet = GSheetHook()
    sheet_name = sheet_range.split("!")[0]
    logging.info(
        "Updating Sheetname: {}  Range {}".format(sheet_name, sheet_range.split("!")[1])
    )
    gsheet.truncate_values(spreadsheet_id=sheet_id, range_=sheet_range, header=False)
    logging.info("Google Doc Truncated")
    logging.info("Data Shape {}".format(data.shape))
    gsheet.write_values(
        spreadsheet_id=sheet_id,
        sheetname=sheet_name,
        dataframe=data,
        start_cell=start_cell,
    )
    logging.info("Google Doc Updated")


# Pull data from BigQuery
def query_bigquery(query, project_id="infarm-data"):
    credential_path = Variable.get("CREDENTIALS_PATH")
    if project_id == "infarm-data":
        sa_file = "airflow_sa.json"
    elif project_id == "production":
        sa_file = "production-datascience-sa.json"
    elif project_id == "staging":
        sa_file = "staging-datascience-sa.json"
    else:
        raise ValueError("Unknown Project ID")
    credential_file = os.path.join(credential_path, sa_file)
    logging.info("Querying BigQuery: {}".format(query))
    client = bigquery.Client.from_service_account_json(credential_file)
    df = client.query(query)
    return df.to_dataframe()
