#!/usr/bin/env python
"""
Utilities to access GCP BigQuery.

TODO: The whole OO structure of this is suspect
but the general intent is that one can creates
objects with different BQ access privileges

Copyright: Lantern Machinery Analytics 2021
"""

import logging
import pathlib
from time import sleep

import pandas
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account
from pandas_gbq import gbq

PROJECT_ID = "just-plate-276602"
CHUNKSIZE = 10000  # size of data chunk when writing to df to BQ table
NUM_RETRIES = 3
WAIT_BETWEEN_RETRIES = 3


class ConnectBigQuery:
    """
    Utilities to connect to GCP BigQuery.
    """

    def __init__(self, project_id=None, key_file=""):
        """Open a big_query client, use env auth. or key file if provided."""
        KEY_FILE_EXTENSION = ".json"
        if (
            key_file is not None
            and pathlib.Path(key_file).is_file()
            and pathlib.Path(key_file).suffix.lower() == KEY_FILE_EXTENSION
        ):
            try:  # TODO see if keyfile exists
                self.credentials = service_account.Credentials.from_service_account_file(
                    key_file
                )
                self.client = bigquery.Client(project_id, credentials=self.credentials)
                self.project_id = self.get_project_id_from_auth()

            except (RuntimeError, TypeError, NameError):
                print("connection error")
                self.project_id = ""
        else:  # use default credentials
            self.client = bigquery.Client()
            self.project_id = self.client.project

    def get_data_from_table(self, query):
        """Pull data based on input query

        NOTES:
            The table name is assumed to be a part of the input query
        """

        try:
            data = self.client.query(query).result().to_dataframe()
            return data
        except NotFound:
            return None

    def create_table_from_query(self, query, destination_table):
        job_config = bigquery.QueryJobConfig(destination=destination_table)
        self.client.query(query, job_config=job_config).result()

    def if_tbl_exists(self, dataset_id, table_id):
        """Return true if table with this name exists."""
        try:
            table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
            self.client.get_table(table_ref)
            # print('table found', table_id)
            return True
        except NotFound:
            return False

    def get_project_id_from_auth(self):
        """
        Retrieve project ID associated with this authorizatin.
        Retrieves 1st ID only, multiple projects not well handled.
        """
        # NOTE: next(.list_projects()) fails with 'object is not an iterator'
        project_list = list(self.client.list_projects())
        return project_list[0].project_id

    def bq_add_rows_from_local_csv(self, target_uri, dataset_id, table_id):
        #  NOT COMPLETE TODO: this should be insert rows from any source...
        """Work in progress."""
        # Check here: https://stackoverflow.com/questions/36673456/
        # bigquery-insert-new-data-row-into-table-by-python/36849400
        pass

    def bq_create_from_pandas(
        self, df, dataset_id, table_id, add_new_columns=False, schema=None
    ):  # Tested 2019-12-30
        """Create a BigQuery Table from a pandas df."""
        table_name = f"{dataset_id}.{table_id}"
        if schema:
            job_config = bigquery.LoadJobConfig(schema=schema)
        else:
            job_config = bigquery.LoadJobConfig()
            # Schema autodetection enabled
            job_config.autodetect = True
        if add_new_columns:
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION,
                bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION,
            ]
        load_job = self.client.load_table_from_dataframe(
            df, table_name, job_config=job_config
        )
        #  print(f'Loading file {dataset_id} into the BQ table {table_id}')
        return load_job.result()

    def bq_create_partitioned_from_pandas(
        self, df, dataset_id, table_id, add_new_columns=False
    ):  # Tested 2019-12-30
        """Create a BigQuery Table from a pandas df."""
        table_name = f"{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.HOUR, field="Datetime"
            ),
        )
        if add_new_columns:
            job_config.schema_update_options = [
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ]
        load_job = self.client.load_table_from_dataframe(
            df, table_name, job_config=job_config
        )
        return load_job.result()

    def bq_create_from_gcs(self, target_uri, dataset_id, table_id):
        """Create a BQ table from a GS csv object."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        job_config = bigquery.LoadJobConfig()
        job_config.autodetect = True  # Schema autodetection enabled
        job_config.skip_leading_rows = 1  # 1st row == field names
        job_config.source_format = bigquery.SourceFormat.CSV  # data format in GCS

        load_job = self.client.load_table_from_uri(
            target_uri, table_ref, job_config=job_config
        )
        print("Starting job {}".format(load_job.job_id))
        print(f"Loading file {target_uri} into the BQ table {table_id}")
        return load_job.result()

    def bq_add_dataframe_to_table(self, df, dataset_id, table_id, schema=None):
        """Append a pandas df to BQ table."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        try:
            table = self.client.get_table(table_ref)
        except NotFound:
            self.bq_create_from_pandas(df, dataset_id, table_id, schema=schema)
            return
        for i in range(NUM_RETRIES):  # retry 3 times
            try:
                # write with streaming API
                write_status = self.client.insert_rows_from_dataframe(table, df)
            except NotFound:
                try:
                    self.bq_create_from_pandas(df, dataset_id, table_id, schema=schema)
                    return
                except Exception:
                    if i == NUM_RETRIES - 1:
                        # wait for table to be created then retry streaming
                        sleep(WAIT_BETWEEN_RETRIES)
                    else:
                        raise Exception
            else:
                if any(write_status):
                    write_status = self.bq_create_from_pandas(
                        df, dataset_id, table_id, schema=schema, add_new_columns=True
                    )

                return write_status

    def check_schema_against_df(self, df_to_insert, dataset_id, table_id):
        """Check number of fields and whether fields are string, bool, or numeric"""
        bqschema = self.client.get_table(f"{dataset_id}.{table_id}").schema
        DATATYPE_DICT = {  # list of BQ fields is not complete
            "O": ["STRING", "RECORD"],
            "i": [
                "BYTES",
                "INT64",
                "INT",
                "SMALLINT",
                "INTEGER",
                "BIGINT",
                "BYTEINT",
                "DECIMAL",
                "FLOAT64",
                "FLOAT",
            ],
            "f": ["NUMERIC", "DECIMAL", "FLOAT64", "FLOAT", "INT64", "INT", "TIMESTAMP"],
            "M": ["DATE", "DATETIME", "TIMESTAMP", "TIME"],
            "b": ["BOOLEAN"],
        }
        for bq_col in bqschema:
            df_col = df_to_insert.dtypes.get(bq_col.name)
            if df_col is None:
                continue  # fine if dataframe is missing a column
            elif bq_col.field_type not in DATATYPE_DICT.get(df_col.kind):
                logging.warning(f"Schema mismatch bq has {bq_col}, input has {df_col}")
                return bq_col, df_col

    def bq_add_and_overwrite_dataframe_to_table(
        self, df_to_insert, dataset_id, table_id, replace_col_name
    ):
        """Add a pandas df to BQ table, overwrite BQ based on replace_col_name"""

        # Check replace_col_name in incoming data
        replace_col_value = pandas.unique(df_to_insert[replace_col_name])
        if len(replace_col_value) != 1:
            logging.warning(
                "Key column must contain only 1 unique value when deleting rows"
            )
            return

        # Check schema compatibility before deleting
        if self.check_schema_against_df(df_to_insert, dataset_id, table_id):
            return

        # delete rows where
        dml_statmenet = f"""
            DELETE FROM `{dataset_id}.{table_id}`
            WHERE `{replace_col_name}` = "{replace_col_value[0]}"
            """
        query_job = self.client.query(dml_statmenet)
        delete_status = query_job.result()
        return (
            self.bq_create_from_pandas(df_to_insert, dataset_id, table_id, True),
            delete_status,
        )

    def bq_add_and_overwrite_dataframe_to_partitioned(
        self, df_to_insert, dataset_id, table_id, replace_col_name
    ):
        """Add a pandas df to BQ table, overwrite BQ based on replace_col_name"""

        # Check replace_col_name in incoming data
        replace_col_value = pandas.unique(df_to_insert[replace_col_name])
        if len(replace_col_value) != 1:
            logging.warning(
                "Key column must contain only 1 unique value when deleting rows"
            )
            return

        # Check schema compatibility before deleting
        if self.check_schema_against_df(df_to_insert, dataset_id, table_id):
            return

        # delete rows where
        dml_statmenet = f"""
            DELETE FROM `{dataset_id}.{table_id}`
            WHERE `{replace_col_name}` = "{replace_col_value[0]}"
            """
        query_job = self.client.query(dml_statmenet)
        delete_status = query_job.result()
        return (
            self.bq_create_partitioned_from_pandas(df_to_insert, dataset_id, table_id),
            delete_status,
        )

    def list_tables_in_dataset(self, dataset_id):
        """Lists the tables in a dataset"""
        tables = self.client.list_tables(dataset_id)
        return tables

    def list_tables_id_in_dataset(self, dataset_id):
        tables = self.client.list_tables(dataset_id)
        return [table.table_id for table in tables]

    def list_columns_name_from_table(self, dataset_id, table_id):
        table = self.client.get_table(f"{self.project_id}.{dataset_id}.{table_id}")
        return [schema.name for schema in table.schema]

    def bq_add_to_table(self, rows_to_insert, dataset_id, table_id):
        """Insert rows into a BigQuery table."""
        # Prepares a reference to the dataset and table
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        # API request to get table call
        table = self.client.get_table(table_ref)
        # API request to insert the rows_to_insert
        # print(f'Inserting rows into BigQuery table {table_id}')
        errors = self.client.insert_rows(table, rows_to_insert)
        return errors

    def bq_add_json_as_string(self):
        """Work in progress."""
        # ref: https://cloud.google.com/bigquery/docs/loading-data-cloud-storage-json
        # Alternately, storing directly in cloud storage (no query needed so why not !):
        # https://cloud.google.com/storage/docs/json_api/v1/objects
        pass

    def add_csv_to_table(self, csv_uri, table_id_as_sql_string):
        """Try to add a csv file to an existing BQ table, note failures"""

        full_table_id = f"{self.project_id}.{table_id_as_sql_string}"
        table_ref = bigquery.table.TableReference.from_string(full_table_id)
        job_config = bigquery.LoadJobConfig()
        # Note that default write disposition it to add
        job_config.skip_leading_rows = 1  # assume there is a header
        job_config.ignore_unknown_values = True  # to handle empty columns
        job_config.max_bad_records = 1  # expect errors in header

        # The source format defaults to CSV, so the line below is optional.
        start_rows = self.client.get_table(table_ref).num_rows
        job_config.source_format = bigquery.SourceFormat.CSV
        load_job = self.client.load_table_from_uri(
            csv_uri, table_ref, job_config=job_config
        )  # API request
        print("Starting job {}".format(load_job.job_id))
        load_job.result()  # Waits for table load to complete.
        print("Job finished.")

        return self.client.get_table(table_ref).num_rows - start_rows

    def find_labels(self, dataset_id, table_id=None, label_name=None):
        """Reads the value for passed label, or returns full label dict if called w.o.
        arg"""
        dataset_id, table_id = check_if_joined_dataset_and_table_id(dataset_id, table_id)
        labels = self.client.get_table(f"{dataset_id}.{table_id}").labels

        if label_name is None:
            return labels

        return labels.get(label_name, None)

    def bq_add_df_to_partitioned(self, df_to_insert, dataset_id, table_id):
        """Append a pandas df to partitioned BQ table."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        for i in range(NUM_RETRIES):  # retry 3 times
            try:
                write_status = self.client.insert_rows_from_dataframe(
                    table_ref,
                    df_to_insert,
                    chunksize=CHUNKSIZE,
                    if_exists="append",
                )
            except gbq.GenericGBQException as e:
                if i == NUM_RETRIES - 1:
                    raise e  # if it fails again after three atempts raise exception
                sleep(3 * (WAIT_BETWEEN_RETRIES**i))  # wait 3, 9, 27s
            except ValueError:  # Assume this is due to table not existing
                logging.warning(
                    f"Table {table_id} not found when inserting rows, creating table"
                )
                self.bq_create_from_pandas(df_to_insert, dataset_id, table_id)
                break
            else:
                return write_status


def check_if_joined_dataset_and_table_id(dataset_id, table_id):
    """looks for a period in dataset_id, and seperates it into 2 vars
    introduced to allow legacy code to work with new convention of
    incorporating table and dataset in one variable"""
    split_dataset = dataset_id.split(".")
    if len(split_dataset) == 2:
        return split_dataset[0], split_dataset[1]
    return dataset_id, table_id


def test_connect_keyfile(project_id, key_file):
    """
    Check handling of keyfile, including bad file type.

    To Run:
        test_connect_keyfile("oceanic-hold-262822","C:/Users/peter/OneDrive
    /Documents/Corporate_/ReliabilitySW/RandD/10-XXXX/10-0003
    /BigQuery/bqa3c636e60078.json")
    """
    BAD_KEYFILES = ["junk.txt", "", "notafile.json", "../Manage_ML.py"]
    # key_file = "C:/Users/peter/OneDrive/Documents/Corporate_/
    # ReliabilitySW/RandD/10-XXXX/10-0003/BigQuery/Master0-022e3e7fc527.json"

    print("=== trying good keyfiles, should connect to new service acct ===")
    bq = ConnectBigQuery(project_id, key_file)
    email = bq.client.get_service_account_email()
    print(f"Connected: credential email = {email}")
    print(f"Project id = {bq.project_id}")
    bq = ConnectBigQuery()
    default_cred_email = bq.client.get_service_account_email()
    print(f"Default credential email = {default_cred_email}")
    print(f"Project id = {bq.project_id}")
    print("=== trying bad keyfiles, should connect to default ===")
    for file_name in BAD_KEYFILES:
        try:
            bq = ConnectBigQuery("", file_name)
            email = bq.client.get_service_account_email()
            print(f"Connected: credential email = {email}")
        except Exception:
            email = bq.client.get_service_account_email()
            print(f"Exception: default credential email = {email}")
        print(f"Project id = {bq.project_id}")


def test_get_labels():
    """Local test function untill a proper py_test module is created"""
    bq = ConnectBigQuery()
    print(f"Project id = {bq.project_id}, should be just-plate-276602")
    di1 = bq.find_labels(
        "plottableData", "ubc_cnn_combined_flows", label_name="display_index"
    )
    labels = bq.find_labels("plottableData", "ubc_cnn_combined_flows")
    print(f"display index = {di1}, should be 'time' ")
    print(f"full label dict = \n{labels}")
