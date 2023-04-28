#!/usr/bin/env python
"""
Handles slicing and returning data in the supplied table name

The typical entry point is DataHandler.get_data.
All outputs are returned in the DataOutput class to simplify integration with UI

Copyright: Lantern Machinery Analytics 2021
"""
import logging as log
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
import repackage

repackage.up(1)
from connect_bigquery import ConnectBigQuery

DISPLAY_INDEX_LABEL_NAME = "display_index"
TIMESTAMP_START_STRING_LABEL_NAME = "timestamp_start_str"
DATA_DICT_ROOT_TABLE = "projectSetup"
DATA_DICT_LABEL_NAME = "data_dict_name"
DATA_DICT_NAME_COLUMN = "name"


def apply_filter(data: pd.DataFrame, filter_df: pd.DataFrame):
    """Return a df of values where all conditions in filter_df are met"""

    """ Filter df contains: N rows and the following columns:
    'and_or_to_prev' : OR if this statement needs to be joined to previous line
    with an OR, otherwise assume AND => NOT YET IMPLEMENTED
    'variable' : same as column name
    'operator' : one of the following strings: "<", ">", "==", "!="
    'value' : string, which should convert to a number
    """

    if not isinstance(filter_df, pd.DataFrame):
        log.warning("Filter must be a data frame")
        return data

    if filter_df.shape[0] == 0:
        log.warning("Empty filter df")
        return data

    or_flag = 0
    for idx, row in filter_df.iterrows():
        col = row["variable"]
        op = row["operator"]
        val = row["value"]
        if (
            'and_or_to_prev' in filter_df
        ):  # Note 2022-02-01 Houdini UI sets this to & by default.
            logic = row["and_or_to_prev"]
        else:
            logic = "&"
        try:
            float(val)
            this_query = " ".join([col, op, val])
        except ValueError:
            if data[col].dtype != 'object':
                raise TypeError("String query attempted on non-string column")
            this_query = create_string_query(col, op, val)
            data = data[data[col].notna()]  # prevent str.contains failure on None
        if idx == filter_df.shape[0] - 1:
            if or_flag:
                this_query = this_query + ")"
        else:
            next = filter_df.loc[(idx + 1)]
            if next["variable"] == col and next["and_or_to_prev"] == "|" and or_flag == 0:
                this_query = "(" + this_query
                or_flag = 1
            elif next["variable"] != col and or_flag == 1:
                this_query = this_query + ")"
                or_flag = 0
        if idx == 0:
            query_string = this_query
        else:
            query_string = f"{query_string} {logic} {this_query}"

    return data.query(
        query_string, engine='python'
    )  # engine = python allows str.contains


def zoom(data, x_col_name, col_names, limits):
    """Zoom into x and y limits of data"""
    row_min, row_max, col_min, col_max = limits
    # Range Filter
    # Note for now keep seperate from value filter as we resolve caching etc..
    if None not in [row_min, row_max]:
        data = data[(data[x_col_name] >= row_min) & (data[x_col_name] <= row_max)]
    if None not in [col_min, col_max]:
        data = data[(data[col_names[0]] >= col_min) & (data[col_names[0]] <= col_max)]
    return data


def create_string_query(column_name, operator_, string_):
    """Check for unsafe content then create string query"""

    blocklist = r"@\`"
    combined_string = column_name + operator_ + string_
    if any(x in blocklist for x in combined_string):
        raise ValueError("Query string contains disallowed characters")

    return column_name + ".str.contains('" + string_ + "')"


class DataOutput:
    def __init__(self, data=None, index=None, message="Not initialized", data_dict=None):
        self.data = data
        self.index = index
        self.message = message
        self.data_dict = data_dict

    def __getattr__(self, __name):
        return self.data.__getattr__(__name)


class DataHandler:
    """
    Class for handling data in the supplied table
    """

    _DEFAULT_MAX_NUM_PTS = 25000

    def __init__(
        self, table_name, key_file=None, debug_mode=False, convert_timestamps=True
    ):
        """Load in the data from the supplied table name"""

        self.timestamp_start_times = {}
        self.convert_timestamps = convert_timestamps
        self.debug_enabled = debug_mode  # Use this when debugging UI connection

        if self.debug_enabled:
            self.labels = {}
            self.data_dict = None
            self.query = None
            try:
                self.data = pd.read_csv(table_name)
            except TypeError:
                return
        else:
            # Create CBQ object and pull all the data from the supplied table
            big_query = ConnectBigQuery(key_file)

            query = f"SELECT * FROM `{table_name}`"
            self.data = big_query.get_data_from_table(query)
            self.table_name = table_name
            self.labels = big_query.find_labels(table_name)
            self.schema = big_query.client.get_table(table_name).schema
            self.col_names = list(_.name for _ in self.schema)
            self.data_dict = self._read_data_dict()
            if self.convert_timestamps:
                self._convert_timestamp_to_number()

            # Sort data by first date or time column in row
            self.data = self.data.sort_values(
                self.data.columns[find_timestamp_column_index(self.data)]
            )

    def _read_data_dict(self):
        """Uses a tables's data_dict_name label to look for descriptions + units"""

        if not self.labels.get(DATA_DICT_LABEL_NAME, False):
            return None

        dict_table_name = (
            f"{DATA_DICT_ROOT_TABLE}.{self.labels.get(DATA_DICT_LABEL_NAME)}"
        )
        query = f"SELECT * FROM `{dict_table_name}`"
        big_query = ConnectBigQuery()
        return big_query.get_data_from_table(query)

    def get_data_dict(self, column_names):
        """Return data dict values for each column_name"""

        if self.data_dict is None:
            return None

        return self.data_dict.loc[
            self.data_dict[DATA_DICT_NAME_COLUMN].isin(column_names)
        ]

    def _convert_timestamp_to_number(self, scale=24 * 3600 * 1000000000):
        """Checks all datatypes and converts any timestamps to int, apply scale
        to return days"""

        for i, dtype_ in enumerate(self.data.dtypes):
            if 'date' in dtype_.name:
                if self.labels.get(TIMESTAMP_START_STRING_LABEL_NAME, False):
                    # calculate time delta sse pandas for consistency with pd.to_numeric
                    # note timing test shows subtracting timestamps is ~as fast as
                    # substracting int
                    t0 = pd.to_datetime(
                        self.labels.get(TIMESTAMP_START_STRING_LABEL_NAME)
                    )
                    s = pd.to_numeric(self.data.iloc[:, i] - t0)
                else:
                    s = pd.to_numeric(self.data.iloc[:, i])
                    t0 = np.min(s)
                    s -= t0
                s /= scale
                self.data.iloc[:, i] = s
                self.timestamp_start_times[dtype_.name] = t0

    def get_display_index(self):
        """Read the display index label"""
        try:
            z_axis = self.labels.get(DISPLAY_INDEX_LABEL_NAME, None)
            return_message = ""
        except AttributeError:
            z_axis = None
            return_message = "index label not found in database"
        return z_axis, return_message

    def find_matching_column_name(self, column_name):
        """Check ONE name against column names, case insensitive
        Note: capital letters are not allowed in labels, but are in column names
        """
        matching_cols = [
            i for i, x in enumerate(self.data.columns.values) if x.lower() == column_name
        ]
        if len(matching_cols) == 1:
            return list(self.data.columns.values[matching_cols]), ""
        elif len(matching_cols) > 1:
            log.warning("Multiple matches to display refrence index")
        else:
            log.warning("Reference index not found")
        return None, "z_axis incorrectly specified"

    def get_columns(self):
        """Return dataframe column names"""
        return self.data.columns.values

    def get_data(
        self,
        x_col_name=None,
        col_names=None,
        row_min=None,
        row_max=None,
        col_min=None,
        col_max=None,
        filter_conds=pd.DataFrame(),  # df with name, operator, value, each row is a cond.
        downsampling_indice=None,
        max_points=_DEFAULT_MAX_NUM_PTS,
        z_axis=None,
    ):
        """Slice the loaded data based on the inputs and return the slice

        NOTES: - The current method of choosing every nth point can return
                 between 0.75 to 1.5 times _DEFAULT_MAX_NUM_PTS due to rounding
                 to the nearest int

        z_axis: returns values from a db column as a second output of this func.
                default: None
                if "index" returns the column defined by label "display_index" in bq
                otherwise returns the column name

        If no data found returns Dataoutput.Data with an empty df.
        If an error is found in the filter statement returns Dataoutut.data = None,
        If col_names == None, returns all columns

        """
        data = self.data
        limits = row_min, row_max, col_min, col_max
        return_message = ""

        # z_axis options
        if z_axis is not None:
            if z_axis.casefold() == "display_index":
                z_axis, return_message = self.get_display_index()  # look for label
            if z_axis is not None:
                z_axis, return_message = self.find_matching_column_name(z_axis)

        if col_names is None:
            col_names = self.get_columns().tolist()
        # Handle single column names
        if not isinstance(col_names, list):
            col_names = [col_names]

        if x_col_name is None:
            x_col_name = col_names[0]

        # Apply value filters
        try:
            data = apply_filter(data, filter_conds)
        except Exception as error_txt:
            dataout = DataOutput(None, None, error_txt)
            return dataout

        # Remove all columns except supplied row/col
        columns = [x_col_name] + col_names

        # Add z column
        if z_axis is not None:
            columns = columns + z_axis

        # Define data_local to allow inplace transformations w/o reducing full data set
        data_local = data.drop(data.columns.difference(columns), axis=1)
        # data_local.dropna(axis=0, inplace=True)

        data_local = zoom(data_local, x_col_name, col_names, limits)

        if self.debug_enabled:
            with open("after_limits_filter_debug.txt", "w") as f:
                print(columns, file=f)
                print(filter_conds, file=f)
                print(limits, file=f)
                print("===data===", file=f)
                print(data_local, file=f)
                print(columns, file=f)
        # Nth Point Filter
        if not downsampling_indice:
            if max_points > 0:  # if input is 0 or less, all points are displayed
                nth_pt = max(1, round(len(data_local) / max_points))
                data_local = data_local[::nth_pt]
        else:
            data_local = data_local[::downsampling_indice]

        # Improve compatibility with UI
        data_local = convert_Int64Dtype(data_local)
        data_dict = self.get_data_dict(columns)
        dataout = DataOutput(data_local, z_axis, return_message, data_dict)

        return dataout


def convert_Int64Dtype(df):
    """Convert extension int64 to np.int64, done for R compatibility"""
    NA_INT = 0
    cols_with_int64 = (i for i, t in enumerate(df.dtypes) if "Int64" in t.name)
    for this_col in cols_with_int64:
        if isinstance(df[df.columns[this_col]].dtype, pd.core.arrays.integer.Int64Dtype):
            df[df.columns[this_col]] = (
                df[df.columns[this_col]].fillna(NA_INT).astype(np.int64)
            )
    return df


def find_timestamp_column_index(df: pd.DataFrame, all_indexes=False):
    indexes = []
    for idx, dtype in enumerate(df.dtypes):
        if "date" in str.lower(dtype.name):
            indexes.append(idx)
        elif "time" in str.lower(dtype.name):
            indexes.append(idx)
    if all_indexes:
        return indexes
    if not indexes:
        log.info("No timestamp column detected, 0 index was returned")
        return 0
    elif len(indexes) > 1:
        log.info(
            "More than one timestamp column detected, index of the first was returned"
        )
    return indexes[0]


class DataHandlerPartitioned(DataHandler):
    """
    Class for handling data in the supplied partitioned table by partition
    """

    def __init__(
        self,
        table_name,
        debug_mode=False,
        convert_timestamps=True,
        start_time=None,
        column_to_set_condition=None,
    ):
        """Load in the data from the supplied table name"""

        if column_to_set_condition is None:
            super().__init__(table_name, debug_mode, convert_timestamps)
        else:
            self.timestamp_start_times = {}
            self.convert_timestamps = convert_timestamps
            self.debug_enabled = False  # Use this when debugging UI connection
            big_query = ConnectBigQuery()
            end_time = (
                datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f") + timedelta(hours=1)
            ).strftime("%Y-%m-%d %H:%M:%S.%f")
            query = f"""SELECT * FROM `{table_name}`
            WHERE {column_to_set_condition} >= DATETIME("{start_time}")
            AND {column_to_set_condition} < DATETIME("{end_time}")"""
            self.data = big_query.get_data_from_table(query)
            self.labels = big_query.find_labels(table_name)
            self.schema = big_query.client.get_table(table_name).schema
            self.col_names = list(_.name for _ in self.schema)
            self.data_dict = self._read_data_dict()
            if self.convert_timestamps:
                self._convert_timestamp_to_number()

            # Sort data - Assume 1st column is timestamp
            self.data = self.data.sort_values(self.data.columns[0])


def main():
    dh = DataHandler('plottableData.image_summary_data')
    zz = dh.get_data()
    zz.data


if __name__ == "__main__":
    main()
    pass  # debugConnectGCS()
