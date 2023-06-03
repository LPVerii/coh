#!/usr/bin/env python

"""
Examples of JSON messages that can be proceed:
1. Ask for availabe tables or availabe anomalies:
'{"request":"tables"}'
'{"request":"anomalies"}'
2. Ask for availabe columns from a table (based on data from 1)
'{"request":"columns", "table_name":"c4-31110"}'
3. Ask for data from columns (based on data from 1 and 2)
'{"request":"data", "table":"c4-31110", "columns":["current"],
    "start_timestamp" :"1568418338", "end_timestamp":"1568457104"}'
There is also a possibility to ask for many columns at the same time
{"request":"data", "table":"c4-31110", "columns":["current", "voltage", "temperature"],
    "start_timestamp" :"1568418338", "end_timestamp":"1568457104"}

Copyright: Lantern Machinery Analytics 2023
"""
import asyncio
import datetime
import json
import logging as log
import http

import pandas as pd
import repackage
import websockets
import numpy as np

repackage.up(2)
from connect_bigquery import ConnectBigQuery
from data_handler import DataHandler
from data_handler import find_timestamp_column_index

with open("frontend_config.json") as f:
    config = json.load(f)

async def handler(websocket, path):
    bq = ConnectBigQuery()
    dh = None
    dh_units = None
    df = lis = None
    dataset_id = config["dataset_id"]
    
    while True:
        message = await websocket.recv()
        # message_dict = {"table": DEFAULT_TABLE_ID, "columns": []}
        message_dict = {}
        try:
            message_dict.update(json.loads(message))
        except json.JSONDecodeError:
            continue
        if "request" not in message_dict:
            continue
        request = message_dict['request']
        dict_out = {"reponse_to": request}

        if request == "connection":
            await websocket.send("Connected")
        if request == "tables":
            df = pd.DataFrame(bq.list_tables_id_in_dataset(dataset_id))
        elif request == "columns":
            names = bq.list_columns_name_from_table(dataset_id, message_dict["table"])
            lis = [
                [name]
                for name in names
                if name
                not in [config["timestamp_column_name"], config["label_column_name"]]
            ]
            df = pd.DataFrame(lis)
        elif request == "data":
            if not dh or dh.table_name.split(".")[1] != message_dict["table"]:
                dh = DataHandler(
                    f"{dataset_id}.{message_dict['table']}", convert_timestamps=False
                )
            if "columns" in message_dict:
                columns = message_dict["columns"]
            columns.extend([config["timestamp_column_name"]])
            bot = datetime.datetime.fromtimestamp(
                float(message_dict.get('start_timestamp', 0))
            )
            top = datetime.datetime.fromtimestamp(
                float(
                    message_dict.get('end_timestamp', 9223279200.0)
                )  # max pandas timestamp
            )
            df = dh.get_data(col_names=columns).data
            df = mark_anomaly_in_label_column(df, bot=bot, top=top)
            df[config["timestamp_column_name"]] = df[
                config["timestamp_column_name"]
            ].apply(lambda x: x.timestamp())
            if config["scale_data"]:
                columns_to_scale = [
                    column
                    for column in df.columns
                    if column not in [config["label_column_name"]]
                ]
                df_agg = df[columns_to_scale].agg([min, max])
                df[columns_to_scale] = scale_data(df[columns_to_scale])
                dict_out["df_with_original_max_min"] = df_agg.to_dict(orient='split')
        elif request == "anomalies":
            df = get_anomalies_df("anomaly", dataset_id)
            idxes = find_timestamp_column_index(df, all_indexes=True)
            for i in idxes:
                try:
                    df.iloc[:, i] = df.iloc[:, i].apply(lambda x: x.timestamp())
                except AttributeError:
                    log.info("Can't extract timestamp from column to sort by")
        elif request == "save":
            if message_dict.get('verify_anomaly', None) is not None:
                bq.client.query(f"UPDATE {config['dataset_id']}.{config['feedback_table_name']} \
                                SET is_true_positive = {bool(message_dict['verify_anomaly'])} \
                                WHERE anomaly_id = '{message_dict['anomaly_id']}'").result()   
            if message_dict.get('anomaly_type', None) is not None:
                bq.client.query(f"UPDATE {config['dataset_id']}.{config['feedback_table_name']} \
                                SET anomaly_type = '{message_dict['anomaly_type']}' \
                                WHERE anomaly_id = '{message_dict['anomaly_id']}'").result()
            continue
        elif request=="unit":
            units_dataset_name = config['units_dataset_name']
            units_table_name = config['units_table_name']
            if not dh_units or dh_units.table_name.split(".")[1] != units_table_name or dh_units.table_name.split(".")[0] != units_dataset_name:
                dh_units = DataHandler(f"{units_dataset_name}.{units_table_name}")
            df = dh_units.get_data(downsampling_indice=1).data
            if message_dict['col_name']=='ALL':
                pass
            else:
                df = df[df['name']==message_dict['col_name']]
        elif request=='histogram':
            if not message_dict.get('bins', None):
                message_dict['bins']='10'
            dataset_name = config['dataset_id']
            table_name = 'anomaly'
            if not dh or dh.table_name.split(".")[1] != table_name or dh.table_name.split(".")[0] != dataset_name:
                dh = DataHandler(f"{dataset_name}.{table_name}")
            data = dh.get_data(col_names='severity', downsampling_indice=1)
            counts, bins = np.histogram(data, bins=int(message_dict['bins']))
            non_empty_bins = bins[:-1][counts > len(data.values)*0.0001]
            hist, bins = np.histogram(data, bins=np.append(non_empty_bins, 1))  # histogram with irregular non-empty bins 
            df = pd.DataFrame({"hist":np.log(hist), "bins":bins[:-1]})
        else:
            continue
        dict_from_df = df.to_dict(orient='split')
        dict_from_df.pop('index', None)
        dict_out['df'] = dict_from_df
        if request == 'columns':
            dict_out['default'] = config['init_cols_for_plot']
        message_to_sent = json.dumps(dict_out)
        await websocket.send(message_to_sent)


def get_anomalies_df(table_id, dataset_id):
    cbq = ConnectBigQuery()
    query = (
        "SELECT "
        + (
            ", ".join(config["cols_from_anomaly_tab"])
            if config["cols_from_anomaly_tab"] is not None
            else "*"
        )
        + f" FROM {dataset_id}.{table_id} ORDER BY start_ts LIMIT 1000000"
    )
    job = cbq.client.query(query)
    df = job.result(timeout=300).to_dataframe()
    return df


def mark_anomaly_in_label_column(df: pd.DataFrame, bot: float, top: float):
    df[config["label_column_name"]] = 0
    df[config["label_column_name"]].mask(
        (bot <= df[config["timestamp_column_name"]])
        & (df[config["timestamp_column_name"]] <= top),
        1,
        inplace=True,
    )
    return df


def scale_data(df):
    # val = (2 *(val - min)/(max-min)) - 1
    df -= df.min()
    columns = [column for column in df.columns if df[column].max() != 0]
    df[columns] /= df[columns].max()
    return 2 * df - 1


async def main():
    async with websockets.serve(handler, "", config["port_number"]):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    asyncio.run(main())
