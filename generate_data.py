
import json

import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
import sys

from uuid import uuid4

from multiprocessing import Pool
import os

from loan_performance import generate_perf
from loan_aquisition import generate_loan
from utils import acq_headers, acq_schema, perf_schema



def load_json(file_path):
    with open(file_path, 'r') as json_file:
        data_dict = json.load(json_file)
    return data_dict

def save_data(data, schema, output_path, partition, name, num):

    
    
    pyarrow_table = pa.Table.from_arrays([pa.array(col) for col in zip(*data)], schema=schema)
    file_name = f'{output_path}/{name}/{name}_{partition}_{num}.parquet'
    pq.write_table(pyarrow_table, file_name, compression='ZSTD')
    del pyarrow_table
    

def generate_loan_and_perf(partition, output_path, sf_name, config_path, max_mem_mb, scale=1):
    print(f"partition = {partition}")

    output_path = f'{output_path}/sf={scale}_{sf_name}/{partition}'

    if not os.path.isdir(output_path):
        try:
            os.makedirs(f'{output_path}/acq')
            os.makedirs(f'{output_path}/perf')
        except Exception as e:
            print(f"Error creating directory: {e}")

    perf_config_path = f"{config_path}/perf/{partition}"
    acq_config_path = f"{config_path}/acq/{partition}"
    # print(f"perf_config_path = {perf_config_path}")
    if not os.path.exists(perf_config_path) or not os.path.exists(acq_config_path):
        print("not existing...")
        return

    perf_conf = load_json(f"{perf_config_path}/perf.json")
    acq_config = load_json(f"{acq_config_path}/acq.json")
    loans = []
    perfs = []
    loan_cnt = 0
    perf_cnt = 0
    memory_size = 0
    for year in range(1999, 2025):
        for month in range(1, 13):
            orig_date = f"{year}-{month:02d}-01"
            # print(f"orig_date = {orig_date}")
            if "loan_cnt_by_date" not in acq_config or orig_date not in acq_config['loan_cnt_by_date']:
                continue

            # Generate loan based on the loan count distribution in the original dataset
            scaled_loan_cnt = int(acq_config['loan_cnt_by_date'][orig_date] * scale)
            for i in range(scaled_loan_cnt):
                loan_id = str(uuid4())
                acq_dict = generate_loan(month, year, loan_id, acq_config)
                loans.append([acq_dict[key] if key in acq_dict else None for key in acq_headers])
                trans = generate_perf(acq_dict, perf_conf)
                perfs.extend(trans)
                loan_cnt += 1
                perf_cnt += len(trans)

                if loan_cnt % 5000 == 0:
                    memory_size = sys.getsizeof(perfs)
                    memory_size += sys.getsizeof(loans)
                    print(f"mem = {memory_size / (1024*1024)}mb")

                if memory_size > 1024*1024*max_mem_mb:
                    print(f"saving tables - chunks {memory_size / (1024*1024)}mb")
                    save_data(loans, acq_schema, output_path, partition, "acq", loan_cnt)
                    loans = []
                    save_data(perfs, perf_schema, output_path, partition, "perf", perf_cnt)                   
                    perfs = []
                    memory_size = 0
                    

    if len(loans) > 0 and len(perfs) > 0:
        # print("saving remaining tables")
        # print(f"perf to {output_path}/perf/perf_{partition}.parquet")
        save_data(perfs, perf_schema, output_path, partition, "perf", perf_cnt)
        del perfs

        # print(f"acq to {output_path}/acq/acq_{partition}.parquet")
        save_data(loans, acq_schema, output_path, partition, "acq", loan_cnt)
        del loans
