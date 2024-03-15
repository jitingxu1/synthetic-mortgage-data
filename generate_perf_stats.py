
import json
import random
import re
import time
import math
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from collections import defaultdict



def write_to_json(data, file_path):
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)


def generate_perf_stats(dataset):
    perf = ibis.read_parquet(f"/Users/claypot/Documents/mortgage/perf/mortgage_full_dataset_parquet_perf_perf_{dataset}.parquet")
    conf_path = "/Users/voltrondata/repos/synthetic-mortgage-data/config"
    acq = ibis.read_parquet(f"/Users/claypot/Documents/mortgage/acq/mortgage_full_dataset_parquet_acq_acq_{dataset}.parquet")
    # acq = acq.mutate(origination_date=ibis._.origination_date.cast(str))
    mortgage = perf.join(acq, acq['loan_id']==perf['loan_id'], how='left')

    
    loan_level = perf.group_by(['loan_id']).aggregate(
        loan_age=ibis._.loan_id.count().cast(str),
        current_loan_delinquency_status=ibis._.current_loan_delinquency_status.replace("XX", "-1.0").cast(int).max().cast(str),
        # current_loan_delinquency_status_last=_.current_loan_delinquency_status.last(),      
    )
    cols = ['loan_age', 'current_loan_delinquency_status']
    config_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(dict))))
    for col in cols:
        agg = loan_level.group_by([col]).aggregate(
            cnt=loan_level.loan_id.count()
        ).to_pandas()
        for _, row in agg.iterrows():
            config_dict['distribution'][f'{col}_weight'][row[col]] = row['cnt']
    # write_to_json(config_dict, f"/Users/voltrondata/repos/synthetic-mortgage-data/config/perf/{dataset}/loan_trans.json" )


    # msa
    agg = mortgage.group_by(['property_state', 'zip', 'msa']).aggregate(
        cnt=ibis._.loan_id.count()
    ).to_pandas()

    for _, row in agg.iterrows():
        config_dict['msa'][row['property_state']][row['zip']][row['msa']] = row['cnt']
    
    # servicer
    agg = mortgage.group_by(['seller_name', 'servicer_name']).aggregate(
        cnt=ibis._.loan_id.count()
    ).to_pandas()

    servicer_info = defaultdict(lambda: defaultdict(lambda: defaultdict(dict)))
    for _, row in agg.iterrows():
        config_dict['servicer'][row['seller_name']][row['servicer_name']] = row['cnt']

    # discrete columns
    agg = perf.group_by('loan_id').aggregate(
            zero_balance_code=ibis._.zero_balance_code.last()
    ).group_by('zero_balance_code').aggregate(
        count=ibis._.loan_id.count()
    ).to_pandas()
    
    for _, row in agg.iterrows():
        config_dict['zero_balance_code_distribution'][row['zero_balance_code']] = row['count']

    # default
    agg = perf.aggregate(
        foreclosure_costs_mean=ibis._.foreclosure_costs.mean(),
        foreclosure_costs_std=ibis._.foreclosure_costs.std(),
        foreclosure_costs_min=ibis._.foreclosure_costs.min(),
        foreclosure_costs_max=ibis._.foreclosure_costs.max(),
        credit_enhancement_proceeds_mean=ibis._.credit_enhancement_proceeds.mean(),
        credit_enhancement_proceeds_std=ibis._.credit_enhancement_proceeds.std(),
        credit_enhancement_proceeds_min=ibis._.credit_enhancement_proceeds.min(),
        credit_enhancement_proceeds_max=ibis._.credit_enhancement_proceeds.max(),
        repurchase_make_whole_proceeds_mean=ibis._.repurchase_make_whole_proceeds.mean(),
        repurchase_make_whole_proceeds_std=ibis._.repurchase_make_whole_proceeds.std(),
        repurchase_make_whole_proceeds_min=ibis._.repurchase_make_whole_proceeds.min(),
        repurchase_make_whole_proceeds_max=ibis._.repurchase_make_whole_proceeds.max(),
        other_foreclosure_proceeds_mean=ibis._.other_foreclosure_proceeds.mean(),
        other_foreclosure_proceeds_std=ibis._.other_foreclosure_proceeds.std(),
        other_foreclosure_proceeds_min=ibis._.other_foreclosure_proceeds.min(),
        other_foreclosure_proceeds_max=ibis._.other_foreclosure_proceeds.max(),
        foreclosure_principal_writeoff_amount_mean=ibis._.foreclosure_principal_writeoff_amount.mean(),
        foreclosure_principal_writeoff_amount_std=ibis._.foreclosure_principal_writeoff_amount.std(),
        foreclosure_principal_writeoff_amount_min=ibis._.foreclosure_principal_writeoff_amount.min(),
        foreclosure_principal_writeoff_amount_max=ibis._.foreclosure_principal_writeoff_amount.max(),
        modification_noninterest_bearing_upb_mean=ibis._.modification_noninterest_bearing_upb.mean(),
        modification_noninterest_bearing_upb_std=ibis._.modification_noninterest_bearing_upb.std(),
        modification_noninterest_bearing_upb_min=ibis._.modification_noninterest_bearing_upb.min(),
        modification_noninterest_bearing_upb_max=ibis._.modification_noninterest_bearing_upb.max(),
        principal_foregiveness_amount_mean=ibis._.principal_foregiveness_amount.mean(),
        principal_foregiveness_amount_std=ibis._.principal_foregiveness_amount.std(),
        principal_foregiveness_amount_min=ibis._.principal_foregiveness_amount.min(),
        principal_foregiveness_amount_max=ibis._.principal_foregiveness_amount.max(),
        
        
    ).to_pandas()
    cols = agg.columns
    pattern = re.compile(r'.*(max|min|std|mean)')
    
    for idx, row in agg.iterrows():
    
    	for col in cols:
    		if bool(pattern.match(col)):
    			config_dict['col_norm_distribution'][col] = row[col]
    
    write_to_json(config_dict, f"/Users/voltrondata/repos/synthetic-mortgage-data/config/perf/{dataset}/perf.json" )
