import ibis
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

from ibis import _



def write_to_json(data, file_path):
    with open(file_path, 'w') as json_file:
        json.dump(data, json_file, indent=4)


def generate_perf_stats(dataset):
    perf = ibis.read_parquet(f"/Users/claypot/Documents/mortgage/perf/mortgage_full_dataset_parquet_perf_perf_{dataset}.parquet")
    conf_path = "/Users/voltrondata/repos/synthetic-mortgage-data/config"
    acq = ibis.read_parquet(f"/Users/claypot/Documents/mortgage/acq/mortgage_full_dataset_parquet_acq_acq_{dataset}.parquet")
    # acq = acq.mutate(origination_date=ibis._.origination_date.cast(str))
    mortgage = perf.join(acq, acq['loan_id']==perf['loan_id'], how='left')

    config_dict = defaultdict(lambda: defaultdict(lambda: defaultdict(lambda: defaultdict(dict))))

    agg = mortgage.group_by(['loan_id']).order_by('monthly_reporting_period').agg(
        zero_balance_code_last = ibis._.zero_balance_code.last(),
        borrower_credit_score_at_origination=ibis._.borrower_credit_score_at_origination.max()
    ).mutate(
        borrower_credit_score_at_origination=ibis.case()
        .when(ibis._.borrower_credit_score_at_origination < 500, 0)
        .when(ibis._.borrower_credit_score_at_origination < 520, 1)
        .when(ibis._.borrower_credit_score_at_origination < 540, 2)
        .when(ibis._.borrower_credit_score_at_origination < 560, 3)
        .when(ibis._.borrower_credit_score_at_origination < 580, 4)
        .when(ibis._.borrower_credit_score_at_origination < 600, 5)
        .when(ibis._.borrower_credit_score_at_origination < 620, 6)
        .when(ibis._.borrower_credit_score_at_origination < 640, 7)
        .when(ibis._.borrower_credit_score_at_origination < 660, 8)
        .when(ibis._.borrower_credit_score_at_origination < 680, 9)
        .when(ibis._.borrower_credit_score_at_origination < 700, 10)
        .when(ibis._.borrower_credit_score_at_origination < 720, 11)
        .when(ibis._.borrower_credit_score_at_origination < 740, 12)
        .when(ibis._.borrower_credit_score_at_origination < 760, 13)
        .when(ibis._.borrower_credit_score_at_origination < 780, 14)
        .when(ibis._.borrower_credit_score_at_origination < 800, 15)
        .when(ibis._.borrower_credit_score_at_origination < 820, 16)
        .when(ibis._.borrower_credit_score_at_origination < 840, 17)
        .when(ibis._.borrower_credit_score_at_origination < 860, 18)
        .else_(19)
        .end()

    ).group_by([
        "borrower_credit_score_at_origination",
        "zero_balance_code_last",
    ]).agg(
        zero_balance_code_cnt=ibis._.zero_balance_code_last.isnull().count()
    ).order_by([
        'borrower_credit_score_at_origination',
        'zero_balance_code_cnt'
    ]).to_pandas()

    for _, row in agg.iterrows():
        config_dict['zero_balance_code_distribution'][row['borrower_credit_score_at_origination']][row['zero_balance_code_last']] = row['zero_balance_code_cnt']

    agg = mortgage.group_by(['loan_id']).order_by('monthly_reporting_period').agg(
        zero_balance_code_last = ibis._.zero_balance_code.last(),
        loan_age_max=ibis._.loan_age.max()
    ).group_by(["zero_balance_code_last", "loan_age_max"]).agg(
        cnt=ibis._.loan_id.count()
    ).order_by(["zero_balance_code_last", "loan_age_max"]).to_pandas()
    for i, row in agg.iterrows():
        config_dict['loan_age_distribution'][row['zero_balance_code_last']][row['loan_age_max']] = row['cnt']

    agg = mortgage.group_by(['loan_id']).order_by('monthly_reporting_period').agg(
        zero_balance_code_last = ibis._.zero_balance_code.last(),
        current_loan_delinquency_status=ibis._.current_loan_delinquency_status.replace("XX", "-1").cast(float).max()
    ).group_by(["zero_balance_code_last", "current_loan_delinquency_status"]).agg(
        cnt=ibis._.loan_id.count()
    ).order_by(["zero_balance_code_last", "current_loan_delinquency_status"]).to_pandas()
    for i, row in agg.iterrows():
        config_dict['delinquency_distribution'][row['zero_balance_code_last']][row['current_loan_delinquency_status']] = row['cnt']


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
    for _, row in agg.iterrows():
        config_dict['servicer'][row['seller_name']][row['servicer_name']] = row['cnt']


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
    
    for _, row in agg.iterrows():
         for col in cols:
              if bool(pattern.match(col)):
                  config_dict['col_norm_distribution'][col] = 0 if np.isnan(row[col]) else row[col]
    
    write_to_json(config_dict, f"./config/perf/{dataset}/perf.json")
            
    
    
if __name__ == '__main__':
    for y in range(2000, 2024):
        for q in range(1, 5):
            if y == 2003 and q in [2,3]: continue
            print(f"working on {y}Q{q}")
            generate_perf_stats(f"{y}Q{q}")