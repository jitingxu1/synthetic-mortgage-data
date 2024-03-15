
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
from uuid import uuid4


data_types = {
 'original_upb': 'int',
 'original_loan_term': 'int',
 'number_of_borrowers': 'int',
 'coborrower_credit_score_at_origination': 'int',
 'number_of_units': 'int',
 'zip': 'int',
 'borrower_credit_score_at_origination':'int',
 'mortgage_issuance_percentage': 'int',
 'mortgage_insurance_type': 'int',
 'original_ltv': 'int',
 'original_cltv': 'int',
 'dti': 'int',
 }


acq_headers = [
 'loan_id',
 'channel',
 'seller_name',
 'original_interest_rate',
 'original_upb',
 'upb_at_issuance',
 'original_loan_term',
 'origination_date',
 'first_payment_date',
 'original_ltv',
 'original_cltv',
 'number_of_borrowers',
 'dti',
 'borrower_credit_score_at_origination',
 'first_time_buyer',
 'loan_purpose',
 'property_type',
 'number_of_units',
 'occupancy_status',
 'property_state',
 'zip',
 'mortgage_issuance_percentage',
 'coborrower_credit_score_at_origination',
 'borrower_credit_score_at_issuance',
 'coborrower_credit_score_at_issuance',
 'relocation_mortgage_indicator'
]

perf_headers = ['loan_id',
 'monthly_reporting_period',
 'servicer_name',
 'master_servicer',
 'current_interest_rate',
 'current_upb',
 'loan_age',
 'remaining_months_to_legal_maturity',
 'remaining_months_to_maturity',
 'maturity_date',
 'msa',
 'current_loan_delinquency_status',
 'loan_payment_history',
 'modification_flag',
 'mortgage_insurance_cancellation_flag',
 'zero_balance_code',
 'zero_balance_effective_date',
 'last_paid_installment_date',
 'foreclosure_date',
 'disposition_date',
 'foreclosure_costs',
 'credit_enhancement_proceeds',
 'repurchase_make_whole_proceeds',
 'other_foreclosure_proceeds',
 'modification_noninterest_bearing_upb',
 'principal_foregiveness_amount',
 'repurchase_make_whole_proceeds_flag',
 'borrower_credit_score_current',
 'coborrower_credit_score_current',
 'foreclosure_principal_writeoff_amount',
 'next_interest_rate_adjustment_date',
 'next_payment_change_date'
 ]


discrete_cols = [
    'channel',
    # 'seller_name',
    'mortgage_issuance_percentage',
    'number_of_borrowers',
    'first_time_buyer',
    'number_of_units',
    'occupancy_status',
    'property_state',
    'loan_purpose',
    'property_type',
    'relocation_mortgage_indicator',
    'zip',
    'original_loan_term',
]

norm_cols = [
'original_ltv',
'original_upb',
'dti',
'coborrower_credit_score_at_origination',
'original_cltv',
'borrower_credit_score_at_origination',
'original_interest_rate', # depends on credit score and other factors
]




perf_schema = pa.schema([
    ("loan_id", pa.string()),
    ("monthly_reporting_period", pa.date32()),
    ("servicer_name", pa.string()),
    ("master_servicer", pa.string()),
    ("current_interest_rate", pa.float64()),
    ("current_upb", pa.float64()),
    ("loan_age", pa.int64()),
    ("remaining_months_to_legal_maturity", pa.int32()),
    ("remaining_months_to_maturity", pa.int32()),
    ("maturity_date", pa.date32()),
    ("msa", pa.string()),
    ("current_loan_delinquency_status", pa.string()),
    ("loan_payment_history", pa.string()),
    ("modification_flag", pa.string()),
    ("mortgage_insurance_cancellation_flag", pa.string()),
    ("zero_balance_code", pa.string()),
    ("zero_balance_effective_date", pa.date32()),
    ("last_paid_installment_date", pa.date32()),
    ("foreclosure_date", pa.date32()),
    ("disposition_date", pa.date32()),
    ("foreclosure_costs", pa.float64()),
    ("credit_enhancement_proceeds", pa.float64()),
    ("repurchase_make_whole_proceeds", pa.float64()),
    ("other_foreclosure_proceeds", pa.float64()),
    ("modification_noninterest_bearing_upb", pa.float64()),
    ("principal_foregiveness_amount", pa.float64()),
    ("repurchase_make_whole_proceeds_flag", pa.string()),
    ("borrower_credit_score_current", pa.int32()),
    ("coborrower_credit_score_current", pa.int32()),
    ("foreclosure_principal_writeoff_amount", pa.float64()),
    ("next_interest_rate_adjustment_date", pa.date32()),
    ("next_payment_change_date", pa.date32())
])

acq_schema = pa.schema([
    ("loan_id", pa.string()),
    ("channel", pa.string()),
    ("seller_name", pa.string()),
    ("original_interest_rate", pa.float64()),
    ("original_upb", pa.float64()),
    ("upb_at_issuance", pa.float64()),
    ("original_loan_term", pa.int32()),
    ("origination_date", pa.date32()),
    ("first_payment_date", pa.date32()),
    ("original_ltv", pa.int64()),
    ("original_cltv", pa.int64()),
    ("number_of_borrowers", pa.int32()),
    ("dti", pa.float64()),
    ("borrower_credit_score_at_origination", pa.int32()),
    ("first_time_buyer", pa.string()),
    ("loan_purpose", pa.string()),
    ("property_type", pa.string()),
    ("number_of_units", pa.int64()),
    ("occupancy_status", pa.string()),
    ("property_state", pa.string()),
    ("zip", pa.string()),
    ("mortgage_issuance_percentage", pa.float64()),
    ("coborrower_credit_score_at_origination", pa.int32()),
    ("borrower_credit_score_at_issuance", pa.int32()),
    ("coborrower_credit_score_at_issuance", pa.int32()),
    ("relocation_mortgage_indicator", pa.string())
])



def column_is_null(orig_date, seller_name, col):
    if col in acq_config['missing_rate'][seller_name]:
        return acq_config['missing_rate'][seller_name][col] > random.random()
    return False

def generate_col_from_distribution(name, orig_date, seller_name):
    if column_is_null(orig_date, seller_name, name):
        return None
    else:
        keys_and_weights = acq_config['distribution'][seller_name][f"{name}_weight"]
        val = get_random(keys_and_weights)
        if name in data_types and data_types[name] == 'int':
            if val == 'NaN':
                return None
            if val == "":
                return None
            return int(float(val))
        return val

def generate_col_from_normal_distribution(name, orig_date, seller_name):
    if column_is_null(orig_date, seller_name, name):
        return None
    else:
        stats = acq_config['col_norm_distribution'][orig_date][seller_name]
        # print(f"stats ={stats}")
        mean = stats[f"{name}_mean"]
        std = stats[f"{name}_std"]
        min_value = stats[f"{name}_min"]
        max_value = stats[f"{name}_max"]

        if not mean or math.isnan(mean):
            return None

        val  = generate_random_within_range(mean, std, min_value, max_value)

        if name in data_types and data_types[name] == 'int':
            if val == 'NaN':
                return None
            if val == "":
                print(name)
            return float(int(round(val)))
        return val



def generate_random_within_range(mean, std, min_val, max_val):

    while True:
        if not std or math.isnan(std):
            return mean
        random_value = np.random.normal(loc=mean, scale=std)
        if min_val <= random_value <= max_val:
            return round(random_value, 3)


def get_random(candidates_weights):

    return random.choices(list(candidates_weights.keys()), list(candidates_weights.values()))[0]

def generate_loan(orig_month, orig_year, loan_id):

    orig_date = f"{orig_year}-{orig_month:02d}-01"

    first_pay_month = orig_month + 2 if orig_month + 2 <= 12 else (orig_month + 2) % 12
    first_pay_year = orig_year + (orig_month + 2) // 12
    seller_name_weight = acq_config['seller_distribution_daily'][orig_date]
    seller_name = get_random(seller_name_weight)

    cols_dict = {}
    cols_dict['seller_name'] = seller_name
    cols_dict['loan_id'] = loan_id
    cols_dict['origination_date'] = datetime.strptime(orig_date, "%Y-%m-%d").date()
    cols_dict['first_payment_date'] = datetime.strptime(f"{first_pay_year}-{first_pay_month}-01", "%Y-%m-%d").date()

    for col in discrete_cols:
        cols_dict[col] = generate_col_from_distribution(col, orig_date, seller_name)

    for col in norm_cols:
        cols_dict[col] = generate_col_from_normal_distribution(col, orig_date, seller_name)

    return cols_dict




def generate_col_from_normal_distribution_perf(name):

    stats = perf_conf['col_norm_distribution']
    # print(f"stats ={stats}")
    mean = stats[f"{name}_mean"]
    std = stats[f"{name}_std"]
    min_value = stats[f"{name}_min"]
    max_value = stats[f"{name}_max"]

    if not mean or math.isnan(mean):
        return None

    val  = generate_random_within_range(mean, std, min_value, max_value)

    if name in data_types and data_types[name] == 'int':
        if val == 'NaN':
            return None
        return float(int(round(val)))
    return val



def generate_perf(loan):

        trans = []
        loan_id = loan['loan_id']
        interest_rate = loan['original_interest_rate']
        orig_date = loan['origination_date']
        seller_name = loan['seller_name']
        loan_term = loan['original_loan_term']
        reporting_month = orig_date + relativedelta(months=1)



        # weights
        loan_age_weight = perf_conf['distribution']['loan_age_weight']
        delingquent_weight = perf_conf['distribution']['current_loan_delinquency_status_weight']
        msa_weight = perf_conf['msa'][loan['property_state']].get(str(loan['zip']), {'00000': 1})
        servicer_weight = perf_conf['servicer'][seller_name]
        zero_balance_code_weight = perf_conf['zero_balance_code_distribution']

        # maturity_month = int(reporting_month+loan_term%12)
        maturity_date = orig_date + relativedelta(months=loan_term)

        age = int(get_random(loan_age_weight))
        delingquent_num = int(get_random(delingquent_weight))
        # print(f"delingquent_num = {delingquent_num}")
        msa = get_random(msa_weight)
        servicer = get_random(servicer_weight)
        zero_balance_code_last = get_random(zero_balance_code_weight)
        # current_loan_delinquency_status = 0.0

        # upb
        upb_skip = random.choices([1,2,3,4])[0]
        monthly_upb = loan['original_upb']/loan_term
        current_actual_upb = loan['original_upb']

        # delingquent
        delingquent_status = 0

        for i in range(0, age+1):

            zero_balance_code = None
            delingquent_status_code = str(delingquent_status).zfill(2)
            if i == age and delingquent_num < 1:
                    delingquent_status_code = random.choices(["00", "XX"], [0.01, 0.99])[0]

            trans.append(
                [
                    loan_id,
                    reporting_month, # reporting month
                    servicer, # servicer
                    "", #master_servicer
                    float(interest_rate) if interest_rate else None, # current_interest_rate
                    None if upb_skip > 0 else round(current_actual_upb, 1) , # current_upb
                    i, # loan_age
                    loan_term, # remaining_months_to_legal_maturity
                    loan_term - 1, # remaining_months_to_maturity
                    maturity_date,
                    msa, # msa
                    delingquent_status_code, # current_loan_delinquency_status
                    "", #loan_payment_history
                    "Y" if random.random() < 0.01 else "N", # modification_flag
                    "", #mortgage_insurance_cancellation_flag
                    zero_balance_code if i < age else zero_balance_code_last, #zero_balance_code
                    reporting_month if delingquent_num >= 1 and i == age else None, #zero_balance_effective_date
                    reporting_month if delingquent_num >= 1 and i == age else None, #'last_paid_installment_date',
                    reporting_month if delingquent_num >= 1 and i == age else None, #'foreclosure_date',
                    reporting_month if delingquent_num >= 1 and i == age else None, # 'disposition_date',
                    generate_col_from_normal_distribution_perf('foreclosure_costs') if delingquent_num > 0 and i == age else None, # 'foreclosure_costs',
                    generate_col_from_normal_distribution_perf('credit_enhancement_proceeds') if delingquent_num > 0 and i == age else None, #'credit_enhancement_proceeds',
                    generate_col_from_normal_distribution_perf('repurchase_make_whole_proceeds') if delingquent_num > 0 and i == age else None, #'repurchase_make_whole_proceeds',
                    generate_col_from_normal_distribution_perf('other_foreclosure_proceeds') if delingquent_num > 0 and i == age else None, # 'other_foreclosure_proceeds',
                    generate_col_from_normal_distribution_perf('modification_noninterest_bearing_upb') if delingquent_num > 0 and i == age else None, # modification_noninterest_bearing_upb',
                    generate_col_from_normal_distribution_perf('principal_foregiveness_amount') if delingquent_num > 0 and i == age else None, #'principal_foregiveness_amount',
                    None, #'repurchase_make_whole_proceeds_flag',
                    None, #'borrower_credit_score_current',
                    None, #'coborrower_credit_score_current',
                    generate_col_from_normal_distribution_perf('credit_enhancement_proceeds') if delingquent_num > 0 and i == age else None, #'foreclosure_principal_writeoff_amount',
                    None, #'next_interest_rate_adjustment_date',
                    None, #'next_payment_change_date'
                ]
            )
            reporting_month += relativedelta(months=1)
            loan_term -= 1
            upb_skip -= 1
            if upb_skip <= 0:
                current_actual_upb -= monthly_upb
            if age - i <= delingquent_num:
                delingquent_status += 1
                current_actual_upb += monthly_upb

        return trans



def load_json(file_path):
    with open(file_path, 'r') as json_file:
        data_dict = json.load(json_file)
    return data_dict

def generate_data(partition, output_path, scale=1):
    loans = []
    perfs = []

    for year in range(1999, 2024):
        for month in range(1, 13):
            orig_date = f"{year}-{month:02d}-01"
            # print(f"working on {orig_date}")
            if orig_date not in acq_config['loan_cnt_by_date']:
                continue

            # Generate loan based on the loan count distribution in the original dataset
            scaled_loan_cnt = int(acq_config['loan_cnt_by_date'][orig_date] * scale)
            for i in range(scaled_loan_cnt):
                loan_id = str(uuid4())
                acq_dict = generate_loan(month, year, loan_id)
                loans.append([acq_dict[key] if key in acq_dict else None for key in acq_headers])
                trans = generate_perf(acq_dict)
                perfs.extend(trans)
    print("saving tables")
    perf_table = pa.Table.from_arrays([pa.array(col) for col in zip(*perfs)], schema=perf_schema)
    pq.write_table(perf_table, f'{output_path}/perf/perf_{partition}.parquet', compression='ZSTD')
    del perf_table
    del perfs

    loan_table = pa.Table.from_arrays([pa.array(col) for col in zip(*loans)], schema=acq_schema)
    pq.write_table(loan_table, f'{output_path}/acq/acq_{partition}.parquet', compression='ZSTD')
    del loan_table
    del loans



def main(start_year, end_year, scale = 0.1):
    global perf_conf 
    global acq_config

    start_time = time.time()
    for y in range(start_year, end_year + 1):
        for q in range(1, 5):
            dataset = f"{y}Q{q}"
            print(f"work on {dataset}")
            # Looks like we are missing 2003Q2 and Q3 data
            if y == 2003 and q in [2, 3]: continue
            # Need to reload the precomputed distribution for each quarter
            
            perf_conf = load_json(f"./config/perf/{dataset}/perf.json")
            acq_config = load_json(f"./config/acq/{dataset}/acq.json")
            generate_data(dataset, './data', scale=scale)



if __name__ == '__main__':
    start_year = 2000
    end_year = 2000
    scale = 0.1
    acq_config = {}
    perf_config = {}
    main(start_year, end_year, scale)


