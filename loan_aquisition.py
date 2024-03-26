import math
import random

from datetime import datetime

from utils import discrete_cols, norm_cols, data_types, generate_random_within_range, get_random_choice


def column_is_null(orig_date, seller_name, col, acq_config):
    if col in acq_config['missing_rate'][seller_name]:
        return acq_config['missing_rate'][seller_name][col] > random.random()
    return False

def generate_col_from_distribution(name, orig_date, seller_name, acq_config):
    if column_is_null(orig_date, seller_name, name, acq_config):
        return None
    else:
        keys_and_weights = acq_config['distribution'][seller_name][f"{name}_weight"]
        val = get_random_choice(keys_and_weights)
        if name in data_types and data_types[name] == 'int':
            if val == 'NaN':
                return None
            if val == "":
                return None
            return int(float(val))
        return val



def generate_col_from_normal_distribution(name, orig_date, seller_name,acq_config):
    if column_is_null(orig_date, seller_name, name, acq_config):
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

def generate_loan(orig_month, orig_year, loan_id, acq_config):

    orig_date = f"{orig_year}-{orig_month:02d}-01"

    first_pay_month = orig_month + 2 if orig_month + 2 <= 12 else (orig_month + 2) % 12
    first_pay_year = orig_year + (orig_month + 2) // 12
    seller_name_weight = acq_config['seller_distribution_daily'][orig_date]
    seller_name = get_random_choice(seller_name_weight)

    cols_dict = {}
    cols_dict['seller_name'] = seller_name
    cols_dict['loan_id'] = loan_id
    cols_dict['origination_date'] = datetime.strptime(orig_date, "%Y-%m-%d").date()
    cols_dict['first_payment_date'] = datetime.strptime(f"{first_pay_year}-{first_pay_month}-01", "%Y-%m-%d").date()

    for col in discrete_cols:
        cols_dict[col] = generate_col_from_distribution(col, orig_date, seller_name, acq_config)

    for col in norm_cols:
        cols_dict[col] = generate_col_from_normal_distribution(col, orig_date, seller_name, acq_config)

    return cols_dict