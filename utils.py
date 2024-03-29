import random
import math
import numpy as np
import pyarrow as pa

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


def get_random_choice(candidates_weights):

    val =  random.choices(list(candidates_weights.keys()), list(candidates_weights.values()))[0]
    if val == "NaN":
        return "0"
    else:
        return val

def get_random_choices_with_range(candidates_weights, min_val = float('-inf'), max_val = float('inf')):

    while True:
        random_value = random.choices(list(candidates_weights.keys()), list(candidates_weights.values()))[0]
        if min_val <= random_value <= max_val:
            return round(random_value, 3)
        
def generate_random_within_range(mean, std, min_val, max_val):

    while True:
        if not std or math.isnan(std):
            return mean
        random_value = np.random.normal(loc=mean, scale=std)
        if min_val <= float(random_value) <= max_val:
            return round(random_value, 3)
        