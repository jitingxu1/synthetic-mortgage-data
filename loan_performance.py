import random
import math
import sys

from datetime import datetime
from dateutil.relativedelta import relativedelta

from utils import get_random_choice, generate_random_within_range, data_types


LAST_LOAN_REPORTING_DATE = datetime.strptime("2023-06-01", '%Y-%m-%d').date()

def generate_col_from_normal_distribution_perf(name, perf_conf):

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



def get_last_loan_status():
    return 10



def generate_perf_data_for_current_loan(loan, perf_conf, zero_balance_code=""):
    '''
    For a current loan, there are several options:
     1) the reporting month is up to "current": '2023-06-01'
     2) current_loan_delinquency_status: 
        - The majority status is non-delinquent: 00
        - some loans have 1 or 2 delingquent
        - very very few loans has more than 3 delinquent
        - TODO: they may have delingquent in the middle, which is ignored in this simulation
    '''
    trans = []
    loan_id = loan['loan_id']
    interest_rate = loan['original_interest_rate']
    orig_date = loan['origination_date']
    seller_name = loan['seller_name']
    loan_term = loan['original_loan_term']
    reporting_month = orig_date + relativedelta(months=1)
    maturity_date = orig_date + relativedelta(months=loan_term)

    # The loan payment will go to the LAST_LOAN_REPORTING_DATE
    max_loan_age = (LAST_LOAN_REPORTING_DATE - orig_date).days // 30
    delingquent_num_weights = perf_conf["delinquency_distribution"][zero_balance_code]
    last_loan_status = int(float(get_random_choice(delingquent_num_weights)))
    current_loan_delinquency_status = 0

    msa_weight = perf_conf['msa'][loan['property_state']].get(str(loan['zip']), {'00000': 1})
    servicer_weight = perf_conf['servicer'][seller_name]

    msa = get_random_choice(msa_weight)
    servicer = get_random_choice(servicer_weight)

    # upb
    upb_skip = random.choices([1,2,3])[0]
    monthly_upb = loan['original_upb']/loan_term
    current_actual_upb = loan['original_upb']

    loan_payment_history = [0]*24

    for i in range(0, max_loan_age):

        trans.append(
            [
                loan_id,
                reporting_month, # reporting month
                servicer, # servicer
                "", #master_servicer
                float(interest_rate) if interest_rate else None, # current_interest_rate
                None if upb_skip > 0 else max(0, round(current_actual_upb, 1)) , # current_upb
                i, # loan_age
                loan_term, # remaining_months_to_legal_maturity
                loan_term - 1, # remaining_months_to_maturity
                maturity_date,
                msa, # msa
                f"{current_loan_delinquency_status:02d}", # current_loan_delinquency_status
                "".join([f"{s:02d}"for s in loan_payment_history]), #loan_payment_history
                "Y" if random.random() < 0.01 else "N", # modification_flag
                "", #mortgage_insurance_cancellation_flag
                zero_balance_code, #zero_balance_code
                None, #zero_balance_effective_date
                None, #'last_paid_installment_date',
                None, #'foreclosure_date',
                None, # 'disposition_date',
                None, # 'foreclosure_costs',
                None, #'credit_enhancement_proceeds',
                None, #'repurchase_make_whole_proceeds',
                None, # 'other_foreclosure_proceeds',
                None, # modification_noninterest_bearing_upb',
                None, #'principal_foregiveness_amount',
                None, #'repurchase_make_whole_proceeds_flag',
                None, #'borrower_credit_score_current',
                None, #'coborrower_credit_score_current',
                None, #'foreclosure_principal_writeoff_amount',
                None, #'next_interest_rate_adjustment_date',
                None, #'next_payment_change_date'
            ]
        )
        reporting_month += relativedelta(months=1)
        loan_term -= 1
        upb_skip -= 1
        if upb_skip <= 0:
            current_actual_upb -= monthly_upb
        
        if i + last_loan_status + 2 > max_loan_age:
                current_loan_delinquency_status += 1
                current_actual_upb += monthly_upb
                current_loan_delinquency_status = min(current_loan_delinquency_status, 99)

        loan_payment_history = loan_payment_history[1: ] +  [current_loan_delinquency_status]

        if i > loan_term or current_actual_upb <=0:
            break
        
    return trans


def generate_perf_data_for_prepaid_loan(loan, perf_conf, zero_balance_code="01"):
    '''
    For a prepaid or matured loan, there are several options:
     1) the loan was prepaid or matured
     2) last loan_delinquency_status: 
        - XX, some of them are 00
        - the last upb is set to 0
        - repurchase_make_whole_proceeds_flag is N
        - zero_balance_code is the last reporting month
     3) need sample a loan age
     4) TODO: it may default in the middle
    '''

    trans = []
    loan_id = loan['loan_id']
    interest_rate = loan['original_interest_rate']
    orig_date = loan['origination_date']
    seller_name = loan['seller_name']
    loan_term = loan['original_loan_term']
    reporting_month = orig_date + relativedelta(months=1)
    maturity_date = orig_date + relativedelta(months=loan_term)

    weights = perf_conf["loan_age_distribution"][zero_balance_code]
    max_loan_age = int(get_random_choice(weights))

    msa_weight = perf_conf['msa'][loan['property_state']].get(str(loan['zip']), {'00000': 1})
    servicer_weight = perf_conf['servicer'][seller_name]

    msa = get_random_choice(msa_weight)
    servicer = get_random_choice(servicer_weight)

    # upb
    upb_skip = random.choices([1,2,3])[0]
    monthly_upb = loan['original_upb']/loan_term
    current_actual_upb = loan['original_upb']

    loan_payment_history = "".join([f"{s:02d}"for s in [0]*24])

    for i in range(0, max_loan_age - 1):

        trans.append(
            [
                loan['loan_id'],
                reporting_month, # reporting month
                servicer, # servicer
                "", #master_servicer
                float(interest_rate) if interest_rate else None, # current_interest_rate
                None if upb_skip > 0 else max(0, round(current_actual_upb, 1)) , # current_upb
                i, # loan_age
                loan_term, # remaining_months_to_legal_maturity
                loan_term - 1, # remaining_months_to_maturity
                maturity_date,
                msa, # msa
                "00", # current_loan_delinquency_status
                loan_payment_history, #loan_payment_history
                "Y" if random.random() < 0.01 else "N", # modification_flag
                "", #mortgage_insurance_cancellation_flag
                "", #zero_balance_code
                None, #zero_balance_effective_date
                None, #'last_paid_installment_date',
                None, #'foreclosure_date',
                None, # 'disposition_date',
                None, # 'foreclosure_costs',
                None, #'credit_enhancement_proceeds',
                None, #'repurchase_make_whole_proceeds',
                None, # 'other_foreclosure_proceeds',
                None, # modification_noninterest_bearing_upb',
                None, #'principal_foregiveness_amount',
                None, #'repurchase_make_whole_proceeds_flag',
                None, #'borrower_credit_score_current',
                None, #'coborrower_credit_score_current',
                None, #'foreclosure_principal_writeoff_amount',
                None, #'next_interest_rate_adjustment_date',
                None, #'next_payment_change_date'
            ]
        )
        reporting_month += relativedelta(months=1)
        loan_term -= 1
        upb_skip -= 1
        if upb_skip <= 0:
            current_actual_upb -= monthly_upb
        
        if i > loan_term or current_actual_upb <=0:
            break

    if random.random() < 0.05: # TODO: Should have a ratio
        last_delingquent = "00"
    else:
        last_delingquent = "XX"

    trans.append(
        [
            loan_id,
            reporting_month, # reporting month
            None, # servicer
            "", #master_servicer
            None, # current_interest_rate
            0.0 , # current_upb
            None, # loan_age
            None, # remaining_months_to_legal_maturity
            None, # remaining_months_to_maturity
            None,
            msa, # msa
            last_delingquent, # current_loan_delinquency_status
            None, #loan_payment_history
            None, # modification_flag
            "", #mortgage_insurance_cancellation_flag
            zero_balance_code, #zero_balance_code
            reporting_month, #zero_balance_effective_date
            None, #'last_paid_installment_date',
            None, #'foreclosure_date',
            None, # 'disposition_date',
            None, # 'foreclosure_costs',
            None, #'credit_enhancement_proceeds',
            None, #'repurchase_make_whole_proceeds',
            None, # 'other_foreclosure_proceeds',
            None, # modification_noninterest_bearing_upb',
            None, #'principal_foregiveness_amount',
            "N",  #'repurchase_make_whole_proceeds_flag',
            None, #'borrower_credit_score_current',
            None, #'coborrower_credit_score_current',
            None, #'foreclosure_principal_writeoff_amount',
            None, #'next_interest_rate_adjustment_date',
            None, #'next_payment_change_date'
        ]
    )

        
    return trans


def generate_perf_data_for_3rd_party_sale_loan(loan, perf_conf, zero_balance_code="02"):
    '''
    For a 3rd party sale loan after N defaults:
     1) sample a number of loan age and delingquencies: 
        - last delingquent number is XX, vew few of them are the number of del
        - the last upb is set to 0
        - zero_balance_code is 02
        - zero_balance_effective_date is last reporting month
        - ┃ last_paid_installment_date 
        - ┃ foreclosure_date  ┃ disposition_date last reporting month
        - ┃ foreclosure_costs ┃ credit_enhancement_proceeds ┃ repurchase_make_whole_proceeds ┃ other_foreclosure_proceeds
        - repurchase_make_whole_proceeds_flag is "N"
     4) TODO: it may default in the middle
    '''

    trans = []
    loan_id = loan['loan_id']
    interest_rate = loan['original_interest_rate']
    orig_date = loan['origination_date']
    seller_name = loan['seller_name']
    loan_term = loan['original_loan_term']
    reporting_month = orig_date + relativedelta(months=1)
    maturity_date = orig_date + relativedelta(months=loan_term)

    weights = perf_conf["loan_age_distribution"][zero_balance_code]
    max_loan_age = int(get_random_choice(weights))
    delingquent_num_weights = perf_conf["delinquency_distribution"][zero_balance_code]
    delingquent_num = int(float(get_random_choice(delingquent_num_weights)))
    current_loan_delinquency_status = 0

    msa_weight = perf_conf['msa'][loan['property_state']].get(str(loan['zip']), {'00000': 1})
    servicer_weight = perf_conf['servicer'][seller_name]

    msa = get_random_choice(msa_weight)
    servicer = get_random_choice(servicer_weight)

    # upb
    upb_skip = random.choices([1,2,3])[0]
    monthly_upb = loan['original_upb']/loan_term
    current_actual_upb = loan['original_upb']

    loan_payment_history = [0]*24
    last_payment_date = None

    for i in range(0, max_loan_age - 1):

        trans.append(
            [
                loan_id,
                reporting_month, # reporting month
                servicer, # servicer
                "", #master_servicer
                float(interest_rate) if interest_rate else None, # current_interest_rate
                None if upb_skip > 0 else max(0, round(current_actual_upb, 1)) , # current_upb
                i, # loan_age
                loan_term, # remaining_months_to_legal_maturity
                loan_term - 1, # remaining_months_to_maturity
                maturity_date,
                msa, # msa
                f"{current_loan_delinquency_status:02d}", # current_loan_delinquency_status
                "".join([f"{s:02d}"for s in loan_payment_history]), #loan_payment_history
                "Y" if random.random() < 0.01 else "N", # modification_flag
                "", #mortgage_insurance_cancellation_flag
                "", #zero_balance_code
                None, #zero_balance_effective_date
                None, #'last_paid_installment_date',
                None, #'foreclosure_date',
                None, # 'disposition_date',
                None, # 'foreclosure_costs',
                None, #'credit_enhancement_proceeds',
                None, #'repurchase_make_whole_proceeds',
                None, # 'other_foreclosure_proceeds',
                None, # modification_noninterest_bearing_upb',
                None, #'principal_foregiveness_amount',
                None, #'repurchase_make_whole_proceeds_flag',
                None, #'borrower_credit_score_current',
                None, #'coborrower_credit_score_current',
                None, #'foreclosure_principal_writeoff_amount',
                None, #'next_interest_rate_adjustment_date',
                None, #'next_payment_change_date'
            ]
        )
        reporting_month += relativedelta(months=1)
        loan_term -= 1
        upb_skip -= 1
        if upb_skip <= 0:
            current_actual_upb -= monthly_upb
        if i + delingquent_num + 2 > max_loan_age:
                if not last_payment_date:
                     last_payment_date = reporting_month - relativedelta(months=1)
                current_loan_delinquency_status += 1
                current_actual_upb += monthly_upb
                current_loan_delinquency_status = min(current_loan_delinquency_status, 99)

        loan_payment_history = loan_payment_history[1:] + [current_loan_delinquency_status]

        if i > loan_term or current_actual_upb <=0:
            break

    if zero_balance_code in ["02", "09", "15"] and random.random() < 0.05:
        last_delingquent = f"{delingquent_num:02d}"
    else:
        last_delingquent = "XX"

    trans.append(
        [
            loan_id,
            reporting_month, # reporting month
            None, # servicer
            "", #master_servicer
            interest_rate, # current_interest_rate
            0.0 , # current_upb
            None, # loan_age
            None, # remaining_months_to_legal_maturity
            None, # remaining_months_to_maturity
            None,
            msa, # msa
            last_delingquent, # current_loan_delinquency_status
            None, #loan_payment_history
            None, # modification_flag
            "", #mortgage_insurance_cancellation_flag
            zero_balance_code, #zero_balance_code
            reporting_month, #zero_balance_effective_date
            last_payment_date, #'last_paid_installment_date',
            reporting_month, #'foreclosure_date',
            reporting_month, # 'disposition_date',
            generate_col_from_normal_distribution_perf('foreclosure_costs', perf_conf), # 'foreclosure_costs',
            generate_col_from_normal_distribution_perf('credit_enhancement_proceeds', perf_conf), #'credit_enhancement_proceeds',
            generate_col_from_normal_distribution_perf('repurchase_make_whole_proceeds', perf_conf), #'repurchase_make_whole_proceeds',
            generate_col_from_normal_distribution_perf('other_foreclosure_proceeds', perf_conf), # 'other_foreclosure_proceeds',
            generate_col_from_normal_distribution_perf('modification_noninterest_bearing_upb', perf_conf), # modification_noninterest_bearing_upb',
            generate_col_from_normal_distribution_perf('principal_foregiveness_amount', perf_conf), #'principal_foregiveness_amount',
            "N", #'repurchase_make_whole_proceeds_flag',
            None, #'borrower_credit_score_current',
            None, #'coborrower_credit_score_current',
            None, #'foreclosure_principal_writeoff_amount',
            None, #'next_interest_rate_adjustment_date',
            None, #'next_payment_change_date'
        ]
    )
   
    return trans


def generate_perf_data_for_repurchased_loan(loan, perf_conf, zero_balance_code="02"):
    '''
    For a Repurchased loan:
     1) sample a number of loan age: 
        - last delingquent number is XX
        - the last upb is set to 0
        - zero_balance_code is 06
        - zero_balance_effective_date is last reporting month
        - repurchase_make_whole_proceeds_flag is "Y"
     4) TODO: it may default in the middle
    '''

    trans = []
    loan_id = loan['loan_id']
    interest_rate = loan['original_interest_rate']
    orig_date = loan['origination_date']
    seller_name = loan['seller_name']
    loan_term = loan['original_loan_term']
    reporting_month = orig_date + relativedelta(months=1)
    maturity_date = orig_date + relativedelta(months=loan_term)

    weights = perf_conf["loan_age_distribution"][zero_balance_code]
    max_loan_age = int(get_random_choice(weights))

    msa_weight = perf_conf['msa'][loan['property_state']].get(str(loan['zip']), {'00000': 1})
    servicer_weight = perf_conf['servicer'][seller_name]

    msa = get_random_choice(msa_weight)
    servicer = get_random_choice(servicer_weight)

    # upb
    upb_skip = random.choices([1,2,3])[0]
    monthly_upb = loan['original_upb']/loan_term
    current_actual_upb = loan['original_upb']

    for i in range(0, max_loan_age):

        trans.append(
            [
                loan_id,
                reporting_month, # reporting month
                servicer, # servicer
                "", #master_servicer
                float(interest_rate) if interest_rate else None, # current_interest_rate
                None if upb_skip > 0 or  i == max_loan_age -1 else max(0, round(current_actual_upb, 1)) , # current_upb
                i if i < max_loan_age -1 else None, # loan_age
                loan_term if i < max_loan_age -1 else None, # remaining_months_to_legal_maturity
                loan_term - 1 if i < max_loan_age -1 else None, # remaining_months_to_maturity
                maturity_date if i < max_loan_age -1 else None,
                msa, # msa
                "00" if i < max_loan_age -1 else "XX", # current_loan_delinquency_status
                "", #loan_payment_history
                "Y" if random.random() < 0.01 else "N", # modification_flag
                "", #mortgage_insurance_cancellation_flag
                "" if i < max_loan_age -1 else zero_balance_code, #zero_balance_code
                None if i < max_loan_age -1 else reporting_month, #zero_balance_effective_date
                None, #'last_paid_installment_date',
                None, #'foreclosure_date',
                None, # 'disposition_date',
                None, # 'foreclosure_costs',
                None, #'credit_enhancement_proceeds',
                None, #'repurchase_make_whole_proceeds',
                None, # 'other_foreclosure_proceeds',
                None, # modification_noninterest_bearing_upb',
                None, #'principal_foregiveness_amount',
                None if i < max_loan_age -1 else "Y", #'repurchase_make_whole_proceeds_flag',
                None, #'borrower_credit_score_current',
                None, #'coborrower_credit_score_current',
                None, #'foreclosure_principal_writeoff_amount',
                None, #'next_interest_rate_adjustment_date',
                None, #'next_payment_change_date'
            ]
        )
        reporting_month += relativedelta(months=1)
        loan_term -= 1
        upb_skip -= 1
        if upb_skip <= 0:
            current_actual_upb -= monthly_upb

        if i > loan_term or current_actual_upb <=0:
            break


        
    return trans


def generate_perf_data_for_non_performing_not_sale_loan(loan, perf_conf, zero_balance_code="02"):
    '''
    For a non_performing_not_sale_loan:
     1) sample a number of loan age: 
        - last delingquent number is XX
        - the last upb is set to 0
        - zero_balance_code is 16
        - zero_balance_effective_date is last reporting month
        - repurchase_make_whole_proceeds_flag is "N"
     4) TODO: it may default in the middle
    '''

    trans = []
    loan_id = loan['loan_id']
    interest_rate = loan['original_interest_rate']
    orig_date = loan['origination_date']
    seller_name = loan['seller_name']
    loan_term = loan['original_loan_term']
    reporting_month = orig_date + relativedelta(months=1)
    maturity_date = orig_date + relativedelta(months=loan_term)


    weights = perf_conf["loan_age_distribution"][zero_balance_code]
    max_loan_age = int(get_random_choice(weights))

    msa_weight = perf_conf['msa'][loan['property_state']].get(str(loan['zip']), {'00000': 1})
    servicer_weight = perf_conf['servicer'][seller_name]

    msa = get_random_choice(msa_weight)
    servicer = get_random_choice(servicer_weight)

    # upb
    upb_skip = random.choices([1,2,3])[0]
    monthly_upb = loan['original_upb']/loan_term
    current_actual_upb = loan['original_upb']

    for i in range(0, max_loan_age):

        trans.append(
            [
                loan_id,
                reporting_month, # reporting month
                servicer, # servicer
                "", #master_servicer
                float(interest_rate) if interest_rate else None, # current_interest_rate
                None if upb_skip > 0 or  i == max_loan_age -1 else max(0, round(current_actual_upb, 1)) , # current_upb
                i if i < max_loan_age -1 else None, # loan_age
                loan_term if i < max_loan_age -1 else None, # remaining_months_to_legal_maturity
                loan_term - 1 if i < max_loan_age -1 else None, # remaining_months_to_maturity
                maturity_date if i < max_loan_age -1 else None,
                msa, # msa
                "00" if i < max_loan_age -1 else "XX", # current_loan_delinquency_status
                "", #loan_payment_history
                "Y" if random.random() < 0.01 else "N", # modification_flag
                "", #mortgage_insurance_cancellation_flag
                "" if i < max_loan_age -1 else zero_balance_code, #zero_balance_code
                None if i < max_loan_age -1 else reporting_month, #zero_balance_effective_date
                None, #'last_paid_installment_date',
                None, #'foreclosure_date',
                None, # 'disposition_date',
                None, # 'foreclosure_costs',
                None, #'credit_enhancement_proceeds',
                None, #'repurchase_make_whole_proceeds',
                None, # 'other_foreclosure_proceeds',
                None, # modification_noninterest_bearing_upb',
                None, #'principal_foregiveness_amount',
                None if i < max_loan_age -1 else "N", #'repurchase_make_whole_proceeds_flag',
                None, #'borrower_credit_score_current',
                None, #'coborrower_credit_score_current',
                None, #'foreclosure_principal_writeoff_amount',
                None, #'next_interest_rate_adjustment_date',
                None, #'next_payment_change_date'
            ]
        )
        reporting_month += relativedelta(months=1)
        loan_term -= 1
        upb_skip -= 1
        if upb_skip <= 0:
            current_actual_upb -= monthly_upb
        
        if i > loan_term or current_actual_upb <=0:
            break
   
    return trans

def generate_perf(loan, perf_conf):
        '''
        Give a loan with information from the loan acquisition data:
            - loan_id: uuid
            - interest rate at loan origination
            - loan term at loan orig
            - seller name
            - loan originate date

        Generate loan payment transaction data:
            - zero_balance_code (most important: loan status)
                "" =  loan is current, loan is the most recent reporting mongth is 2023-06-01, has remianing upb.
                01 = Prepaid or Matured, upb is 0.
                02 = Third Party Sale
                03 = Short Sale
                06 = Repurchased
                09 = Deed-in-Lieu; REO Disposition
                15 = Non Performing Note Sale
                16 = Reperforming Note Sale
                96 = Removal (non-credit event), Applies to all CAS deals prior to and including 2015-C03:
                97 = Delinquency (credit event due to D180)
                98 = Other Credit Event
            - loan age
            - msa
            - servicer
            - reporting month
            - upb

         - 
        '''

        # The most important feature of a loan: zero_balance_code
        credit_score = loan["borrower_credit_score_at_origination"]
        credit_score_bin = 0
        if credit_score:
            if credit_score < 500:
                credit_score_bin = 0
            else:
                credit_score_bin = (credit_score - 500) // 20 + 1
        zero_balance_code_weight = perf_conf['zero_balance_code_distribution'][str(int(float(credit_score_bin)))]
        zero_balance_code = get_random_choice(zero_balance_code_weight)

        if zero_balance_code == "":
            return generate_perf_data_for_current_loan(loan, perf_conf, zero_balance_code)
        elif zero_balance_code == "01":
            return generate_perf_data_for_prepaid_loan(loan, perf_conf, zero_balance_code)
        elif zero_balance_code in ["02", "03", "09", "15"]:
            return generate_perf_data_for_3rd_party_sale_loan(loan, perf_conf, zero_balance_code)
        elif zero_balance_code in ["06"]:
            return generate_perf_data_for_repurchased_loan(loan, perf_conf, zero_balance_code)
        elif zero_balance_code in ["16"]:
            return generate_perf_data_for_non_performing_not_sale_loan(loan, perf_conf, zero_balance_code)
        else: # all others, al most no loan here
            return generate_perf_data_for_non_performing_not_sale_loan(loan, perf_conf, zero_balance_code)
        