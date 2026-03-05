
import pandas as pd

import logging
logger = logging.getLogger(__name__)
exclusion_master_file = '/Users/balkrishna/Documents/Projects/market_place_360/campaign/config/exclusion_master.xlsx'
exclusion_mstr_df = pd.read_excel(exclusion_master_file)
rules_map = exclusion_mstr_df.set_index('exclusion_name')
print(exclusion_mstr_df.head())


input_df = pd.read_csv("/Users/balkrishna/Documents/Projects/market_place_360/campaign/export/email_2326.csv")

"""
step1: read input, read exclusion rule

step2: create an order

"""
# print(input_df.head())
def apply_exclusion_rule(step, input_rule):
    print("Input Exclusion Rule " + input_rule)
    # extract required customers from the db and compare
    print("Exclusion Master Table Data :  "+ step  +" Selected rule : " + input_rule)
    
    table_name = rules_map.loc[rule, 'exclusion_table_nm']
    include_cond = rules_map.loc[rule, 'include_condition']
    exclude_cond = rules_map.loc[rule, 'exclude_condition']

# extracting exclusions from the input file 
exclusion_rules = input_df['exclusion_rule'].iloc[0]
# creating a dedicated list values
rules_list = exclusion_rules.split("|")

# call the apply rule fucntions to execute each exclusions 
for step, rule in enumerate(rules_list,start=1):
    apply_exclusion_rule(step,rule.strip())





# DND | Global_OptOut | Blacklisted | Email_Optout

