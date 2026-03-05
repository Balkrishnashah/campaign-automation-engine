import pandas as pd
from config.db_config  import engine,target_schema
from datetime import datetime
import logging
logger = logging.getLogger(__name__)
logger.info("######### Initiating Exclusion Engine ##############")




'''
step1: read input, read exclusion rule
step2: create an order
step3: apply exclusion on each rule in one order
step4: create excluded records and valid records
step5: pass valid records from previous rule to next rule as input record
step6: combine all exclusion recrods.

'''




#  build final_list of excluded records


# def push_exclusion_to_(exl_df):
#     exclusion_recrods = pd.DataFrame()
#     exclusion_recrods = pd.DataFrame(columns=['customer_ref_no','exclusion_name','campaign_id','created_at'])
#     exclusion_recrods = pd.concat([exclusion_recrods, excluded_rows], ignore_index=True) 
#     return


# print(input_df.head())
def apply_exclusion_rule(step, input_rule,current_valid_df, rules_map):
    # logger.info("Input Exclusion Rule " + input_rule)
    # extract required customers from the db and compare
    # logger.info(f"Exclusion Master Table Data :  {step}  Selected rule : {input_rule}")
    
    try: 
        table_name = rules_map.loc[input_rule, 'table']
        schema_name = rules_map.loc[input_rule, 'schema']
        exclusion_rule = rules_map.loc[input_rule, 'exclusion_rule']
        exclusion_col = rules_map.loc[input_rule, 'column']
        excluded_rows = pd.DataFrame()
    
        temp_exp_table_nm = 'export_'+ datetime.now().strftime("%Y_%m_%d_%H_%M_%S") # replace with time once finalzied strftime("%Y_%m_%d_%H_%M_%S")
        logger.debug(f"Db Table pushed {temp_exp_table_nm}")
        temp_exp_table_df = current_valid_df['customer_ref_no']
        # insert Data back to DB as temp 
        temp_exp_table_df.to_sql(
            name=temp_exp_table_nm,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            schema="clm"
        )

        #excute to perform exclusion : extract excluded customers.
        query = f"""
        select cur.customer_ref_no
        from {target_schema}.{temp_exp_table_nm} cur
        where exists (
            select 1
            from {schema_name}.{table_name} cem
            where cem.customer_ref_no = cur.customer_ref_no
            and  {exclusion_rule}
        )
        """ 

        # this will store total excldued rows at first execution
        excluded_rows = pd.read_sql(query, engine)
        #filter Valid ids
        # excluded_ids = set(excluded_rows['customer_ref_no'])
        
        valid_df = pd.DataFrame()
        valid_df = current_valid_df[
            ~current_valid_df['customer_ref_no'].isin(excluded_rows['customer_ref_no'])
            ]

        input_count = len(current_valid_df)
        excluded_count = len(excluded_rows)
        valid_count = len(valid_df)
        
        # if actual input count is equal to valid and removed rows then its correct 
        if input_count == (excluded_count + valid_count):
            logger.info(f"counts matched post applying exclusion filter for:  {input_rule}")
            # add additional columns
            excluded_rows['exclusion_name'] = input_rule
            excluded_rows['campaign_id'] = valid_df['campaign_id'].iloc[0]
            excluded_rows['created_at'] = datetime.now()
        
            # if required then add below step to dorp the table manually
            # with engine.begin() as conn:
            #     conn.execute(f"""
            #     DROP TABLE {target_schema}.{temp_exp_table_nm}
            #     """)
            return valid_df, excluded_rows
            
        else:
            raise ValueError("Count mismatch  Something wrong in filtering logic")
            logger.error("Count mismatch , Something wrong in filtering logic")
        
        
    except Exception as e:
        logger.error(f"Exception Occurred while processing Exclsuion : {e}")
    

# input_df = pd.read_csv("/Users/balkrishna/Documents/Projects/market_place_360/campaign/export/email_2326.csv")
    

def execute_exclusion(input_df):
    
    try:
        # create a deep copy
        excl_start_df = input_df.copy()
        
        #load the exlcusion master rule exel
        exclusion_master_file = '/Users/balkrishna/Documents/Projects/market_place_360/campaign/config/exclusion_master.xlsx'
        exclusion_mstr_df = pd.read_excel(exclusion_master_file)
        # put a index on exclusion name 
        rules_map = exclusion_mstr_df.set_index('exclusion_name')
        logger.debug(exclusion_mstr_df.head())
        
        # define our final exclusion list 
        total_exclusion_records = pd.DataFrame(columns=['customer_ref_no','exclusion_name','campaign_id','created_at'])
        
        # extracting exclusions from the input file 
        exclusion_rules = input_df['exclusion_rule'].iloc[0]
        
        # creating a dedicated list values
        rules_list = exclusion_rules.split("|")
        
        # call the apply rule fucntions to execute each exclusions 
        for step, rule in enumerate(rules_list,start=1):
            # logger.info(f"Exclusion  {step} executed out of {len(rules_list)} : ")
            try:
                
                logger.info(f"Exclusion  {step} executed out of {len(rules_list)} : ")
                logger.info(f"Exclusion Executed for {rule.strip()} ")

                # Call to apply exclusion rule 
                valid_df,curr_exclusion_df = apply_exclusion_rule(step,rule.strip(), excl_start_df, rules_map)
                
                # concat all exclusion recrods in 1 df
                if (len(curr_exclusion_df) > 0) and step == 1:
                    total_exclusion_records = curr_exclusion_df.copy() 
                elif (len(curr_exclusion_df) > 0):
                    total_exclusion_records = pd.concat([total_exclusion_records, curr_exclusion_df], ignore_index=True)
            except Exception as e:
                raise(e)
                logger.error(e)
            logger.info(f"Current Rule exclusion rows {len(curr_exclusion_df)} ")
            logger.info(f"Current Rule valid rows     {len(valid_df)} ")
            logger.info(f"Toal  exclusion rows        {len(total_exclusion_records)} ")
            excl_start_df = valid_df.copy()
    
    except Exception as e:
        raise(e)
        logger.error(f"Exception occurred : {e}")
        
    return valid_df,total_exclusion_records
    
        

# DND | Global_OptOut | Blacklisted | Email_Optout -- abhik liye this one we will keep

