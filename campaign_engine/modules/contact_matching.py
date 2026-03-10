from datetime import datetime
from config.db_config import engine, target_schema
import pandas as pd

import logging
logger = logging.getLogger(__name__)

def execute_contact_matching(input_df, input_channel):
    
    valid_df = pd.DataFrame()
    current_valid_df = input_df.copy()
    
    logger.debug(f'current valid df : {current_valid_df.head()}')
    
    
    try: 
    
        logger.info('initating contact matching ')
        if input_channel.lower() == 'sms':
            contact_col = 'phone'
        else:
            contact_col = 'email'
            
        logger.info('extracting column. ' + contact_col)
        
        
        
        
        temp_con_table_nm = 'conmatch_'+ datetime.now().strftime("%Y_%m_%d") # replace with time once finalzied strftime("%Y_%m_%d_%H_%M_%S")
       
        
        temp_con_table_df = current_valid_df['customer_ref_no']
        
        logger.debug(f'count of temp con table df {len(temp_con_table_df)}')
        # insert Data back to DB as temp 
        temp_con_table_df.to_sql(
            name=temp_con_table_nm,
            con=engine,
            if_exists="replace",
            index=False,
            method="multi",
            schema=target_schema
            )
        logger.debug(f"Db Table pushed {temp_con_table_nm}")
        
        logger.debug(input_df.head())
        try:
            curr_contact_source = input_df['contact_source'].iloc[0]
            logger.debug(f'Current contact source. : {curr_contact_source}')
        except Exception as e:
            logger.debug(f'unable to extract contact source from input csv fiel : {e}')
        
        
        query = f""" 
            SELECT a.*, 
                ab.{contact_col} AS EMAIL
            FROM {target_schema}.{temp_con_table_nm} a
            INNER JOIN 
            ( select customer_ref_no, {contact_col} 
            from cdm.customer_contact_source 
            WHERE source_type = '{curr_contact_source}') as ab
            ON a.customer_ref_no = ab.customer_ref_no
            """

        try:
            # execute db query
            valid_df = pd.read_sql(query,engine)
            # valid_df.to_csv("/Users/balkrishna/Documents/Projects/market_place_360/campaign/export/con_email_2326.csv", index=False)
            # input_df.to_csv("/Users/balkrishna/Documents/Projects/market_place_360/campaign/export/con_email__source_2326.csv", index=False)
        except Exception as e:
            logger.error(f"Contact matching : {e}")
        
        if len(valid_df) == len(input_df):
            logger.debug(f'count before merge : {len(valid_df)}, Input file :  {len(input_df)}')
        
            logger.debug(valid_df.head())
            valid_df = valid_df.merge(input_df.drop(columns=['email']), on="customer_ref_no",how='inner')
            # valid_df = valid_df.reset_index(drop=True)
            logger.debug(f'Merge Output : \n{valid_df.head()}')
            logger.debug(f'count after merge : {len(valid_df)}')
            logger.debug(f'count of columns  : {valid_df.shape[1]}')
            
            return valid_df
    except Exception as e :
        logger.debug(f'error {e}')
        
    return valid_df