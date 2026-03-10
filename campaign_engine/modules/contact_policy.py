
from datetime import datetime
from config.db_config import engine,target_schema
import pandas as pd
import numpy as np
from sqlalchemy import text
import logging
logger = logging.getLogger(__name__.upper())



#Call contact Policy Capping  Module
''' step1 : final dataset from exclsuion is ready
step2 : identify channel
step3 : put customer ids to DB
step4 : perform inner join on contact policy table and extract required channel column in python
step5 : calculate contact policy flags
        CC flags : if channel counter <=0 then 'CC'
        CO flag : if overall counter <=0 then 'CO'
        if both are null then put 'ELIGIBLE' else flag code ==> contact_policy_flag
step6 : insert those flag code who are not eligible into a error table.
step7 : pass the valid records DF to another next step

contact policy update :
step8 : build contact policy update method for the these custmers
step9 : update channel counter and overall counter by reducing 1, and update datetime
'''
                


def update_contact_policy(input_df, input_channel_nm):
    status ='success'
    contact_policy_cols = input_channel_nm +"_cap"
    temp_con_table_nm = 'contact_'+ datetime.now().strftime("%Y_%m_%d") # replace with time once finalzied strftime("%Y_%m_%d_%H_%M_%S")
    logger.debug(f"Db Table pushed {temp_con_table_nm}")
    temp_con_table_df = input_df['customer_ref_no']
    # insert Data back to DB as temp 
    try:
        temp_con_table_df.to_sql(
        name=temp_con_table_nm,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        schema=target_schema
        )
    except Exception as e:
         logger.error(f"contact policy update DB Insertion failed : {e}")
    
    query = f""" 
    update cdm.customer_contact_policy a
    set {contact_policy_cols} = {contact_policy_cols} - 1
    where exists (
        select 1 from {target_schema}.{temp_con_table_nm} b
        where a.customer_ref_no = b.customer_ref_no
    )
    """
    
    try:
        # execute db query 
        with engine.begin() as conn:
                conn.execute(query)
    except Exception as e:
        logger.error(f"contact policy DB Update failed : {e}")
    
    return status
                
def execute_contact_policy(input_df, input_channel_nm):
    
    logger.info('Inside Contact Policy Method')
    
    contact_policy_cols = input_channel_nm +"_cap"
    logger.debug(f"extracting contact policy col name : {contact_policy_cols}")
    current_valid_df = input_df.copy()
    
    temp_con_table_nm = 'contact_'+ datetime.now().strftime("%Y_%m_%d") # replace with time once finalzied strftime("%Y_%m_%d_%H_%M_%S")
    logger.debug(f"Db Table pushed {temp_con_table_nm}")
    temp_con_table_df = current_valid_df['customer_ref_no']
    # insert Data back to DB as temp 
    temp_con_table_df.to_sql(
        name=temp_con_table_nm,
        con=engine,
        if_exists="replace",
        index=False,
        method="multi",
        schema=target_schema
        )
    
    
    query = f""" 
    select a.*,b.{contact_policy_cols},b.max_cap
    from {target_schema}.{temp_con_table_nm} a
    inner join cdm.customer_contact_policy b
    on a.customer_ref_no = b.customer_ref_no
    """
    
    try:
        # execute db query 
        valid_df = pd.read_sql(query,engine)
    except Exception as e:
        logger.error(f"contact policy DB insertion failed : {e}")
    
    
    logger.debug(f'Output of the contact policy join : {valid_df.head()}')
    
    # calcualte contact policy flag 
    valid_df['CP_flag'] = np.select(
        [ valid_df[f'{contact_policy_cols}'] <=0, valid_df['max_cap'] <=0 ],
        ['CC','CO'],
        default='ELIGIBLE'
    )
    
    # extract customer who failed contact policy rules
    invalid_df = valid_df[
        valid_df['CP_flag'] != 'ELIGIBLE'
    ]
    logger.info('before eligible')
    logger.debug(valid_df.head())
    logger.debug(len(valid_df))
    
    
    logger.info(f'Total Contact Policy Records :          {len(valid_df)}')
    
    # remove invalid customers from valid list
    valid_df = valid_df[
    ~valid_df['customer_ref_no'].isin(invalid_df['customer_ref_no'])
        ]
    
    logger.info(f'Total Eligible Contact Policy Records : {len(valid_df)}')
    logger.info(f'Total Ineligible Contact Policy Records : {len(invalid_df)}')
    
    logger.info(f" {len(current_valid_df)} == ({len(valid_df) } + {len(invalid_df)} )")
    if len(current_valid_df) == (len(valid_df) + len(invalid_df) ):
        logger.info('count matched')
        logger.debug(f'count before merge : {len(valid_df)}')
        valid_df = valid_df.merge(current_valid_df, how='left')
        valid_df = valid_df.drop(columns=['email_cap', 'max_cap'] , axis=1)
        logger.debug(f'Merge Output : \n{valid_df.head()}')
        logger.debug(f'count after merge : {len(valid_df)}')
        logger.debug(f'count of columns  : {valid_df.shape[1]}')
       
        
        
        with engine.begin() as conn:
                query = f"DROP TABLE IF EXISTS {target_schema}.{temp_con_table_nm}"
                conn.execute(text(query))
    
    
    
    return valid_df, invalid_df