
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
        con_stage1_df = pd.read_sql(query,engine)
    except Exception as e:
        logger.error(f"contact policy DB insertion failed : {e}")
    
    logger.debug(con_stage1_df.head())
    
    # calcualte contact policy flag 
    con_stage1_df['CP_flag'] = np.select(
        [ con_stage1_df[f'{contact_policy_cols}'] <=0, con_stage1_df['max_cap'] <=0 ],
        ['CC','CO'],
        default='ELIGIBLE'
    )
    
    # extract customer who failed contact policy rules
    con_stage2_df = con_stage1_df[
        con_stage1_df['CP_flag'] != 'ELIGIBLE'
    ]
    logger.info('before eligible')
    logger.debug(con_stage1_df.head())
    logger.debug(len(con_stage1_df))
    
    
    logger.info(f'Total Contact Policy Records :          {len(con_stage1_df)}')
    
    # remove invalid customers from valid list
    con_stage1_df = con_stage1_df[
    ~con_stage1_df['customer_ref_no'].isin(con_stage2_df['customer_ref_no'])
        ]
    
    logger.info(f'Total Eligible Contact Policy Records : {len(con_stage1_df)}')
    logger.info(f'Total Ineligible Contact Policy Records : {len(con_stage2_df)}')
    
    logger.info(f" {len(current_valid_df)} == ({len(con_stage1_df) } + {len(con_stage2_df)} )")
    if len(current_valid_df) == (len(con_stage1_df) + len(con_stage2_df) ):
        logger.info('count matched')
        with engine.begin() as conn:
                query = f"DROP TABLE IF EXISTS {target_schema}.{temp_con_table_nm}"
                conn.execute(text(query))
    
    
    
    return con_stage1_df, con_stage2_df