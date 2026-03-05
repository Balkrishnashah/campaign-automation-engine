
import pandas as pd
from config.db_config import engine
from config.campaign_config import seedlist_export_columns
import logging

logger = logging.getLogger(__name__)

logger.info("########## Liverun Module ##########")
def process_seedlist(input_df):
    # input_df = pd.read_csv("/Users/balkrishna/Documents/Projects/market_place_360/campaign/export/sms_2326.csv")
    # curr_df = input_df.copy()
    logger.info("Seedlist Execution Started")
    logger.info("Executing ... select customer_ref_no, first_name, last_name, email from cdm.seedlist_users")
    seedlist_df = pd.read_sql("""
        select customer_ref_no, first_name, last_name, email
        from cdm.seedlist_users
    """, engine)
    # we will build actual seedlist customer database. 
    logger.debug("Extracted Seed list Data  :\n " )
    logger.debug(seedlist_df.head())
    logger.debug('execution completed, Seedlist customer extracted ')
    try:
        for c in seedlist_export_columns:
            logger.info("Addition of Column "+ str(c)+" from the input campaign file")
            seedlist_df[c] = input_df[c].iloc[0]
    except Exception as e:
        raise(e)
        logger.info(f"Excepetion occured : {e}")
    logger.info("Final Seedlist Data of the campaign : \n")
    # logger.info(f"\n {seedlist_df.head()}")
    
    return seedlist_df
    
    
