import logging
import pandas as pd
from modules.exclusion import execute_exclusion

# initiate the module logger
logger = logging.getLogger(__name__)
logger.info("########## Liverun Module ##########")


def process_liverun(input_df):
    try: 
        liverun_df = pd.DataFrame()
        
        # Call exclusion process 
        liverun_df,total_exclusion_records = execute_exclusion(input_df)
        
        try: 
            if len(liverun_df) > 0:
                logger.info("Exclusion Executed completely ")
                logger.debug(f"Final Valid Record count : {len(liverun_df)}")
                logger.debug(f"Final Exclusion Record count : {len(total_exclusion_records)}")
                
                #Call contact Policy Capping  Module
                
                
                #Call Contact Matching Module
                
                
                
                # Channel Check
                
                    # Call Channel module to execute campaign
                
                
                
        except Exception as e:
            raise(e)
            logger.error('No Valid record count found')
    except Exception as e:
        logger.error(e)
    
    # Call the exclusion Module
    # step1 : extract exclusion list
    
    return liverun_df