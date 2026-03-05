import logging
import pandas as pd

# initiate the module logger
logger = logging.getLogger(__name__)



logger.info("########## Liverun Module ##########")
def process_liverun(input_df):
    try: 
        liverun_df = pd.DataFrame()
    except Exception as e:
        logger.error(e)
    
    # Call the exclusion Module
    # step1 : extract exclusion list
    
    return liverun_df