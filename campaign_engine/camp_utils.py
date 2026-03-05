#this file keeps the resuable fucntions
import shutil
import os 
from datetime import datetime

from campaign_config import EXPORT_DIR,PROCESSED_DIR,FAILED_EXPORT_DIR,INPROGRESS_DIR

import logging
logger = logging.getLogger(__name__)

# move files to inprogress
def move_to_inprogress(input_filename):
    src_filepath =str(EXPORT_DIR) + "/" + input_filename
    mv_filepath = str(INPROGRESS_DIR)+"/"+ input_filename #"_"+datetime.now().strftime("%Y%m%d_%H:%M:%S")
    try:
        shutil.move(src=src_filepath, dst=mv_filepath)
        logger.info(f"Moved {src_filepath} -> {mv_filepath}")
        return 'success'
    except Exception as e:
        logger.info(f"Moved Failed for {src_filepath}:  {e}")
        return 'error'
        

# move files to failed dir
def move_to_failed(input_filename):
    src_filepath =str(INPROGRESS_DIR) + "/" + input_filename
    mv_filepath = str(FAILED_EXPORT_DIR)+"/"+ input_filename +"_"+datetime.now().strftime("%Y%m%d_%H:%M:%S")
    try:
        shutil.move(src=src_filepath, dst=mv_filepath)
        logger.info(f"Moved {src_filepath} -> {mv_filepath}")
        return 'success'
    except Exception as e:
        logger.info(f"Moved Failed for {src_filepath}:  {e}")
        return 'error'
        

# move files to processed dir
def move_to_processed(input_filename):
    src_filepath =str(INPROGRESS_DIR) + "/" + input_filename
    mv_filepath = str(PROCESSED_DIR)+"/"+ input_filename +"_"+datetime.now().strftime("%Y%m%d_%H:%M:%S")
    print(src_filepath, "\n", mv_filepath)
    try:
        shutil.move(src=src_filepath, dst=mv_filepath)
        logger.info(f"Moved {src_filepath} -> {mv_filepath}")
        return 'success'
    except Exception as e:
        logger.error(f"Moved Failed for {src_filepath}:  {e}")
        return 'error'
        


