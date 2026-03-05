from pathlib import Path
from datetime import datetime
import logging

PARENT_DIR = '/Users/balkrishna/Documents/Projects/market_place_360'

BASE_DIR = Path("/Users/balkrishna/Documents/Projects/market_place_360/campaign")
EXPORT_DIR = BASE_DIR / "export"
FAILED_EXPORT_DIR = BASE_DIR / "processed_failed"
PROCESSED_DIR = BASE_DIR / "processed"
LOGS_DIR = BASE_DIR / "logs"
LOGS_CREATE_FLAG = 'yes'
INPROGRESS_DIR = BASE_DIR / "inprogress_campaign"


#Export Format File Path
EXPORT_FORMAT_FILE_PATH = "/Users/balkrishna/Documents/Projects/market_place_360/campaign/config/column_master.xlsx"
admin_email = "balkrishna.11.shah@gmail.com"

# log_file_nm = LOGS_DIR / "forward_integration.log"
# log file with datetime
log_file_nm = LOGS_DIR / f"forward_integration.log" # Renamed the log file to this for every excution log ==> forward_integration_{datetime.now():%y%m%d}.log


   
# if LOGS_CREATE_FLAG.upper() == 'YES':
#      logging.basicConfig(
#         filename=log_file_nm,
#         filemode="w",
#         level=logging.INFO,
#         format=" %(asctime)s | %(name)s  | %(levelname)s | %(message)s" #format="%(asctime)s | %(levelname)s | %(message)s"
#     )
# else:
#     # disable the log printing
#     logging.disable(logging.CRITICAL)
    
if LOGS_CREATE_FLAG.upper() == 'YES':
    handlers = [
        logging.FileHandler(log_file_nm, mode='w'),
        # Turning off the console log
        # logging.StreamHandler()  
    ]
else:
    handlers = [logging.StreamHandler()]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)
  
# Additional Column list from the actual input file format which will be added in the seedlist file 

  
seedlist_export_columns = ['campaign_id','segment_id','execution_type',
              'campaign_execution_count','template_id',
              'product_pitched']

