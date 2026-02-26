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
log_file_nm = LOGS_DIR / f"forward_integration_{datetime.now():%y%m%d}.log"


   
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
        logging.StreamHandler()
    ]
else:
    handlers = [logging.StreamHandler()]

logging.basicConfig(level=logging.DEBUG, handlers=handlers)
  
