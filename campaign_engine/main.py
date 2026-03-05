# print("hello world")

#Module wise import
#Module1
from config.campaign_config import EXPORT_DIR,LOGS_CREATE_FLAG,log_file_nm
from export_process import export_process
import logging
from datetime import datetime

# start the logging
logger = logging.getLogger(__name__)
logger.info(f"\n\nExecution Started of forward integration at: {datetime.now()} ")


'''
Description : 

This Module will read the files from export directory and call the campaign_process function to execute
the campaign file.
check performed by this module
'''


logger.info("Module_no : 01")
logger.info("Module_Name : Read_Export_DIR\n")
 



#reading directories to check new  csv files
# sorted(EXPORT_DIR.glob("*.csv"), key=lambda f: f.stat().st_mtime):
csv_files = list(sorted(EXPORT_DIR.glob('*.csv'), key=lambda f: f.stat().st_mtime))
logger.info(f'Total Number of CSV Files found: {len(csv_files)}\n')
for i in range(len(csv_files)):
    # print(csv_files[i].name)
    logger.info(f'File read at index {i} : \n ')
    if csv_files[i].suffix.lower() == '.csv':
        execution_stat = export_process(export_filename=csv_files[i].name, filenm_shortnm=csv_files[i].stem, channel=csv_files[i].stem.split('_')[0])
    logger.info("#------ End of Forward Integration Process ------#")