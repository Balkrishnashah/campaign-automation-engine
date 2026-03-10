import sys

from config.campaign_config import EXPORT_DIR,FAILED_EXPORT_DIR,PROCESSED_DIR,LOGS_DIR
import logging
import os 

from campaign_audit import *
from config.campaign_config import EXPORT_DIR,EXPORT_FORMAT_FILE_PATH,admin_email
from camp_utils import *

from modules.liverun import process_liverun
from modules.seedlist import process_seedlist
from email_alert import send_alert
from modules.export_validator import load_column_master,validate_columns

import pandas as pd
'''
Module Added: 
Campaign Audit: Log
move to Inprogress , Failed, Processed 


'''

# comm id
from communication_id_generator import CommunicationIDGenerator 

# define logger for the logs from this module
logger = logging.getLogger(__name__.upper())

def export_process(export_filename, filenm_shortnm, channel):
    export_filename_path = EXPORT_DIR / export_filename
    logger.info(f"Input file path : {export_filename_path}")
    if os.path.exists(export_filename_path):
        logger.info(f'\n###---- Execution Started for Following File ----###\n filename = {export_filename}\n Channel = {channel}')
        
        #insert into campaign audit table with START
        auditor = CampaignAuditor()
        try:
            #read the raw file
            raw_df = pd.read_csv(export_filename_path)
            input_count = len(raw_df)
                
            #Log Campaign file:
            job_id = auditor.start_campaign(
                script_name='forward_integration.py',
                filename=export_filename,
                input_count=input_count,
                input_channels=channel
            )
            
            logger.info(f'file already shared for logging with job id {job_id}')
            logger.info('Campaign file processing will start below')
            # move files to in progress folder. 
            # move_to_progress = move_to_inprogress(export_filename)
            
            if input_count > 0:
                #  Call to generate communication ID;
                logger.info(f"{export_filename}")
                
                
                
                #Validate Export file's Column Format
                schema_map = load_column_master(filepath=EXPORT_FORMAT_FILE_PATH)
                
                try:
                    format_result = validate_columns(raw_df=raw_df, 
                                                         schema_map=schema_map, 
                                                         channel=channel)
                except Exception as e:
                    logger.error(e)
                    
                
                if not format_result["status"]:
                    #call email alert module
                    error_message = (
                                        "Export Format Validation Failed, please refer below results <br>"
                                        "Missing Columns: " + str(format_result["missing"]) + "<br>"
                                        "Extra Columns: " + str(format_result["extra"])
                                    )
                    logger.error("Missing Columns: " + str(format_result["missing"]))
                    logger.error("Extra Columns: " + str(format_result["extra"]))
                    send_alert("failure", analyst_email=admin_email, error=error_message)
                    auditor.fail_campaign(error_message="Export Validation Failed")
                    sys.exit(1)
                    
                    
                #seedlist and liverun module
                # extract execution_type from the input csv 
                
                execution_type = raw_df['execution_type'].iloc[0].lower()
                
                process_df = pd.DataFrame()
                try : 
                    if execution_type == 'seedlist':
                        logger.info("Calling Seedlist Module ... ")
                        process_df = process_seedlist(raw_df)
                    else: 
                        logger.info("Calling Liverun Module ... ")
                        process_df = process_liverun(raw_df, channel,auditor)
                except Exception as e:
                    raise(e)
                    logger.error(f"Execution Type Error : {e}")        
                
                # log the final output dataset
                logger.debug('### Final Live run data received ####')
                logger.debug(f'Count of total records : {len(process_df)} Out of {len(raw_df)}')
                logger.debug(process_df.head())
                
                
                
                
                # attach communication id
                '''
                comm_gen = CommunicationIDGenerator()
                raw_df['communication_id'] = comm_gen.generateId(len(raw_df))
                logger.info(f"\n {raw_df.head()}")
                '''
                
                
                
                
                #Log file Failed
                # auditor.fail_campaign(error_message='file not proper')
                
                # move file to process folder
                # move_to_success = move_to_processed(export_filename)
                
                # send email module
                
                '''
                analyst_email = "balkrishna.11.shah@gmail.com"
                send_alert("success", analyst_email=analyst_email)
                send_alert("failure", analyst_email=analyst_email, error="Error Message")
                '''
                
            else:
                logger.error(e)
                auditor.fail_campaign(error_message='File is corrupted')
                # move_to_fail = move_to_failed(export_filename)
        except Exception as e:
            logger.info('failed at file reading')
            logger.error(e)
            auditor.fail_campaign(error_message=str(e))
        
        
    else:
        logger.error(f"Input File Missing {export_filename} stopping the job")
    execution_status = 'Failed'
    return execution_status

