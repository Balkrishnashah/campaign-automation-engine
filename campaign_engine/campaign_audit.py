import pandas as pd
from datetime import datetime
import os
from campaign_config import PARENT_DIR
from pathlib import Path

import logging
logger = logging.getLogger(__name__)

import uuid
from db_config import conn


class CampaignAuditor:
    
    def __init__(self):
        # self.audit_file = Path(audit_file)
        self.job_id =str(uuid.uuid4())
        self.start_time = datetime.now()
        

    
        
    def start_campaign(self, script_name, filename, input_count, input_channels):
        #Log campaign start 
        self.job_id = self.job_id
        # self.start_time = datetime.now()
        
        logger.info(f'\tCampaign Started - Job ID:{self.job_id}')
        logger.info(f'\tInput Records:{input_count}')
        logger.info(f'\tChannels:{input_channels}')
        logger.info(f'\tStart time:{self.start_time}')
        
        try:
            curr = conn.cursor()
            
            curr.execute("""
                    INSERT INTO clm.campaign_audit_log
                    (id,script_name,filename, status,start_time,end_time,execution_time_seconds,input_record_count,output_record_count,channels,error_message)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                    self.job_id,
                    script_name,
                    filename,
                    'START',
                    self.start_time,
                    None,
                    None,
                    input_count,
                    None,
                    input_channels,
                    None
                    ))
            logger.info('Insertion completed in the db table')
            conn.commit()
        except Exception as e:
            logger.error(f"Inseriton Failed : {e}")
        
        return self.job_id
    
    
    
    def complete_campaign(self, output_count):
        """Log campaign completion"""
        if not self.job_id or not self.start_time:
            raise ValueError("Must call start_campaign first")
        
        end_time = datetime.now()
        execution_time = (end_time - self.start_time).total_seconds()
        
        # Update the record for this job_id
        self._update_audit_record(
            status='COMPLETE',
            end_time=end_time,
            execution_time=execution_time,
            output_count=output_count,
            error_message=None
        )
        
        logger.info(f"\tCampaign Completed - Job ID: {self.job_id}")
        logger.info(f"\tOutput Records: {output_count}")
        logger.info(f"\tExecution Time: {execution_time:.2f} seconds")
        logger.info(f"\tEnd Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
    def fail_campaign(self, error_message):
        """Log campaign failure"""
        if not self.job_id or not self.start_time:
            raise ValueError("Must call start_campaign first")
        
        end_time = datetime.now()
        execution_time = (end_time - self.start_time).total_seconds()
        
        self._update_audit_record(
            status='failed',
            end_time=end_time,
            execution_time=execution_time,
            output_count=0,
            error_message=error_message
        )
        
        logger.info(f"\tCampaign Failed - Job ID: {self.job_id}")
        logger.info(f"\tOutput Records: {0}")
        logger.info(f"\tExecution Time: {execution_time:.2f} seconds")
        logger.info(f"\tEnd Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        
        
        
    def _update_audit_record(self, status, end_time, execution_time, 
                            output_count, error_message):
        """Update the audit record with final details"""
        
        #Write to DB
        try:
            curr = conn.cursor()
            
            curr.execute("""
                        UPDATE clm.campaign_audit_log
                        SET
                            status = %s,
                            end_time = %s,
                            execution_time_seconds = %s,
                            output_record_count = %s,
                            error_message = %s
                        WHERE id = %s
                    """,
                    (
                        status,
                        end_time,
                        execution_time,
                        output_count,
                        error_message,
                        self.job_id
                    )
                )

           
            logger.info('   Log Closure Updated in the db table')
            conn.commit()
        except Exception as e:
            logger.error(f"Inseriton Failed : {e}")