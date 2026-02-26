import pandas as pd
from datetime import datetime
import os
from campaign_config import PARENT_DIR
from pathlib import Path

import logging
logger = logging.getLogger(__name__)

import uuid



class CampaignAuditor:
    
    def __init__(self, audit_file=f'{PARENT_DIR}/campaign_scripts/DB_files/campaign_audit.csv'):
        self.audit_file = Path(audit_file)
        self.job_id =str(uuid.uuid4())
        self.start_time = datetime.now()
        
        folder = os.path.dirname(self.audit_file)
        if folder:  # Only if there's a folder in the path
            os.makedirs(folder, exist_ok=True)
            logger.info(f" Created folder: {folder}")
            
        # ✅ Create folder        
        self.audit_file.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Folder ready: {self.audit_file.parent}")
        
        # ✅ Create file with header if not exists
        if not self.audit_file.exists():
            header_cols = [
                "id","script_name","filename","status","start_time","end_time",
                "execution_time_seconds","input_record_count",
                "output_record_count","channels","error_message"
            ]

            pd.DataFrame(columns=header_cols).to_csv(self.audit_file, index=False)

            logger.info(f"Audit file created with header: {self.audit_file}")
    

    
        
    def start_campaign(self, script_name, filename, input_count, input_channels):
        #Log campaign start 
        self.job_id = self.job_id
        # self.start_time = datetime.now()
        
        entry = pd.DataFrame([{
            'id':self.job_id,
            'script_name':script_name,
            'filename':filename,
            'status':'START',
            'start_time':self.start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time':'',
            'execution_time_seconds':'',
            'input_record_count':input_count,
            'output_record_count':'',
            'channels':input_channels,
            'error_message':''
        }])
        
        entry.to_csv(self.audit_file, mode='a', index=False)
        
        logger.info(f'Campaign Started - Job ID:{self.job_id}')
        logger.info(f'   Input Records:{input_count}')
        logger.info(f'   Channels:{input_channels}')
        logger.info(f'   Start time:{self.start_time}')
        
        
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
        
        logger.info(f"   Campaign Completed - Job ID: {self.job_id}")
        logger.info(f"   Output Records: {output_count}")
        logger.info(f"   Execution Time: {execution_time:.2f} seconds")
        logger.info(f"   End Time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
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
        
        
    def _update_audit_record(self, status, end_time, execution_time, 
                            output_count, error_message):
        """Update the audit record with final details"""
        # Read entire audit file
        audit_df = pd.read_csv(self.audit_file)
        
        # Update the row for this job_id
        mask = audit_df['id'] == self.job_id
        audit_df.loc[mask, 'status'] = status
        audit_df.loc[mask, 'end_time'] = end_time.strftime('%Y-%m-%d %H:%M:%S')
        audit_df.loc[mask, 'execution_time_seconds'] = execution_time
        audit_df.loc[mask, 'output_record_count'] = output_count
        audit_df.loc[mask, 'error_message'] = error_message
        
        # Write back entire file
        audit_df.to_csv(self.audit_file, index=False)