import logging
import pandas as pd
from modules.exclusion import execute_exclusion

# initiate the module logger
logger = logging.getLogger(__name__)
logger.info("########## Liverun Module ##########")


def process_liverun(input_df,input_channel):
    try: 
        liverun_df = pd.DataFrame()
        
        # Call exclusion process 
        liverun_df,total_exclusion_records = execute_exclusion(input_df)
        
        try: 
            if len(liverun_df) > 0:
                logger.info("Exclusion Executed completely ")
                logger.debug(f"Final Valid Record count : {len(liverun_df)}")
                logger.debug(f"Final Exclusion Record count : {len(total_exclusion_records)}")
                
                
                # Channel Check
                
                #Call contact Policy Capping  Module
                ''' step1 : final dataset from exclsuion is ready
                step2 : identify channel
                step3 : put customer ids to DB
                step4 : perform inner join on contact policy table and extract channel column in python
                step5 : calculate contact policy flags
                        CC flags : if channel counter <=0 then 'CC'
                        CO flag : if overall counter <=0 then 'CO'
                        if both are null then put 'ELIGIBLE' else flag code ==> contact_policy_flag
                step6 : insert those flag code who are not eligible into a error table.
                step7 : pass the valid records DF to another next step
                
                contact policy update :
                step8 : build contact policy update method for the these custmers
                step9 : update channel counter and overall counter by reducing 1, and update datetime
                '''
                    
                    
                    
                #Call Contact Matching Module
                    
                '''
                step1 : push liverun df post contact policy into db
                step2 : extract the contact details based on channel type and contact source
                step3 : store output in python.
                
                '''

                
                # Call Channel module to execute campaign
                '''
                step1 : check Execution is it API based or SFTP based
                step2 : if API based build json payload
                step3 : call the rest API to send data to channel 
                step4 : if SFTP based then build a csv file
                step4 : call the Rest APIs to move files to SFTP server
                step5 : store final output in following tables and create their data.
                        1. sms_communication_hist
                        2. email_communication_hist
                        3. campaign_hist
                        4. exclusion_records [ data from exclusions + contact policy]
                step6 : end campaign audit
                step7 : send alert with summary of the campaign [beautifully design]
                        1. input file
                        2. input count
                        3. output count
                        4. channel 
                        5. execution message
                '''

        except Exception as e:
            raise(e)
            logger.error('No Valid record count found')
    except Exception as e:
        logger.error(e)
    
    # Call the exclusion Module
    # step1 : extract exclusion list
    
    return liverun_df