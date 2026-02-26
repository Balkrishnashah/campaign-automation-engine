import logging
import pandas as pd



#Export Format File Path
EXPORT_FORMAT_FILE_PATH = "/Users/balkrishna/Documents/Projects/market_place_360/campaign/config/column_master.xlsx"

'''
Load master excel and extract all columns in dict 
which will be resued for each operation 
'''

# assign logger with its module name
logger = logging.getLogger(__name__)


def load_column_master(filepath):
    df = pd.read_excel(filepath)
    
    schema_map = {}
    
    for col in df.columns:
        schema_map[col] = (
            df[col]
            .dropna()
            .astype(str)
            .tolist()
            )
    
    return schema_map


def validate_columns(raw_df,schema_map,channel):
    format_col = channel + "_format"
    expected_cols = schema_map[format_col]
    actual_cols = raw_df.columns.to_list()
    logger.info(f"Expected Columns : {expected_cols}")
    logger.info(f"Actual Columns   : {actual_cols}")
    
    
    status = (expected_cols == actual_cols)
    
    missing = list(set(expected_cols) - set(actual_cols))
    extra = list(set(actual_cols) - set(expected_cols))

    if status:
        logger.info(f"{format_col} column validation PASSED")
    else:
        logger.error(f"{format_col} column validation FAILED")

    return {
        "status": status,
        "missing": missing,
        "extra": extra,
        "expected": expected_cols,
        "actual": actual_cols
    }
    
    

    


