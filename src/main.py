import os
import sys
import time
import logging

# Ensure the src directory is in the system path for imports
sys.path.append(os.path.join(os.path.dirname(__file__)))

from extract import extract_csv
from transform import run_transformation
from load import load_to_postgres

# Configure central logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('pipeline_execution.log') # Keeps a physical log file
    ]
)
logger = logging.getLogger('PipelineOrchestrator')

def main():

    logger.info("INITIALIZING ETL PIPELINE: ICETEX ODS COLOMBIA")

    
    start_time = time.time()
    
    # Define paths dynamically
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Créditos_Otorgados._20260304.csv')
    
    try:
        # Phase 1: Extract
        logger.info(">>> PHASE 1: EXTRACT <<<")
        raw_df = extract_csv(RAW_DATA_PATH)
        
        # Phase 2: Transform
        logger.info(">>> PHASE 2: TRANSFORM <<<")
        transformed_data = run_transformation(raw_df)
        
        # Phase 3: Load
        logger.info(">>> PHASE 3: LOAD <<<")
        load_to_postgres(transformed_data)
        
        end_time = time.time()
        execution_time = round(end_time - start_time, 2)
        
        logger.info("===================================================")
        logger.info(f"PIPELINE COMPLETED SUCCESSFULLY IN {execution_time} SECONDS.")
        logger.info("===================================================")
        
    except Exception as e:
        logger.error(f"PIPELINE FAILED: A critical error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()