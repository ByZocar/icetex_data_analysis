import pandas as pd
import os
import logging

# Configure logging for production-grade output
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_csv(file_path: str) -> pd.DataFrame:
    """
    Reads the raw CSV file and loads it into a pandas DataFrame.
    Validates file existence and handles initial parsing.
    """
    if not os.path.exists(file_path):
        logging.error(f"Source file not found at: {file_path}")
        raise FileNotFoundError(f"Source file not found at: {file_path}")
        
    logging.info(f"Starting extraction from: {file_path}")
    
    try:
        # Load data. We use thousands=',' to fix the "2,015" issue right at ingestion.
        # We enforce VIGENCIA as string first to avoid floating point errors before Transform phase.
        df = pd.read_csv(file_path, thousands=',', dtype={'VIGENCIA': str})
        
        logging.info(f"Extraction successful. Rows: {df.shape[0]}, Columns: {df.shape[1]}")
        return df
        
    except Exception as e:
        logging.error(f"Error during extraction: {e}")
        raise

if __name__ == "__main__":
    # Test execution block
    # Dynamically resolve the path based on your project structure
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Créditos_Otorgados._20260304.csv')
    
    # Run the extraction
    raw_df = extract_csv(RAW_DATA_PATH)
    print(raw_df.head(2))