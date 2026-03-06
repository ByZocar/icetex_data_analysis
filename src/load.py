import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Database connection parameters 
DB_USER = 'postgres'
DB_PASSWORD = 'WHITE5807' 
DB_HOST = 'localhost'
DB_PORT = '5432'
DB_NAME = 'icetex_ods_dw'

DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

def clear_existing_data(engine):
    """
    Truncates tables to ensure the script is idempotent (can be run multiple times safely).
    CASCADE ensures the fact table is cleared before dimensions.
    """
    logging.info("Clearing existing data from Data Warehouse to prevent duplicates...")
    with engine.begin() as conn:
        # Truncating dimensions with CASCADE will automatically truncate the fact table
        conn.execute(text("TRUNCATE TABLE dim_period CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_geography CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_program CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_student_profile CASCADE;"))
    logging.info("Database truncated successfully.")

def load_to_postgres(transformed_data: dict):
    """
    Loads transformed dataframes into PostgreSQL.
    Order is critical: Dimensions first, then Fact table to maintain Referential Integrity.
    """
    logging.info("Initiating load process to PostgreSQL...")
    engine = create_engine(DATABASE_URI)
    
    try:
        # 1. Clear old data for idempotency
        clear_existing_data(engine)
        
        # 2. Define the strict loading order
        load_order = [
            ('dim_period', transformed_data['dim_period']),
            ('dim_geography', transformed_data['dim_geography']),
            ('dim_program', transformed_data['dim_program']),
            ('dim_student_profile', transformed_data['dim_student_profile']),
            ('fact_credits', transformed_data['fact_credits'])
        ]
        
        # 3. Execute bulk inserts
        for table_name, df in load_order:
            logging.info(f"Loading table: {table_name} ({len(df)} rows)...")
            
            # Use if_exists='append' because the tables are already created with proper schemas/constraints
            df.to_sql(
                name=table_name,
                con=engine,
                if_exists='append',
                index=False,
                method='multi', # Optimizes bulk inserts
                chunksize=10000 # Prevents memory overload
            )
            logging.info(f"Successfully loaded {table_name}.")
            
        logging.info("ETL Pipeline completed successfully. Data Warehouse is populated.")
        
    except Exception as e:
        logging.error(f"Failed to load data into database: {e}")
        raise
    finally:
        engine.dispose()

if __name__ == "__main__":
    # Integration Test: Run the full pipeline
    import sys
    import os
    
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from extract import extract_csv
    from transform import run_transformation
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Créditos_Otorgados._20260304.csv')
    
    try:
        # Step 1: Extract
        logging.info("--- PHASE 1: EXTRACT ---")
        raw_data = extract_csv(RAW_DATA_PATH)
        
        # Step 2: Transform
        logging.info("--- PHASE 2: TRANSFORM ---")
        transformed_data = run_transformation(raw_data)
        
        # Step 3: Load
        logging.info("--- PHASE 3: LOAD ---")
        load_to_postgres(transformed_data)
        
    except Exception as e:
        logging.error(f"Pipeline execution halted due to error: {e}")