import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_and_standardize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans raw data: handles nulls, standardizes text, and corrects data types.
    """
    logging.info("Starting data cleaning and standardization...")
    df_clean = df.copy()
    
    # 1. Strip whitespace from column names just in case
    df_clean.columns = df_clean.columns.str.strip()
    
    # 2. Handle missing values based on EDA findings
    # Filling nulls in SECTOR IES with 'NO CLASIFICADO'
    df_clean['SECTOR IES'] = df_clean['SECTOR IES'].fillna('NO CLASIFICADO')
    
    # 3. Standardize text columns (uppercase, strip whitespaces)
    text_cols = df_clean.select_dtypes(include=['object']).columns
    for col in text_cols:
        df_clean[col] = df_clean[col].astype(str).str.strip().str.upper()
        
    # 4. Type Casting: Fix the VIGENCIA column (remove commas, cast to int)
    # E.g., "2,015" -> "2015" -> 2015
    df_clean['VIGENCIA'] = df_clean['VIGENCIA'].str.replace(',', '', regex=False).astype(int)
    
    logging.info("Data cleaning completed.")
    return df_clean

def generate_dimensions(df: pd.DataFrame) -> dict:
    """
    Extracts unique records for each dimension and generates Surrogate Keys (SKs).
    """
    logging.info("Generating dimensions and Surrogate Keys...")
    
    # Dim Period
    dim_period = df[['VIGENCIA', 'PERIODO OTORGAMIENTO']].drop_duplicates().reset_index(drop=True)
    dim_period.insert(0, 'sk_period', dim_period.index + 1)
    
    # Dim Geography
    # Note: Using the exact typo from the source data 'CÓDIGO DEDEPARTAMENTO DE ORIGEN'
    dim_geography = df[['CÓDIGO DEDEPARTAMENTO DE ORIGEN', 'DEPARTAMENTO DE ORIGEN', 'CATEGORÍA DEL MUNICIPIO DE ORIGEN']].drop_duplicates().reset_index(drop=True)
    dim_geography.insert(0, 'sk_geography', dim_geography.index + 1)
    
    # Dim Program
    dim_program = df[['SECTOR IES', 'NIVEL DE FORMACIÓN', 'MODALIDAD DE LÍNEA', 'MODALIDAD DEL CRÉDITO']].drop_duplicates().reset_index(drop=True)
    dim_program.insert(0, 'sk_program', dim_program.index + 1)
    
    # Dim Student Profile
    dim_student = df[['SEXO AL NACER', 'ESTRATO SOCIOECONÓMICO']].drop_duplicates().reset_index(drop=True)
    dim_student.insert(0, 'sk_student_profile', dim_student.index + 1)
    
    logging.info(f"Dimensions created. Period: {len(dim_period)}, Geo: {len(dim_geography)}, Prog: {len(dim_program)}, Student: {len(dim_student)}")
    
    return {
        'period': dim_period,
        'geography': dim_geography,
        'program': dim_program,
        'student': dim_student
    }

def generate_fact_table(df_clean: pd.DataFrame, dimensions: dict) -> pd.DataFrame:
    """
    Maps Surrogate Keys back to the main dataframe to create the Fact Table.
    """
    logging.info("Mapping Surrogate Keys to create Fact Table...")
    
    fact_df = df_clean.copy()
    
    # Merge Period SK
    fact_df = fact_df.merge(dimensions['period'], on=['VIGENCIA', 'PERIODO OTORGAMIENTO'], how='left')
    
    # Merge Geography SK
    fact_df = fact_df.merge(dimensions['geography'], on=['CÓDIGO DEDEPARTAMENTO DE ORIGEN', 'DEPARTAMENTO DE ORIGEN', 'CATEGORÍA DEL MUNICIPIO DE ORIGEN'], how='left')
    
    # Merge Program SK
    fact_df = fact_df.merge(dimensions['program'], on=['SECTOR IES', 'NIVEL DE FORMACIÓN', 'MODALIDAD DE LÍNEA', 'MODALIDAD DEL CRÉDITO'], how='left')
    
    # Merge Student Profile SK
    fact_df = fact_df.merge(dimensions['student'], on=['SEXO AL NACER', 'ESTRATO SOCIOECONÓMICO'], how='left')
    
    # Select only the SKs, the required textual attribute, and the measure
    fact_cols = [
        'sk_period', 
        'sk_geography', 
        'sk_program', 
        'sk_student_profile', 
        'RANGO DEL VALOR TOTAL DESEMBOLSADO', 
        'NÚMERO DE NUEVOS BENEFICIARIOS DE CRÉDITO'
    ]
    
    fact_table = fact_df[fact_cols].copy()
    
    # Rename columns to match the SQL database schema exactly
    fact_table.rename(columns={
        'RANGO DEL VALOR TOTAL DESEMBOLSADO': 'rango_valor_desembolsado',
        'NÚMERO DE NUEVOS BENEFICIARIOS DE CRÉDITO': 'total_nuevos_beneficiarios'
    }, inplace=True)
    
    logging.info(f"Fact table created with {len(fact_table)} records.")
    return fact_table

def run_transformation(raw_df: pd.DataFrame) -> dict:
    """
    Orchestrates the transformation pipeline.
    """
    df_clean = clean_and_standardize(raw_df)
    dimensions = generate_dimensions(df_clean)
    fact_table = generate_fact_table(df_clean, dimensions)
    
    # --- FIX: Rename Dimension Columns to match PostgreSQL schema exactly ---
    logging.info("Renaming dimension columns to match database schema...")
    
    dimensions['period'].rename(columns={
        'VIGENCIA': 'vigencia',
        'PERIODO OTORGAMIENTO': 'periodo_otorgamiento'
    }, inplace=True)
    
    dimensions['geography'].rename(columns={
        'CÓDIGO DEDEPARTAMENTO DE ORIGEN': 'codigo_departamento',
        'DEPARTAMENTO DE ORIGEN': 'departamento',
        'CATEGORÍA DEL MUNICIPIO DE ORIGEN': 'categoria_municipio'
    }, inplace=True)
    
    dimensions['program'].rename(columns={
        'SECTOR IES': 'sector_ies',
        'NIVEL DE FORMACIÓN': 'nivel_formacion',
        'MODALIDAD DE LÍNEA': 'modalidad_linea',
        'MODALIDAD DEL CRÉDITO': 'modalidad_credito'
    }, inplace=True)
    
    dimensions['student'].rename(columns={
        'SEXO AL NACER': 'sexo_al_nacer',
        'ESTRATO SOCIOECONÓMICO': 'estrato_socioeconomico'
    }, inplace=True)
    
    # Package everything to send to the Load phase
    transformed_data = {
        'dim_period': dimensions['period'],
        'dim_geography': dimensions['geography'],
        'dim_program': dimensions['program'],
        'dim_student_profile': dimensions['student'],
        'fact_credits': fact_table
    }
    
    return transformed_data

if __name__ == "__main__":
    # Test execution block
    import sys
    import os
    
    # Append src path to import extract
    sys.path.append(os.path.dirname(os.path.abspath(__file__)))
    from extract import extract_csv
    
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    RAW_DATA_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'Créditos_Otorgados._20260304.csv')
    
    try:
        raw_df = extract_csv(RAW_DATA_PATH)
        final_data = run_transformation(raw_df)
        
        print("\n--- TRANSFORMATION RESULTS ---")
        for table_name, table_df in final_data.items():
            print(f"Table: {table_name} | Shape: {table_df.shape}")
            
        print("\nSample of Fact Table:")
        print(final_data['fact_credits'].head(3))
        
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")