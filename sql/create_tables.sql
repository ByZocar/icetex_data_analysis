-- ETL Project: ICETEX Credit Analysis
-- Star Schema Definitio
-- 1. DIMENSION TABLES

-- Dimension: Period
CREATE TABLE dim_period (
    sk_period SERIAL PRIMARY KEY, -- Surrogate Key
    vigencia INT NOT NULL,        -- Transformed from string with commas
    periodo_otorgamiento VARCHAR(10) NOT NULL
);

-- Dimension: Geography
CREATE TABLE dim_geography (
    sk_geography SERIAL PRIMARY KEY,
    codigo_departamento INT NOT NULL,
    departamento VARCHAR(100) NOT NULL,
    categoria_municipio VARCHAR(100) NOT NULL
);

-- Dimension: Program & Institution
CREATE TABLE dim_program (
    sk_program SERIAL PRIMARY KEY,
    sector_ies VARCHAR(50) DEFAULT 'NO CLASIFICADO', -- Handling nulls
    nivel_formacion VARCHAR(100) NOT NULL,
    modalidad_linea VARCHAR(100) NOT NULL,
    modalidad_credito VARCHAR(100) NOT NULL
);

-- Dimension: Student Profile
CREATE TABLE dim_student_profile (
    sk_student_profile SERIAL PRIMARY KEY,
    sexo_al_nacer VARCHAR(20) NOT NULL,
    estrato_socioeconomico INT NOT NULL
);

-- 2. FACT TABLE

-- Fact: Credits (Grain: Aggregated new beneficiaries per cohort profile)
CREATE TABLE fact_credits (
    sk_fact_credit SERIAL PRIMARY KEY,
    sk_period INT NOT NULL,
    sk_geography INT NOT NULL,
    sk_program INT NOT NULL,
    sk_student_profile INT NOT NULL,
    rango_valor_desembolsado VARCHAR(10) NOT NULL,
    
    -- Measure
    total_nuevos_beneficiarios INT NOT NULL,

    -- Explicit Foreign Keys
    CONSTRAINT fk_period FOREIGN KEY (sk_period) REFERENCES dim_period(sk_period),
    CONSTRAINT fk_geography FOREIGN KEY (sk_geography) REFERENCES dim_geography(sk_geography),
    CONSTRAINT fk_program FOREIGN KEY (sk_program) REFERENCES dim_program(sk_program),
    CONSTRAINT fk_student_profile FOREIGN KEY (sk_student_profile) REFERENCES dim_student_profile(sk_student_profile)
);

-- Indexes for performance on Foreign Keys (Industry Best Practice)
CREATE INDEX idx_fact_period ON fact_credits(sk_period);
CREATE INDEX idx_fact_geography ON fact_credits(sk_geography);
CREATE INDEX idx_fact_program ON fact_credits(sk_program);
CREATE INDEX idx_fact_student ON fact_credits(sk_student_profile);