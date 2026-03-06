# ETL Project: Colombia & Sustainable Development Goals (SDG 4: Quality Education)

## 1. Project Objective & Evaluation Criteria
**Business Scenario:** Analyze the shift in higher education funding in Colombia, specifically evaluating the "crowding-out" effect caused by the transition from demand-side subsidies (*Generación E*) to supply-side gratuity (*Matrícula Cero*), and the subsequent defunding of the ICETEX credit system.
* **SDG Target:** Goal 4 (Quality Education) - ensuring equal access for all women and men to affordable and quality technical, vocational and tertiary education.
* **Analytical Question:** How has the implementation of *Matrícula Cero* and the 2025 ICETEX budget cuts displaced vulnerable students (Strata 1, 2, and 3) from the private higher education sector?
* **Evaluation Criteria for Success:** The project is successful if the ETL pipeline accurately integrates over 100k raw cohort records into a dimensional model without data loss, allowing the BI tool to dynamically compute the Official/Private enrollment ratio variance (YoY) exclusively using server-side SQL processing.

## 2. Grain Definition (MANDATORY)
**Grain:** One row in the fact table (`fact_credits`) represents the total aggregate count of new beneficiaries (`total_nuevos_beneficiarios`), defined by Academic Period, Geographic Origin, Demographic Profile, and Institutional Sector.
*(Note: The raw source data is pre-aggregated by the government portal into statistical cohorts. Therefore, the architectural grain is cohort-level, not individual-student-level).*

## 3. Star Schema Design Decisions
The dimensional model implements a Star Schema architecture optimized for OLAP operations in PostgreSQL.
* **Fact Table:** `fact_credits` stores the core measure (`total_nuevos_beneficiarios`) and foreign keys.
* **Dimensions:** * `dim_period`: Tracks academic terms and years.
  * `dim_geography`: Stores departmental and municipal categories.
  * `dim_program`: Classifies the academic level, credit modality, and the critical `sector_ies` (Official vs. Private).
  * `dim_student_profile`: Contains demographic data (`sexo_al_nacer`, `estrato_socioeconomico`).
* **Design Decisions & Technical Debt Mitigation:**
  1. **Surrogate Keys (SKs):** Implemented explicit auto-incrementing integer SKs (`sk_period`, `sk_geography`, etc.) generated during the Transform phase via Pandas. This decouples the Data Warehouse from unpredictable changes in government source IDs.
  2. **Referential Integrity Constraints:** Strict primary-to-foreign key mapping is enforced at the database level.
  3. **Null Handling in Dimensions:** Missing values in the `SECTOR IES` column are mapped to a default 'NO CLASIFICADO' dimension member to preserve referential integrity without losing measure counts.

*(See Entity Relationship Diagram below)*
![Star Schema Diagram](C:\Users\andre\Documents\Icetex\diagrams\star_schema.png)

## 4. Data Quality Assumptions & Profiling Log
During the EDA phase (`notebooks/eda.ipynb`), the following quality issues were identified and addressed in the transformation layer:
* **Type Mismatch / Formatting:** The `VIGENCIA` (Year) column contained thousands separators (e.g., "2,015") and was loaded as a string. *Fix:* Stripped commas and cast to `int64` via `pandas.Series.str.replace`.
* **Missing Values:** 9,184 rows (8.54%) lacked `SECTOR IES` classification. *Fix:* Imputed with 'NO CLASIFICADO'.
* **Categorical Noise:** Text columns presented trailing spaces and case inconsistencies. *Fix:* Standardized all strings using `.strip()` and `.upper()`.
* **Statistical Distribution & Outliers:** The measure column showed right-skewed distribution with values up to 668 beneficiaries per row. *Assumption:* These are valid mass-enrollment blocks typical of public sector reporting, not errors. They were preserved.

## 5. ETL Logic (Data Pipeline)
The pipeline is orchestrated via `main.py` ensuring a strict, idempotent execution sequence:
1. **Extract:** Validates and ingests the raw CSV file (`Créditos_Otorgados._20260304.csv`) utilizing specific `dtype` mappings to prevent early floating-point errors.
2. **Transform:** Executes cleaning algorithms, string standardization, handles missing values, dynamically extracts unique dimension tables, and generates sequential Surrogate Keys before performing `LEFT JOINS` to map SKs to the final Fact table.
3. **Load:** Connects to PostgreSQL via `SQLAlchemy`. Applies a `TRUNCATE CASCADE` strategy for idempotency, then executes chunked bulk inserts (Dimensions first, Fact table last) to honor database referential constraints.

## 6. How to Run the Project
1. Clone the repository and navigate to the root directory.
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
3. Set up a local PostgreSQL instance. Create a database named icetex_ods_dw.
4. Execute the schema creation script located at sql/create_tables.sql in your database.
5. Update database credentials in src/load.py.
6. Run the ETL Orchestrator:

python main.py

7. Generate the Data Mart view in PostgreSQL for BI consumption:
CREATE OR REPLACE VIEW vw_kpi_desplazamiento_matricula AS
SELECT dp.vigencia, dprog.sector_ies, dst.estrato_socioeconomico, SUM(fc.total_nuevos_beneficiarios) AS total_beneficiarios
FROM fact_credits fc
JOIN dim_period dp ON fc.sk_period = dp.sk_period
JOIN dim_program dprog ON fc.sk_program = dprog.sk_program
JOIN dim_student_profile dst ON fc.sk_student_profile = dst.sk_student_profile
WHERE dst.estrato_socioeconomico IN (1, 2, 3) AND dprog.sector_ies != 'NO CLASIFICADO'
GROUP BY dp.vigencia, dprog.sector_ies, dst.estrato_socioeconomico;

## 7. Example Outputs & Visualizations

The BI architecture implements Server-side Processing (Pushdown). Power BI does not import the CSV or the raw tables; it queries the optimized `vw_kpi_desplazamiento_matricula` Data Mart.

### Business Insights: The "Crowding-Out" Effect & ICETEX Defunding

* **Phase 1: Demand-Side Subsidy (2018 - 2021):** The private sector captured 91% of ICETEX approvals for vulnerable strata. The Official/Private ratio stood at 10% (1 public student for every 10 private students funded).
* **Phase 2: Sector Crisis (2022 - 2025):** Accompanied by severe structural budget cuts to ICETEX operations (a 33% budget reduction confirmed for 2025), the private sector experienced a massive displacement effect. The pipeline calculates a devastating **-89.77% YoY Net Variation** in the private sector by 2025, completely centralizing demand within the public sector.
