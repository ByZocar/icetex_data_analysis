# ETL Project: Colombia & Sustainable Development Goals (SDG 4: Quality Education)

## 1. Project Objective
The primary business objective of this project is to analyze the shift in higher education funding in Colombia, specifically evaluating the "crowding-out" effect of recent government policy changes (transitioning from demand-side subsidies like *Generación E* to supply-side gratuity like *Matrícula Cero*). 

Using open government data from ICETEX ("Créditos Otorgados 2015-2025"), this production-grade ETL pipeline quantifies the variation in credit allocation between Official (Public) and Private institutions for vulnerable populations (Strata 1, 2, and 3).

## 2. Grain Definition (Mandatory Requirement)
**Grain:** One row in the fact table (`fact_credits`) represents the total count of new beneficiaries (`total_nuevos_beneficiarios`), aggregated by Academic Period, Geographic Origin, Demographic Profile, and Institutional Sector. 
*Note: The raw dataset is pre-aggregated by cohorts, hence the grain is at the cohort level, not the individual student level.*

## 3. Star Schema Design Decisions
The dimensional model follows a Star Schema architecture optimized for OLAP queries in PostgreSQL:
* **Fact Table:** `fact_credits` stores the core measure (`total_nuevos_beneficiarios`) and foreign keys.
* **Dimensions:** * `dim_period`: Tracks academic terms and years.
  * `dim_geography`: Stores departmental and municipal categories.
  * `dim_program`: Classifies the academic level, credit modality, and the critical `sector_ies` (Official vs. Private).
  * `dim_student_profile`: Contains demographic data (`sexo_al_nacer`, `estrato_socioeconomico`).
* **Design Decisions:**
  1. **Surrogate Keys (SKs):** Implemented explicit auto-incrementing integer SKs (`sk_period`, `sk_geography`, etc.) to decouple the Data Warehouse from potential changes in ICETEX source IDs.
  2. **Null Handling in Dimensions:** Missing values in the `SECTOR IES` column are mapped to a default 'NO CLASIFICADO' dimension member to preserve referential integrity without losing the measure counts.

## 4. Data Quality Assumptions & Profiling Log
During the EDA phase, several data quality issues were identified and addressed in the transformation layer:
* **Type Mismatch / Formatting:** The `VIGENCIA` (Year) column contained thousands separators (e.g., "2,015") and was loaded as a string. *Assumption:* This is a portal export artifact. *Fix:* Stripped commas and cast to `int64`.
* **Missing Values:** 9,184 rows (8.54%) lacked `SECTOR IES` classification. *Fix:* Imputed with 'NO CLASIFICADO' to maintain the integrity of the total beneficiaries count.
* **Categorical Noise:** Text columns presented trailing spaces and case inconsistencies. *Fix:* Standardized all strings using `.strip()` and `.upper()`.
* **Statistical Distribution:** The measure column showed right-skewed distribution with outliers up to 668 beneficiaries per row. *Assumption:* These are valid mass-enrollment blocks typical of public sector reporting, not errors.

## 5. ETL Logic (Data Pipeline)
1. **Extract:** Reads the raw CSV file (`Créditos_Otorgados._20260304.csv`) using Pandas.
2. **Transform:** Applies data cleaning (comma removal, type casting), string standardization, null imputation, and generates the unique dimension DataFrames required for the Star Schema.
3. **Load:** Connects to the PostgreSQL database, loads the Dimension tables first to generate Surrogate Keys, retrieves those SKs, maps them to the Fact table, and finally loads the Fact table ensuring strict referential integrity.



## 7. BI Architecture & Server-Side Processing (Workshop 2.8)

To comply with the requirement of generating reports exclusively from the Data Warehouse, the Power BI dashboard does not connect to the raw CSV nor does it import the entire Star Schema into memory.

Instead, a specific **Data Mart view** (`vw_kpi_desplazamiento_matricula`) was created in PostgreSQL. Power BI connects directly to this view using the *Pushdown/Server-side processing* paradigm. This delegates the heavy computational load (JOINs and aggregations) to the database engine, ensuring an optimized, production-level BI architecture. All subsequent YoY (Year-over-Year) calculations and ratios are computed dynamically in memory using DAX measures.




## 8. Business Insights: The "Crowding-Out" Effect & ICETEX Defunding (2018-2025)

The ETL pipeline and subsequent data visualization successfully validate the hypothesis regarding the shift in higher education funding policies. The analysis is divided into two distinct political periods:

### Phase 1: Demand-Side Subsidy Dominance (2018 - 2021)
During the previous administration, characterized by programs like *Generación E*, the funding model heavily favored private institutions. 
* **Market Composition:** The private sector captured **91%** of all ICETEX credit approvals for vulnerable strata, compared to just 9% for the official sector.
* **Ratio:** The Official/Private ratio stood at **10%**, indicating that for every 10 students funded in a private university, only 1 was funded in a public one through this mechanism.

### Phase 2: Supply-Side Transition and Sector Crisis (2022 - 2025)
The introduction of the *Matrícula Cero* policy (direct funding to public institutions) was accompanied by severe structural budget cuts to ICETEX operations, triggering a massive displacement effect.
* **The 2025 Budget Defunding:** The Colombian Comptroller General's Office confirmed a 33% budget reduction for ICETEX in 2025 (dropping from $1.2 trillion to $859 billion COP), which directly cut administered credit lines by 47%. 
* **The Collapse in New Credits:** Institutional reports indicate that new credits plummeted by 80% (from 50,000 to roughly 10,000 in 2025). This perfectly correlates with our Data Mart visualization, which exposes a devastating **-89.77% YoY Net Variation** in the private sector for 2025.
* **Subsidy Reductions:** The removal of interest rate subsidies severely affected active borrowers, causing monthly payment quotas for strata 1 and 2 to skyrocket by up to 93%.

**Conclusion:** The data proves a definitive "crowding-out" effect. The government's policy shift and the subsequent defunding of ICETEX have effectively dismantled the financial mechanisms that allowed vulnerable populations (Strata 1, 2, and 3) to access private higher education, centralizing the demand almost exclusively within the capacity constraints of the official public sector.

## 6. How to Run the Project
1. Clone the repository and navigate to the root directory.
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt