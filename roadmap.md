# Roadmap de Ingeniería ETL - Fase 2: Automatización, Calidad y Contexto Macroeconómico

## Objetivo Estratégico y Analítico
Demostrar mediante modelado dimensional y cruce de datos el "Crowding-Out Effect" en la educación superior en Colombia, correlacionando el desfinanciamiento del ICETEX y la implementación de Matrícula Cero con factores macroeconómicos (Inflación/Desempleo). La infraestructura debe procesar >100k registros garantizando cero pérdida de datos, idempotencia y observabilidad total.

---

## Fase 1: Arquitectura de Extracción e Integración API (Semana 1)
- [ ] **1.1 Configuración Socrata API (Datos Abiertos Colombia):**
    - Obtener App Token para el endpoint de Socrata (Librería `sodapy`).
    - Definir endpoint objetivo: Índice de Precios al Consumidor (IPC) histórico o Tasa de Desempleo (DANE).
    - Implementar lógica de paginación (`limit`, `offset`) para evitar timeouts de la API.
- [ ] **1.2 Extracción y Tipado Fuerte (Extract):**
    - Desarrollar `extract_api.py`.
    - Forzar tipado en la ingesta: Convertir strings numéricos de la API a `float32` y fechas a `datetime64[ns]`.
    - Implementar un mecanismo de *retry* con *exponential backoff* en caso de fallos de red (usando `tenacity`).
- [ ] **1.3 Integración y Cruce de Granularidad (Merge Strategy):**
    - **Problema:** La API puede tener nombres de departamentos diferentes (ej. "VALLE" vs "VALLE DEL CAUCA").
    - **Solución:** Crear un diccionario de mapeo estandarizado o usar librerías de similitud de strings (FuzzyWuzzy/TheFuzz) con un umbral de >90% para homogeneizar las llaves de cruce con `dim_geography`.
    - Realizar un `LEFT JOIN` desde la tabla de hechos hacia los datos macroeconómicos utilizando el año (`vigencia`) y el `codigo_departamento` como llaves compuestas.

---

## Fase 2: Ingeniería de Confiabilidad de Datos (Great Expectations) (Semana 1-2)
- [ ] **2.1 Inicialización del Data Context:**
    - Ejecutar `great_expectations init` en la raíz del proyecto.
    - Configurar un `Datasource` apuntando a las salidas temporales de Pandas (archivos Parquet en `/data/interim/`).
- [ ] **2.2 Diseño de Expectation Suites (Validaciones Estrictas):**
    - **Suite 1: Integridad Dimensional (Post-Merge)**
        - `expect_column_values_to_not_be_null`: Aplicado obligatoriamente a `sk_period`, `sk_geography`, `sk_program`, `sk_student_profile`.
        - `expect_column_distinct_values_to_be_in_set`: En `sector_ies` (Solo permitir: 'OFICIAL', 'PRIVADA', 'NO CLASIFICADO').
    - **Suite 2: Integridad de Medidas (Fact Table)**
        - `expect_column_values_to_be_between`: `total_nuevos_beneficiarios` debe ser `>= 0`.
        - `expect_table_row_count_to_be_between`: Asegurar que el DataFrame final tenga entre `100,000` y `120,000` registros. (Un drop masivo indica fallo en el JOIN).
- [ ] **2.3 Checkpoints y Data Docs:**
    - Configurar Checkpoints para que generen un reporte HTML (Data Docs) en cada corrida.
    - Establecer un `ActionList` que lance una excepción de Python si la Suite 1 o 2 falla, deteniendo el flujo inmediatamente.

---

## Fase 3: Orquestación del Pipeline (Apache Airflow) (Semana 2-3)
- [ ] **3.1 Arquitectura de Contenedores:**
    - Escribir `Dockerfile` extendiendo `apache/airflow:2.8.0-python3.10`.
    - Instalar dependencias exactas en el contenedor (`requirements-airflow.txt`).
- [ ] **3.2 Diseño del Grafo Acíclico Dirigido (DAG):**
    - **Parámetros del DAG:** `schedule_interval='@monthly'`, `catchup=False`, `max_active_runs=1`.
    - **Paso de Datos:** Prohibido pasar DataFrames completos por XCom (límite de memoria). XCom solo pasará las rutas de los archivos `.parquet` guardados en una carpeta temporal montada.
- [ ] **3.3 Topología de Tareas (TaskFlow API o PythonOperator):**
    - `[Task 1a]` `extract_icetex_csv` 
    - `[Task 1b]` `extract_macro_api` 
    - `[Task 2]` `transform_and_merge_data` (Depende de 1a y 1b).
    - `[Task 3]` `run_gx_validation` (Depende de 2).
    - `[Task 4]` `load_to_postgres` (Depende de 3).
- [ ] **3.4 Lógica de Carga Idempotente (PostgreSQL):**
    - En lugar de `TRUNCATE CASCADE` (que borra todo el histórico), implementar lógica de *Upsert* (Insert on Conflict Update) en PostgreSQL usando SQLAlchemy, basándose en restricciones de unicidad sobre las llaves foráneas en `fact_credits`.

---

## Fase 4: Inteligencia de Negocios de Alto Nivel (Tableau) (Semana 4)
- [ ] **4.1 Modelado en el Data Server:**
    - Crear una vista materializada `mv_desplazamiento_macro` en PostgreSQL que pre-calcule la varianza YoY (Year over Year) del ratio Oficial/Privado.
- [ ] **4.2 Desarrollo Visual Gerencial:**
    - **KPIs Principales:** Tasa de Desplazamiento (%), Variación IPC (%), Pérdida de Cuota Privada.
    - **Gráficos:** - Slope Chart (Gráfico de pendiente) mostrando la caída de matrícula privada vs aumento público entre 2018 y 2025.
        - Scatter Plot (Dispersión): Inflación/Desempleo en el Eje X vs % de caída de créditos privados en el Eje Y (para probar correlación).