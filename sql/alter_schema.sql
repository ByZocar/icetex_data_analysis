-- =============================================================================
-- Migración de Esquema — Fase 4: Columnas Macroeconómicas en fact_credits
-- =============================================================================
--
-- Problema: El pipeline de merge (transform_and_merge.py) cruza los datos
-- ICETEX con indicadores macroeconómicos DANE (IPC / Pobreza Monetaria),
-- pero las columnas resultantes no se propagaban a PostgreSQL porque el DDL
-- original de fact_credits no las contemplaba.
--
-- Solución: Añadir columnas NULLABLE de tipo NUMERIC a la tabla de hechos.
-- Son dimensiones degeneradas (el valor macro depende solo de año y depto,
-- pero vive en la fila del hecho para evitar un JOIN adicional en consultas
-- analíticas de Tableau / Power BI).
--
-- Idempotencia: IF NOT EXISTS garantiza que el script es seguro de ejecutar
-- múltiples veces sin error.
--
-- Ejecutar ANTES de correr el pipeline de carga actualizado.
-- =============================================================================

-- ─── Columnas del dataset IPC (Índice de Precios al Consumidor) ─────────────
-- Fuente: DANE vía Socrata  (ej. dataset er9f-6p48)
-- Después de la agregación anual en transform_and_merge.py, las columnas
-- numéricas del API se promedian por (año × departamento).

ALTER TABLE fact_credits
-- ─── Columna de PIB Departamental ───────────────────────────────────────────
-- Fuente: DANE vía Socrata
-- Se incluye para el análisis de correlación con desplazamiento de matrícula.

ALTER TABLE fact_credits
    ADD COLUMN IF NOT EXISTS pib_miles_millones NUMERIC(15, 4);

COMMENT ON COLUMN fact_credits.pib_miles_millones IS 'Producto Interno Bruto (miles de millones) del departamento en el año del crédito. Extraído de la API Socrata DANE.';

-- ─── Índice parcial para filtros analíticos comunes ─────────────────────────
-- Acelera las consultas de Tableau/Power BI que filtran por años con datos macro.
CREATE INDEX IF NOT EXISTS idx_fact_pib_not_null 
    ON fact_credits (sk_period, sk_geography) 
    WHERE pib_miles_millones IS NOT NULL;

-- =============================================================================
-- Verificación post-migración (ejecutar manualmente):
--   SELECT column_name, data_type, is_nullable
--   FROM information_schema.columns
--   WHERE table_name = 'fact_credits'
--   ORDER BY ordinal_position;
-- =============================================================================
