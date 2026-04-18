-- =============================================================================
-- Data Mart Analítico — Fase 4: Vista Materializada para Power BI / Tableau
-- =============================================================================
--
-- Vista:   mv_impacto_macro_desplazamiento
-- Propósito: Alimentar el dashboard con los indicadores clave del
--            "Crowding-Out Effect" (desplazamiento de matrícula privada → pública)
--            cruzados con contexto macroeconómico real (IPC / Pobreza Monetaria)
--            ahora almacenado directamente en fact_credits.
--
-- Granularidad final:
--   Una fila por (Año × Departamento × Sector IES)
--
-- Métricas incluidas:
--   • total_beneficiarios          — SUM(total_nuevos_beneficiarios)
--   • inflacion_promedio           — AVG(inflacion_anual)   [desde fact_credits]
--   • ipc_promedio                 — AVG(ipc_indice)        [desde fact_credits]
--   • pobreza_promedio             — AVG(pobreza_monetaria) [desde fact_credits]
--   • variacion_yoy_pct            — % variación interanual via LAG (beneficiarios)
--   • variacion_yoy_inflacion_pct  — % variación interanual via LAG (inflación)
--   • participacion_sector_pct     — cuota de mercado del sector dentro del depto/año
--   • variacion_cuota_pp           — cambio en puntos porcentuales vs año anterior
--
-- Dependencias:
--   fact_credits (con columnas macro: inflacion_anual, ipc_indice, pobreza_monetaria)
--   dim_period, dim_geography, dim_program, dim_student_profile
--
-- Prerequisito:
--   Ejecutar sql/alter_schema.sql ANTES de crear esta vista si las columnas
--   macroeconómicas aún no existen en fact_credits.
--
-- Autor:  ETL Pipeline — Fase 4
-- Fecha:  2026-04-17
-- =============================================================================

-- Eliminar versión previa si existe (idempotencia)
DROP MATERIALIZED VIEW IF EXISTS mv_impacto_macro_desplazamiento;

CREATE MATERIALIZED VIEW mv_impacto_macro_desplazamiento AS

-- ─── CTE 1: Agregación base por (año, departamento, sector) ─────────────────
-- Filtra solo estratos 1, 2 y 3 (población objetivo de créditos ICETEX).
-- INCLUYE métricas macroeconómicas reales desde fact_credits.
WITH base_agregada AS (
    SELECT
        dp.vigencia,
        dg.departamento,
        dg.codigo_departamento,
        pr.sector_ies,

        -- ── Medida principal ──
        SUM(fc.total_nuevos_beneficiarios)   AS total_beneficiarios,
        COUNT(*)                              AS num_registros_fact,

        -- ── Métricas macroeconómicas REALES (desde fact_credits) ──
        -- AVG es correcto porque el mismo valor macro se replica en cada fila
        -- del hecho para una combinación (año, departamento) dada.
        AVG(fc.inflacion_anual)              AS inflacion_promedio,
        AVG(fc.ipc_indice)                   AS ipc_promedio,
        AVG(fc.pobreza_monetaria)            AS pobreza_promedio

    FROM fact_credits   fc
    JOIN dim_period          dp ON fc.sk_period          = dp.sk_period
    JOIN dim_geography       dg ON fc.sk_geography       = dg.sk_geography
    JOIN dim_program         pr ON fc.sk_program         = pr.sk_program
    JOIN dim_student_profile sp ON fc.sk_student_profile = sp.sk_student_profile

    -- ── Filtro analítico: solo estratos objetivo ──
    WHERE sp.estrato_socioeconomico IN (1, 2, 3)
      AND pr.sector_ies IN ('OFICIAL', 'PRIVADA')

    GROUP BY
        dp.vigencia,
        dg.departamento,
        dg.codigo_departamento,
        pr.sector_ies
),

-- ─── CTE 2: Totales por (año, departamento) para cuota de mercado ───────────
totales_depto AS (
    SELECT
        vigencia,
        departamento,
        SUM(total_beneficiarios) AS total_depto_anio
    FROM base_agregada
    GROUP BY vigencia, departamento
),

-- ─── CTE 3: Enriquecimiento con Window Functions ───────────────────────────
-- LAG sobre la partición (departamento, sector) ordenada por año.
-- Calcula la variación interanual (YoY) de beneficiarios Y de indicadores macro.
enriquecida AS (
    SELECT
        ba.vigencia,
        ba.departamento,
        ba.codigo_departamento,
        ba.sector_ies,
        ba.total_beneficiarios,
        ba.num_registros_fact,

        -- ── Métricas macroeconómicas directas ──
        ROUND(ba.inflacion_promedio, 4)      AS inflacion_promedio,
        ROUND(ba.ipc_promedio, 4)            AS ipc_promedio,
        ROUND(ba.pobreza_promedio, 4)        AS pobreza_promedio,

        -- ── Cuota de mercado del sector en el departamento ese año ──
        ROUND(
            ba.total_beneficiarios * 100.0
            / NULLIF(td.total_depto_anio, 0),
            2
        ) AS participacion_sector_pct,

        -- ── Beneficiarios del año anterior (mismo depto + sector) ──
        LAG(ba.total_beneficiarios)
            OVER w_depto_sector AS beneficiarios_anio_anterior,

        -- ── Variación interanual (%) de beneficiarios ──
        ROUND(
            (
                ba.total_beneficiarios
                - LAG(ba.total_beneficiarios) OVER w_depto_sector
            ) * 100.0
            / NULLIF(
                LAG(ba.total_beneficiarios) OVER w_depto_sector,
                0
              ),
            2
        ) AS variacion_yoy_pct,

        -- ── Variación interanual (%) de inflación ──
        ROUND(
            (
                ba.inflacion_promedio
                - LAG(ba.inflacion_promedio) OVER w_depto_sector
            ) * 100.0
            / NULLIF(
                LAG(ba.inflacion_promedio) OVER w_depto_sector,
                0
              ),
            2
        ) AS variacion_yoy_inflacion_pct,

        -- ── Variación interanual (%) de pobreza monetaria ──
        ROUND(
            (
                ba.pobreza_promedio
                - LAG(ba.pobreza_promedio) OVER w_depto_sector
            ) * 100.0
            / NULLIF(
                LAG(ba.pobreza_promedio) OVER w_depto_sector,
                0
              ),
            2
        ) AS variacion_yoy_pobreza_pct,

        -- ── Cuota del año anterior para ver tendencia ──
        LAG(
            ROUND(
                ba.total_beneficiarios * 100.0
                / NULLIF(td.total_depto_anio, 0),
                2
            )
        ) OVER w_depto_sector AS participacion_anterior_pct,

        -- ── Variación interanual (pp) de la cuota de mercado ──
        ROUND(
            ba.total_beneficiarios * 100.0
            / NULLIF(td.total_depto_anio, 0)
            - LAG(
                ba.total_beneficiarios * 100.0
                / NULLIF(td.total_depto_anio, 0)
              ) OVER w_depto_sector,
            2
        ) AS variacion_cuota_pp,

        -- ── Rank del departamento por volumen dentro de cada sector/año ──
        RANK() OVER (
            PARTITION BY ba.vigencia, ba.sector_ies
            ORDER BY ba.total_beneficiarios DESC
        ) AS rank_depto_por_sector

    FROM base_agregada  ba
    JOIN totales_depto   td
      ON ba.vigencia     = td.vigencia
     AND ba.departamento = td.departamento

    -- Named window: reutilizada por todos los LAG (reduce repetición y errores)
    WINDOW w_depto_sector AS (
        PARTITION BY ba.departamento, ba.sector_ies
        ORDER BY ba.vigencia
    )
)

-- ─── SELECT final de la vista materializada ─────────────────────────────────
SELECT
    vigencia,
    departamento,
    codigo_departamento,
    sector_ies,
    total_beneficiarios,
    num_registros_fact,
    inflacion_promedio,
    ipc_promedio,
    pobreza_promedio,
    participacion_sector_pct,
    beneficiarios_anio_anterior,
    variacion_yoy_pct,
    variacion_yoy_inflacion_pct,
    variacion_yoy_pobreza_pct,
    participacion_anterior_pct,
    variacion_cuota_pp,
    rank_depto_por_sector
FROM enriquecida
ORDER BY vigencia, departamento, sector_ies;

-- =============================================================================
-- ÍNDICES sobre la vista materializada para rendimiento en Power BI / Tableau
-- =============================================================================
-- Unique index requerido por REFRESH MATERIALIZED VIEW CONCURRENTLY
CREATE UNIQUE INDEX idx_mv_impacto_pk
    ON mv_impacto_macro_desplazamiento (vigencia, departamento, sector_ies);

-- Índices de filtrado rápido para los slicers más comunes del dashboard
CREATE INDEX idx_mv_impacto_vigencia
    ON mv_impacto_macro_desplazamiento (vigencia);

CREATE INDEX idx_mv_impacto_depto
    ON mv_impacto_macro_desplazamiento (departamento);

CREATE INDEX idx_mv_impacto_sector
    ON mv_impacto_macro_desplazamiento (sector_ies);

CREATE INDEX idx_mv_impacto_yoy
    ON mv_impacto_macro_desplazamiento (variacion_yoy_pct);

-- Índice compuesto para scatter plots (inflación vs caída de créditos)
CREATE INDEX idx_mv_impacto_inflacion_yoy
    ON mv_impacto_macro_desplazamiento (inflacion_promedio, variacion_yoy_pct);

-- =============================================================================
-- REFRESH: Ejecutar desde Airflow después de cada carga exitosa
-- =============================================================================
-- En el DAG, añadir un PythonOperator o PostgresOperator al final:
--
--   @task(task_id="refresh_datamart")
--   def refresh_datamart() -> None:
--       """Refreshes the materialized view after fact_credits load."""
--       from sqlalchemy import create_engine, text
--       import os
--       engine = create_engine(os.environ["ICETEX_DW_URI"])
--       with engine.begin() as conn:
--           conn.execute(text(
--               "REFRESH MATERIALIZED VIEW CONCURRENTLY "
--               "mv_impacto_macro_desplazamiento"
--           ))
--       engine.dispose()
--
-- CONCURRENTLY permite actualizar sin bloquear lecturas de Power BI,
-- pero REQUIERE el UNIQUE INDEX (idx_mv_impacto_pk) definido arriba.
--
-- Para un refresh bloqueante (más rápido, pero bloquea SELECT):
--   REFRESH MATERIALIZED VIEW mv_impacto_macro_desplazamiento;
-- =============================================================================
