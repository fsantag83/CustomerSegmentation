# Gold Layer Creation Script
# Reads from Silver layer and creates analysis-ready Gold tables

# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
base::set.seed(444)

# Load required packages with explicit namespaces
required_pkgs <- c("DBI", "duckdb", "dplyr", "lubridate", "glue")
install_if_missing <- function(pkg) {
  if (!base::requireNamespace(pkg, quietly = TRUE)) utils::install.packages(pkg)
}
base::invisible(base::lapply(required_pkgs, install_if_missing))
base::rm(install_if_missing, required_pkgs)

# Define paths for Silver and Gold layers
org_id <- "0001_Fonedh"
silver_path <- base::file.path("Data", "Silver", org_id, "Model_2023_2024", paste0(org_id, ".duckdb"))
gold_path <- base::file.path("Data", "Gold", org_id, "Model_2023_2024", paste0(org_id, ".duckdb"))

# Create Gold directory if it doesn't exist
if (!base::dir.exists(base::dirname(gold_path))) {
  base::dir.create(base::dirname(gold_path), recursive = TRUE)
}

# Connect to Silver and Gold databases
silver_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = silver_path, read_only = TRUE)
gold_con <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = gold_path,
  config = base::list(
    memory_limit = "16GB",
    threads = "6"
  ), 
  read_only = FALSE
)

# Attach Silver database to Gold connection
DBI::dbExecute(gold_con, glue::glue("ATTACH '{silver_path}' AS silver_db"))

# Create prepared clients table with derived variables
base::message("Preparing client data with derived variables...")

DBI::dbExecute(gold_con, "
  CREATE TABLE IF NOT EXISTS gold_clientes AS
  WITH reference_date AS (
    SELECT '2024-12-31'::DATE AS date
  ),
  
  prepared_clients AS (
    SELECT 
      c.normalized_id AS num_identificacion,
      c.rol,
      DATEDIFF('years', c.fecha_nacimiento, (SELECT date FROM reference_date)) AS edad,
      DATEDIFF('years', c.fecha_ingreso, (SELECT date FROM reference_date)) AS antiguedad,
      
      -- Type conversions and transformations
      CASE
        WHEN c.actividad_economica = 'Independiente' THEN 'Prestación Servicios'
        ELSE c.tipo_contrato
      END AS tipo_contrato,
      
      CASE
        WHEN c.tipo_contrato = 'Término Indefinido' THEN 'Indefinido'
        ELSE 'Otro'
      END AS tipo_contrato_group,
      
      CASE
        WHEN c.actividad_economica = 'Asalariado' THEN 'Asalariado'
        ELSE 'Otro'
      END AS actividad_economica_group,
      
      CASE
        WHEN c.nivel_escolaridad IN ('Primaria', 'Bachillerato') THEN '01_Basico'
        WHEN c.nivel_escolaridad IN ('Tecnico', 'Tecnologia') THEN '02_Tecnologico'
        WHEN c.nivel_escolaridad = 'Universitario' THEN '03_Universitario'
        ELSE '04_Posgrado'
      END AS nivel_escolaridad_group,
      
      CASE
        WHEN c.sector_economico IN ('Sector Privado Esal', 'Sector Privado Financiero Solidario') 
          THEN 'Sector Privado'
        ELSE c.sector_economico
      END AS sector_economico_group,
      
      CASE
        WHEN c.estrato = '1' THEN '1'
        WHEN c.estrato = '2' THEN '2'
        WHEN c.estrato = '3' THEN '3'
        ELSE '4-5-6'
      END AS estrato_group,
      
      c.activos,
      c.pasivos,
      c.salario_actual + COALESCE(c.otros_ingresos, 0) AS ingresos,
      c.egresos
    FROM silver_db.silver_clientes c
  )
  
  SELECT 
    pc.*,
    CASE WHEN pc.tipo_contrato_group = 'Indefinido' THEN 1 ELSE 0 END AS tipo_contrato_indefinido,
    CASE WHEN pc.tipo_contrato_group = 'Otro' THEN 1 ELSE 0 END AS tipo_contrato_otro,
    CASE WHEN pc.actividad_economica_group = 'Asalariado' THEN 1 ELSE 0 END AS actividad_economica_asalariado,
    CASE WHEN pc.actividad_economica_group = 'Otro' THEN 1 ELSE 0 END AS actividad_economica_otro,
    CASE WHEN pc.sector_economico_group = 'Sector Privado' THEN 1 ELSE 0 END AS sector_economico_sector_privado,
    CASE WHEN pc.sector_economico_group = 'Sector Publico Administrativo' THEN 1 ELSE 0 END AS sector_economico_sector_publico_administrativo,
    CASE WHEN pc.sector_economico_group = 'Sector Publico Educacion' THEN 1 ELSE 0 END AS sector_economico_sector_publico_educacion,
    CASE WHEN pc.sector_economico_group = 'Sector Publico Pensionado' THEN 1 ELSE 0 END AS sector_economico_sector_publico_pensionado,
    CASE WHEN pc.sector_economico_group = 'Sector Publico Salud' THEN 1 ELSE 0 END AS sector_economico_sector_publico_salud
  FROM prepared_clients pc
  WHERE pc.rol = 'Asociado'
")

# Process transactions
base::message("Processing transaction data...")

DBI::dbExecute(gold_con, "
  CREATE TABLE IF NOT EXISTS gold_movimientos AS
  WITH renamed_transactions AS (
    SELECT
      normalized_id AS num_identificacion,
      fecha_transaccion,
      tipo_producto AS producto,
      tipo_transaccion AS tipo,
      tipo_canal_transaccion AS canal,
      monto_transaccion AS monto,
      strftime(fecha_transaccion, '%Y-%m') AS month_yr,
      CASE WHEN tipo_canal_transaccion = 'Físico' THEN 'Fisico' ELSE 'Electronico' END AS canal_group
    FROM silver_db.silver_movimientos
  ),
  
  transaction_features AS (
    SELECT
      num_identificacion,
      month_yr,
      COUNT(*) AS n_tran,
      SUM(CASE WHEN producto = 'Ahorro' AND tipo = 'Egreso' THEN monto ELSE 0 END) AS ahorro_egreso,
      SUM(CASE WHEN producto = 'Ahorro' AND tipo = 'Ingreso' THEN monto ELSE 0 END) AS ahorro_ingreso,
      SUM(CASE WHEN producto = 'Credito' AND tipo = 'Egreso' THEN monto ELSE 0 END) AS credito_egreso,
      SUM(CASE WHEN producto = 'Credito' AND tipo = 'Ingreso' THEN monto ELSE 0 END) AS credito_ingreso,
      SUM(CASE WHEN canal_group = 'Fisico' AND tipo = 'Egreso' THEN monto ELSE 0 END) AS fisico_egreso,
      SUM(CASE WHEN canal_group = 'Fisico' AND tipo = 'Ingreso' THEN monto ELSE 0 END) AS fisico_ingreso,
      SUM(CASE WHEN canal_group = 'Electronico' AND tipo = 'Egreso' THEN monto ELSE 0 END) AS electronico_egreso,
      SUM(CASE WHEN canal_group = 'Electronico' AND tipo = 'Ingreso' THEN monto ELSE 0 END) AS electronico_ingreso
    FROM renamed_transactions
    GROUP BY num_identificacion, month_yr
  ),
  
  months_active AS (
    SELECT
      num_identificacion,
      COUNT(DISTINCT month_yr) AS n_mes
    FROM transaction_features
    GROUP BY num_identificacion
  ),
  
  aggregated_features AS (
    SELECT
      t.num_identificacion,
      SUM(t.ahorro_egreso) AS ahorro_egreso,
      SUM(t.ahorro_ingreso) AS ahorro_ingreso,
      SUM(t.credito_egreso) AS credito_egreso,
      SUM(t.credito_ingreso) AS credito_ingreso,
      SUM(t.fisico_egreso) AS fisico_egreso,
      SUM(t.fisico_ingreso) AS fisico_ingreso,
      SUM(t.electronico_egreso) AS electronico_egreso,
      SUM(t.electronico_ingreso) AS electronico_ingreso,
      COUNT(*) AS num_transacciones
    FROM transaction_features t
    GROUP BY t.num_identificacion
  )
  
  SELECT
    a.num_identificacion,
    a.ahorro_egreso / m.n_mes AS ahorro_egreso,
    a.ahorro_ingreso / m.n_mes AS ahorro_ingreso,
    a.credito_egreso / m.n_mes AS credito_egreso,
    a.credito_ingreso / m.n_mes AS credito_ingreso,
    a.fisico_egreso / m.n_mes AS fisico_egreso,
    a.fisico_ingreso / m.n_mes AS fisico_ingreso,
    a.electronico_egreso / m.n_mes AS electronico_egreso,
    a.electronico_ingreso / m.n_mes AS electronico_ingreso,
    a.num_transacciones / m.n_mes AS num_transacciones,
    m.n_mes
  FROM aggregated_features a
  JOIN months_active m ON a.num_identificacion = m.num_identificacion
")

# Create final affiliates table
base::message("Creating gold_affiliates table...")

DBI::dbExecute(gold_con, "
  CREATE TABLE IF NOT EXISTS gold_affiliates AS
  SELECT
    c.num_identificacion,
    c.activos AS \"Activos\",
    c.pasivos AS \"Pasivos\",
    c.ingresos AS \"Ingresos\",
    c.egresos AS \"Egresos\",
    m.ahorro_egreso AS \"Ahorro Egr.\",
    m.ahorro_ingreso AS \"Ahorro Ing.\",
    m.credito_egreso AS \"Credito Egr.\",
    m.credito_ingreso AS \"Credito Ing.\",
    m.fisico_egreso AS \"Fisico Egr.\",
    m.fisico_ingreso AS \"Fisico Ing.\",
    m.electronico_egreso AS \"Elec. Egr.\",
    m.electronico_ingreso AS \"Elec. Ing.\",
    m.num_transacciones AS \"Num. Trans.\",
    c.tipo_contrato_indefinido AS \"T.Contr. Indef.\",
    c.tipo_contrato_otro AS \"T.Contr. Otro\",
    c.actividad_economica_asalariado AS \"Act. Eco. Asalar.\",
    c.actividad_economica_otro AS \"Act. Eco. Otro\",
    c.sector_economico_sector_privado AS \"S.Econ. Priv.\",
    c.sector_economico_sector_publico_administrativo AS \"S.Econ. Pub. Admon.\",
    c.sector_economico_sector_publico_educacion AS \"S.Econ. Pub. Edu.\",
    c.sector_economico_sector_publico_pensionado AS \"S.Econ. Pub. Pens.\",
    c.sector_economico_sector_publico_salud AS \"S.Econ. Pub. Salud\"
  FROM gold_clientes c
  JOIN gold_movimientos m ON c.num_identificacion = m.num_identificacion
")

clientes <- DBI::dbGetQuery(gold_con, "SELECT * FROM gold_affiliates")

# Verify record counts
affiliates_count <- DBI::dbGetQuery(gold_con, "SELECT COUNT(*) FROM gold_affiliates")
base::message(glue::glue("Gold layer created successfully with {affiliates_count} affiliates"))

# Close database connections
DBI::dbExecute(gold_con, "DETACH DATABASE silver_db;")
DBI::dbDisconnect(silver_con)
DBI::dbDisconnect(gold_con)

base::rm(list = base::ls())
