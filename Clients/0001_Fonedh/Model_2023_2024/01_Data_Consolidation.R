# Silver Layer Creation Script
# Reads from Bronze layer and creates standardized Silver tables

# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
base::set.seed(444)

# Load required packages with explicit namespaces
required_pkgs <- c("DBI", "duckdb", "dplyr", "tidyr", "lubridate", "glue", "writexl")
install_if_missing <- function(pkg) {
  if (!base::requireNamespace(pkg, quietly = TRUE)) utils::install.packages(pkg)
}
base::invisible(base::lapply(required_pkgs, install_if_missing))
base::rm(install_if_missing, required_pkgs)

# Define paths for Bronze and Silver layers
org_id <- "0001_Fonedh"
bronze_path <- base::file.path("Data", "Bronze", org_id, "Model_2023_2024", paste0(org_id, ".duckdb"))
silver_path <- base::file.path("Data", "Silver", org_id, "Model_2023_2024", paste0(org_id, ".duckdb"))

# Create Silver directory if it doesn't exist
if (!base::dir.exists(base::dirname(silver_path))) {
  base::dir.create(base::dirname(silver_path), recursive = TRUE)
}

# Connect to Bronze and Silver databases
bronze_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = bronze_path, read_only = TRUE)
silver_con <- DBI::dbConnect(
  duckdb::duckdb(), 
  dbdir = silver_path,
  config = base::list(
    memory_limit = "16GB",
    threads = "6"
  ),
  read_only = FALSE
)

# Attach Bronze database to Silver connection
DBI::dbExecute(silver_con, glue::glue("ATTACH '{bronze_path}' AS bronze_db"))

# Define semester date ranges
semester_dates <- base::list(
  "2023-I" = c("2023-01-01", "2023-06-30"),
  "2023-II" = c("2023-07-01", "2023-12-31"),
  "2024-I" = c("2024-01-01", "2024-06-30"),
  "2024-II" = c("2024-07-01", "2024-12-31")
)

# Function to add missing columns
add_missing_columns_sql <- function(table_name, expected_cols) {
  existing_cols <- DBI::dbGetQuery(
    bronze_con, 
    glue::glue("SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
  )$column_name
  
  missing_cols <- base::setdiff(expected_cols, existing_cols)
  
  if (base::length(missing_cols) > 0) {
    for (col in missing_cols) {
      DBI::dbExecute(
        bronze_con,
        glue::glue("ALTER TABLE {table_name} ADD COLUMN {col} VARCHAR")
      )
    }
  }
}

# Process clients data
base::message("Processing clients data...")

# Get expected column names from Bronze
expected_cols_clientes <- DBI::dbGetQuery(bronze_con, "SELECT * FROM bronze_clientes LIMIT 0")
expected_cols_clientes <- base::names(expected_cols_clientes)
add_missing_columns_sql("bronze_clientes", expected_cols_clientes)

# Process clients (Keep only the latest semester per client)
DBI::dbExecute(silver_con, "
  CREATE TABLE IF NOT EXISTS silver_clientes AS
  WITH RankedClients AS (
    SELECT 
      c.*,
      UPPER(TRIM(c.num_identificacion)) AS normalized_id,
      c.semestre,
      ROW_NUMBER() OVER (
        PARTITION BY UPPER(TRIM(c.num_identificacion)) 
        ORDER BY c.semestre DESC
      ) AS rn
    FROM bronze_db.bronze_clientes c
  )
  SELECT * FROM RankedClients WHERE rn = 1
")

# Process transactions data
base::message("Processing transactions data...")

# Initialize semester information for each transaction
DBI::dbExecute(silver_con, "
  CREATE TABLE IF NOT EXISTS silver_movimientos AS
  WITH dated_transactions AS (
    SELECT 
      m.*,
      UPPER(TRIM(m.num_identificacion)) AS normalized_id,
      regexp_replace(m.source_file, '.*/', '') AS file_name
    FROM bronze_db.bronze_movimientos m
  )
  
  SELECT 
    t.*,
    CASE
      WHEN t.fecha_transaccion BETWEEN '2023-01-01' AND '2023-06-30' THEN '2023-I'
      WHEN t.fecha_transaccion BETWEEN '2023-07-01' AND '2023-12-31' THEN '2023-II'
      WHEN t.fecha_transaccion BETWEEN '2024-01-01' AND '2024-06-30' THEN '2024-I'
      WHEN t.fecha_transaccion BETWEEN '2024-07-01' AND '2024-12-31' THEN '2024-II'
      ELSE NULL
    END AS semestre
  FROM dated_transactions t
")

# Apply referential integrity (Optimized filtering using JOINs)
base::message("Applying referential integrity...")

DBI::dbExecute(silver_con, "
  CREATE TABLE silver_clientes_filtered AS
  SELECT DISTINCT c.*
  FROM silver_clientes c
  INNER JOIN silver_movimientos m ON m.normalized_id = c.normalized_id
")

DBI::dbExecute(silver_con, "
  CREATE TABLE silver_movimientos_filtered AS
  SELECT DISTINCT m.*
  FROM silver_movimientos m
  INNER JOIN silver_clientes_filtered c ON c.normalized_id = m.normalized_id
")

# Replace Original Tables
DBI::dbExecute(silver_con, "DROP TABLE IF EXISTS silver_clientes")
DBI::dbExecute(silver_con, "ALTER TABLE silver_clientes_filtered RENAME TO silver_clientes")

DBI::dbExecute(silver_con, "DROP TABLE IF EXISTS silver_movimientos")
DBI::dbExecute(silver_con, "ALTER TABLE silver_movimientos_filtered RENAME TO silver_movimientos")

# Indexing after Filtering
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_cliente_id ON silver_clientes(normalized_id)")
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_mov_id ON silver_movimientos(normalized_id)")
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_mov_date ON silver_movimientos(fecha_transaccion)")

# Optimize Database Performance
DBI::dbExecute(silver_con, "VACUUM")
DBI::dbExecute(silver_con, "ANALYZE")

# Load data into R for Excel export
clientes <- DBI::dbGetQuery(silver_con, "SELECT * FROM silver_clientes")
movimientos <- DBI::dbGetQuery(silver_con, "SELECT * FROM silver_movimientos")

# Define the output file path
output_file <- base::file.path("Clients", org_id, "Model_2023_2024", "Results", 
                               "Plantilla_EstructuraBase_2023_2024_Fonedh.xlsx")

# Ensure the Results directory exists
if (!base::dir.exists(base::dirname(output_file))) {
  base::dir.create(base::dirname(output_file), recursive = TRUE)
}

# Create a list of dataframes to export
data_to_export <- base::list(
  "InformaciónCliente" = clientes,
  "Movimientos" = movimientos
)

# Write to Excel with multiple sheets
writexl::write_xlsx(data_to_export, path = output_file)

base::message(glue::glue("Silver layer created successfully and saved to {output_file}"))

# Close database connections
DBI::dbExecute(silver_con, "DETACH DATABASE bronze_db;")
DBI::dbDisconnect(bronze_con)
DBI::dbDisconnect(silver_con)

# Clean up environment
base::rm(list = base::ls())