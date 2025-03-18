# Gold Layer Creation Script for LAFT Crime Data
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
base::sapply(required_pkgs, require, character.only = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define paths for Silver and Gold layers
org_id <- "0000_CrimesLAFT"
silver_path <- base::file.path("Data", "Silver", org_id, "2022_2023_2024", paste0(org_id, ".duckdb"))
gold_path <- base::file.path("Data", "Gold", org_id, "2022_2023_2024", paste0(org_id, ".duckdb"))

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

# Create the delictividad table with crime rates by department and year
base::message("Creating gold_delictividad table with crime rates...")

delictividad_query <- "
CREATE TABLE IF NOT EXISTS gold_delictividad AS
WITH combined_data AS (
  SELECT 
    c.departamento,
    c.year,
    c.n_cases AS crimes,
    p.total AS population,
    CASE 
      WHEN p.total > 0 THEN (c.n_cases * 100000.0 / p.total) 
      ELSE NULL 
    END AS rate_per_100k
  FROM silver_db.silver_crimes c
  LEFT JOIN silver_db.silver_population p 
    ON c.departamento = p.departamento 
    AND c.year = p.anio
  WHERE c.year IN (2022, 2023, 2024)
    AND p.anio IN (2022, 2023, 2024)
),

pivoted_data AS (
  SELECT
    departamento,
    MAX(CASE WHEN year = 2022 THEN crimes ELSE NULL END) AS crimes22,
    MAX(CASE WHEN year = 2022 THEN population ELSE NULL END) AS pop22,
    MAX(CASE WHEN year = 2022 THEN rate_per_100k ELSE NULL END) AS rate22,
    MAX(CASE WHEN year = 2023 THEN crimes ELSE NULL END) AS crimes23,
    MAX(CASE WHEN year = 2023 THEN population ELSE NULL END) AS pop23,
    MAX(CASE WHEN year = 2023 THEN rate_per_100k ELSE NULL END) AS rate23,
    MAX(CASE WHEN year = 2024 THEN crimes ELSE NULL END) AS crimes24,
    MAX(CASE WHEN year = 2024 THEN population ELSE NULL END) AS pop24,
    MAX(CASE WHEN year = 2024 THEN rate_per_100k ELSE NULL END) AS rate24
  FROM combined_data
  GROUP BY departamento
)

SELECT * FROM pivoted_data
ORDER BY departamento
"

tryCatch({
  DBI::dbExecute(gold_con, delictividad_query)
  base::message("Successfully created gold_delictividad table")
}, error = function(e) {
  base::message(glue::glue("Error creating gold_delictividad: {e$message}"))
  base::message("Query was: ", delictividad_query)
  base::stop("Failed to create gold_delictividad table")
})

# Verify record counts and sample data
delictividad_count <- DBI::dbGetQuery(gold_con, "SELECT COUNT(*) FROM gold_delictividad")
base::message(glue::glue("Gold delictividad table created with {delictividad_count} department records"))

# Sample data display
sample_data <- DBI::dbGetQuery(gold_con, "
  SELECT 
    departamento,
    crimes22, 
    pop22, 
    ROUND(rate22, 2) AS rate22,
    crimes23, 
    pop23, 
    ROUND(rate23, 2) AS rate23,
    crimes24, 
    pop24, 
    ROUND(rate24, 2) AS rate24
  FROM gold_delictividad 
  ORDER BY rate24 DESC NULLS LAST
  LIMIT 10
")
base::message("Top 10 departments by 2024 crime rate:")
base::print(sample_data)

# Create indexes for better performance
DBI::dbExecute(gold_con, "CREATE INDEX IF NOT EXISTS idx_delictividad_dept ON gold_delictividad(departamento)")

# Optimize Database Performance
DBI::dbExecute(gold_con, "VACUUM")
DBI::dbExecute(gold_con, "ANALYZE")

# Close database connections
DBI::dbExecute(gold_con, "DETACH DATABASE silver_db;")
DBI::dbDisconnect(silver_con)
DBI::dbDisconnect(gold_con)

# Clean up environment
base::rm(list = base::ls())
