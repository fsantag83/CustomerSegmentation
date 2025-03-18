# Silver Layer Creation Script for LAFT Crime Data
# Reads from Bronze layer and creates standardized Silver tables

# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
base::set.seed(444)

# Load required packages with explicit namespaces
required_pkgs <- c("DBI", "duckdb", "dplyr", "tidyr", "lubridate", "glue", "stringr")
install_if_missing <- function(pkg) {
  if (!base::requireNamespace(pkg, quietly = TRUE)) utils::install.packages(pkg)
}
base::invisible(base::lapply(required_pkgs, install_if_missing))
base::sapply(required_pkgs, require, character.only = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define paths for Bronze and Silver layers
org_id <- "0000_CrimesLAFT"
bronze_path <- base::file.path("Data", "Bronze", org_id, "2022_2023_2024", paste0(org_id, ".duckdb"))
silver_path <- base::file.path("Data", "Silver", org_id, "2022_2023_2024", paste0(org_id, ".duckdb"))

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

# Function to normalize text by removing accents
normalize_text <- function(text_column) {
  return(glue::glue("
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                {text_column}, 
                '[áÁ]', 'a', 'g'
              ),
              '[éÉ]', 'e', 'g'
            ),
            '[íÍ]', 'i', 'g'
          ),
          '[óÓ]', 'o', 'g'
        ),
        '[úÚ]', 'u', 'g'
      ),
      '[ñÑ]', 'n', 'g'
    )
  "))
}

# Process population data with recoding logic
base::message("Processing population data with recoding...")

population_query <- glue::glue("
  CREATE OR REPLACE TABLE silver_population AS
  WITH step1 AS (
    -- Step 1: Filter for rows where area_geografica = 'Total'
    SELECT 
      cod_dpto,
      departamento,
      anio,
      total
    FROM bronze_db.bronze_population
    WHERE area_geografica = 'Total'
    AND anio IN (2022, 2023, 2024)
  ),

  step2 AS (
    -- Step 2: Initial standardization for special cases
    SELECT
      cod_dpto,
      CASE 
        WHEN departamento LIKE 'Archipiélago de San Andrés%' THEN 'San Andres'
        WHEN departamento = 'Bogota, D.C.' THEN 'Bogota'
        WHEN departamento = 'La Guajira' THEN 'Guajira'
        WHEN departamento = 'Valle del Cauca' THEN 'Valle'
        ELSE departamento
      END AS departamento,
      anio,
      total
    FROM step1
  ),

  step3 AS (
    -- Step 3: Normalize text (remove accents)
    SELECT
      cod_dpto,
      {normalize_text('departamento')} AS departamento_normalized,
      anio,
      total
    FROM step2
  ),

  step4 AS (
    -- Step 4: Final standardization with EXACT case matching for target departments
    SELECT
      cod_dpto,
      CASE 
        WHEN LOWER(departamento_normalized) = 'bogota' THEN 'Bogota'
        WHEN LOWER(departamento_normalized) = 'norte de santander' THEN 'Norte de Santander'
        WHEN LOWER(departamento_normalized) = 'san andres' THEN 'San Andres'
        ELSE CONCAT(UPPER(SUBSTRING(departamento_normalized, 1, 1)), LOWER(SUBSTRING(departamento_normalized, 2)))
      END AS departamento,
      anio,
      total
    FROM step3
  )

  -- Final step: Aggregate by department and year
  SELECT
    cod_dpto,
    departamento,
    anio,
    SUM(total) AS total
  FROM step4
  GROUP BY cod_dpto, departamento, anio
  ORDER BY departamento, anio
")

tryCatch({
  DBI::dbExecute(silver_con, population_query)
  base::message("Successfully created silver_population table with recoding")
}, error = function(e) {
  base::message(glue::glue("Error creating silver_population: {e$message}"))
  base::message("Query was: ", population_query)
  base::stop("Failed to process population data")
})

update_query <- "
  UPDATE silver_population
  SET departamento = 'Bogota'
  WHERE LOWER(departamento) = 'bogota, d.c.'
"

# Execute the update query
tryCatch({
  DBI::dbExecute(silver_con, update_query)
  base::message("Successfully updated departamento field in silver_population table.")
}, error = function(e) {
  base::message(glue::glue("Error updating departamento field: {e$message}"))
})

# Process crime data with recoding logic
base::message("Processing crime data with recoding...")

crimes_query <- glue::glue("
  CREATE OR REPLACE TABLE silver_crimes AS
  WITH step1 AS (
    -- Step 1: Initial selection with year filter
    SELECT
      CASE 
        WHEN municipio = 'Bogotá D.C. (CT)' THEN 'Bogota'
        WHEN departamento = 'Bogota, D.C.' THEN 'Bogota'
        WHEN departamento = 'La Guajira' THEN 'Guajira'
        WHEN departamento = 'Valle del Cauca' THEN 'Valle'
        ELSE departamento
      END AS departamento,
      EXTRACT(YEAR FROM fecha) AS year,
      cantidad
    FROM bronze_db.bronze_crimes
    WHERE EXTRACT(YEAR FROM fecha) IN (2022, 2023, 2024)
  ),

  step2 AS (
    -- Step 2: Normalize text (remove accents)
    SELECT
      {normalize_text('departamento')} AS departamento_normalized,
      year,
      cantidad
    FROM step1
  ),

  step3 AS (
    -- Step 3: Final standardization with EXACT case matching for target departments
    SELECT
      CASE 
        WHEN LOWER(departamento_normalized) = 'bogota' THEN 'Bogota'
        WHEN LOWER(departamento_normalized) = 'norte de santander' THEN 'Norte de Santander'
        WHEN LOWER(departamento_normalized) = 'san andres' THEN 'San Andres'
        ELSE CONCAT(UPPER(SUBSTRING(departamento_normalized, 1, 1)), LOWER(SUBSTRING(departamento_normalized, 2)))
      END AS departamento,
      year,
      cantidad
    FROM step2
  )

  -- Final step: Aggregate by department and year
  SELECT
    departamento,
    year,
    SUM(cantidad) AS n_cases
  FROM step3
  GROUP BY departamento, year
  ORDER BY departamento, year
")

tryCatch({
  DBI::dbExecute(silver_con, crimes_query)
  base::message("Successfully created silver_crimes table with recoding")
}, error = function(e) {
  base::message(glue::glue("Error creating silver_crimes: {e$message}"))
  base::message("Query was: ", crimes_query)
})

# Verify data consistency 
base::message("Verifying data consistency...")

# Get normalized departments from both tables
crime_depts_normalized <- DBI::dbGetQuery(silver_con, "
  SELECT DISTINCT departamento 
  FROM silver_crimes 
  ORDER BY departamento
")

pop_depts_normalized <- DBI::dbGetQuery(silver_con, "
  SELECT DISTINCT departamento 
  FROM silver_population 
  ORDER BY departamento
")

# Check for departments in crimes but not in population
missing_in_pop <- base::setdiff(crime_depts_normalized$departamento, pop_depts_normalized$departamento)
if (base::length(missing_in_pop) > 0) {
  base::message("Warning: The following departments have crime data but no population data:")
  base::print(missing_in_pop)
}

# Check for departments in population but not in crimes
missing_in_crimes <- base::setdiff(pop_depts_normalized$departamento, crime_depts_normalized$departamento)
if (base::length(missing_in_crimes) > 0) {
  base::message("Warning: The following departments have population data but no crime data:")
  base::print(missing_in_crimes)
}

# Create indexes for better performance
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_population_dept ON silver_population(departamento)")
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_population_anio ON silver_population(anio)")
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_crimes_dept ON silver_crimes(departamento)")
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_crimes_year ON silver_crimes(year)")

# Optimize Database Performance
DBI::dbExecute(silver_con, "VACUUM")
DBI::dbExecute(silver_con, "ANALYZE")

# Verify data
population_count <- DBI::dbGetQuery(silver_con, "SELECT COUNT(*) FROM silver_population")
crimes_count <- DBI::dbGetQuery(silver_con, "SELECT COUNT(*) FROM silver_crimes")


base::message(glue::glue("Silver layer created successfully with:
  - {population_count} population records
  - {crimes_count} crime aggregation records"))

# Close database connections
DBI::dbExecute(silver_con, "DETACH DATABASE bronze_db;")
DBI::dbDisconnect(bronze_con)
DBI::dbDisconnect(silver_con)

# Clean up environment
base::rm(list = base::ls())