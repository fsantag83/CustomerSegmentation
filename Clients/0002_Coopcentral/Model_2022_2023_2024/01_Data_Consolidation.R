# Silver Layer Creation Script for Coopcentral
# Reads from Bronze layer and creates standardized Silver tables

# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
base::set.seed(444)

# Load required packages with explicit namespaces
required_pkgs <- c("DBI", "duckdb", "dplyr", "tidyr", "lubridate", "glue")
install_if_missing <- function(pkg) {
  if (!base::requireNamespace(pkg, quietly = TRUE)) utils::install.packages(pkg)
}
base::invisible(base::lapply(required_pkgs, install_if_missing))
base::sapply(required_pkgs, require, character.only = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define paths for Bronze and Silver layers
org_id <- "0002_Coopcentral"
bronze_path <- base::file.path("Data", "Bronze", org_id, "Model_2022_2023_2024", paste0(org_id, ".duckdb"))
silver_path <- base::file.path("Data", "Silver", org_id, "Model_2022_2023_2024", paste0(org_id, ".duckdb"))

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

# Check if Bronze tables exist
bronze_tables <- DBI::dbListTables(bronze_con)
if (!("bronze_clientes" %in% bronze_tables) || !("bronze_movimientos" %in% bronze_tables)) {
  base::stop("Required Bronze tables not found. Please ensure 'bronze_clientes' and 'bronze_movimientos' exist.")
}

# First, examine the structure of the bronze tables to get the correct column names
bronze_clientes_cols <- DBI::dbGetQuery(bronze_con, "DESCRIBE bronze_clientes")
bronze_movimientos_cols <- DBI::dbGetQuery(bronze_con, "DESCRIBE bronze_movimientos")

# Find the likely candidate columns for identification and dating
id_col_cliente <- "documento"
date_col_movimiento <- "fecha_transac"
id_col_movimiento <- "num_ident"
jurisdiccion_col <- "jurisdiccion"
monto_col <- "monto"

base::message(glue::glue("Using column mappings:
  Client ID column: {id_col_cliente}
  Transaction ID column: {id_col_movimiento}
  Transaction date column: {date_col_movimiento}
  Jurisdiction column: {jurisdiccion_col}
  Amount column: {monto_col}"))

# Process clients data
base::message("Processing clients data...")

# Check if the year column exists in bronze_clientes, if not, use a default value from source_file
year_col_check <- DBI::dbGetQuery(silver_con, glue::glue("
  SELECT COUNT(*) as count 
  FROM information_schema.columns 
  WHERE table_name = 'bronze_clientes' 
  AND column_name = 'year'
"))

if (year_col_check$count == 0) {
  base::message("Year column not found in bronze_clientes, extracting from source_file")
  # Add a year calculation based on source_file if the column doesn't exist
  year_extraction <- "REGEXP_EXTRACT(source_file, '20[0-9]{2}')"
} else {
  year_extraction <- "year"
}

# Process clients (Keep only the latest year per client)
client_query <- glue::glue("
  CREATE TABLE IF NOT EXISTS silver_clientes_raw AS
  WITH RankedClients AS (
    SELECT 
      c.*,
      UPPER(TRIM(c.\"{id_col_cliente}\")) AS normalized_id,
      {year_extraction} AS year_value,
      ROW_NUMBER() OVER (
        PARTITION BY UPPER(TRIM(c.\"{id_col_cliente}\")) 
        ORDER BY {year_extraction} DESC
      ) AS rn
    FROM bronze_db.bronze_clientes c
  )
  SELECT * FROM RankedClients WHERE rn = 1
")

tryCatch({
  DBI::dbExecute(silver_con, client_query)
  base::message("Successfully created silver_clientes_raw table")
}, error = function(e) {
  base::message(glue::glue("Error creating silver_clientes_raw table: {e$message}"))
  base::message("SQL query was: ", client_query)
  base::stop("Failed to create silver_clientes_raw table")
})

# Process transactions data
base::message("Processing transactions data...")

# Check if the date column is actually a date type or needs conversion
date_type_check <- DBI::dbGetQuery(silver_con, glue::glue("
  SELECT data_type
  FROM information_schema.columns
  WHERE table_name = 'bronze_movimientos'
  AND column_name = '{date_col_movimiento}'
"))

date_conversion <- if (date_type_check$data_type == "DATE") {
  glue::glue("{date_col_movimiento}")
} else {
  glue::glue("TRY_CAST({date_col_movimiento} AS DATE)")
}

# Create a mapping table for jurisdiccion to internacional
base::message("Creating jurisdiction mapping table...")

# Create a dataframe with the mapping
jurisdiction_mapping <- base::data.frame(
  original = c(
    "ALBANIA", "ALEMANIA", "ANDORRA", "ARGENTINA", "ARUBA", "AUSTRALIA", "AUSTRIA", 
    "AZERBAIYÁN", "BELGICA", "BÉLGICA", "BIELORRUSIA", "BOLIVIA, ESTADO PLURINACIONAL DE", 
    "BRASIL", "BULGARIA", "CAMBOYA", "CANADA", "CANADÁ", "CHILE", "CHINA", "CHIPRE", 
    "CIUDAD DEL VATICANO", "COREA DEL SUR", "COSTA RICA", "CROACIA", "CUBA", "CURAÇAO", 
    "CURAZAO", "DINAMARCA", "ECUADOR", "EGIPTO", "EL SALVADOR", "EMIRATOS ARABES UNIDOS", 
    "EMIRATOS ÁRABES UNIDOS", "ESLOVAQUIA", "ESPANA", "ESPAÑA", "ESTADOS UNIDOS", 
    "ESTONIA", "FILIPINAS", "FINLANDIA", "FRANCIA", "GHANA", "GIBRALTAR", "GRECIA", 
    "GUATEMALA", "HONDURAS", "HONG KONG", "HUNGRIA", "HUNGRÍA", "INDIA", "INDONESIA", 
    "IRLANDA", "ISLANDIA", "ISLAS TURCAS Y CAICOS", "ISRAEL", "ITALIA", "JAMAICA", 
    "JAPON", "JORDANIA", "KAZAJISTAN", "KENIA", "LAO, REPUBLICA DEMOCRATICA POPULAR", 
    "LETONIA", "LIECHTENSTEIN", "LITUANIA", "LUXEMBURGO", "MALASIA", "MALTA", "MARRUECOS", 
    "MEXICO", "MÉXICO", "MOLDAVIA, REPUBLICA DE", "MOLDAVIA, REPÚBLICA DE", "MONACO", 
    "MONTENEGRO", "NIGERIA", "NORUEGA", "NUEVA ZELANDA", "PAISES BAJOS", "PAÍSES BAJOS", 
    "PALESTINA, ESTADO DE", "PANAMA", "PANAMÁ", "PARAGUAY", "PERU", "PERÚ", "POLONIA", 
    "PORTUGAL", "PUERTO RICO", "QATAR", "REINO UNIDO", "REPUBLICA CHECA", "REPÚBLICA CHECA", 
    "REPUBLICA DOMINICANA", "REPÚBLICA DOMINICANA", "REUNIÓN", "RUANDA", "RUMANIA", 
    "RUSIA", "RUSIA, FEDERACIÓN DE", "SAN MARTIN (PARTE FRANCESA)", "SINGAPUR", "SUDAFRICA", 
    "SUECIA", "SUIZA", "TAILANDIA", "TAIWAN, PROVINCIA DE CHINA", "TANZANIA, REPUBLICA UNIDA DE", 
    "TURQUIA", "TURQUÍA", "UCRANIA", "UGANDA", "URUGUAY", "VENEZUELA, REPUBLICA BOLIVARIANA DE", 
    "VENEZUELA, REPÚBLICA BOLIVARIANA DE", "VIET NAM", "ZAMBIA", "ARMENIA", "BARRANCABERMEJA", 
    "BARRANQUILLA", "BOGOTA D.C", "BOGOTA D.C.", "BOGOTÁ D.C.", "BUCARAMANGA", "CALI", 
    "CARTAGENA", "CUCUTA", "CÚCUTA", "GIRON", "GIRÓN", "IBAGU?", "IBAGUE", "IBAGUÉ", 
    "MEDELLIN", "NEIVA", "PEREIRA", "SAN GIL", "VILLAVICENCIO", "#N/D"
  ),
  standardized = c(
    "ALBANIA", "ALEMANIA", "ANDORRA", "ARGENTINA", "ARUBA", "AUSTRALIA", "AUSTRIA", 
    "AZERBAIYAN", "BELGICA", "BELGICA", "BIELORRUSIA", "BOLIVIA", "BRASIL", "BULGARIA", 
    "CAMBOYA", "CANADA", "CANADA", "CHILE", "CHINA", "CHIPRE", "CIUDAD DEL VATICANO", 
    "COREA DEL SUR", "COSTA RICA", "CROACIA", "CUBA", "CURAZAO", "CURAZAO", "DINAMARCA", 
    "ECUADOR", "EGIPTO", "EL SALVADOR", "EMIRATOS ARABES UNIDOS", "EMIRATOS ARABES UNIDOS", 
    "ESLOVAQUIA", "ESPANA", "ESPANA", "ESTADOS UNIDOS", "ESTONIA", "FILIPINAS", "FINLANDIA", 
    "FRANCIA", "GHANA", "GIBRALTAR", "GRECIA", "GUATEMALA", "HONDURAS", "HONG KONG", 
    "HUNGRIA", "HUNGRIA", "INDIA", "INDONESIA", "IRLANDA", "ISLANDIA", "ISLAS TURCAS Y CAICOS", 
    "ISRAEL", "ITALIA", "JAMAICA", "JAPON", "JORDANIA", "KAZAJISTAN", "KENIA", "LAOS", 
    "LETONIA", "LIECHTENSTEIN", "LITUANIA", "LUXEMBURGO", "MALASIA", "MALTA", "MARRUECOS", 
    "MEXICO", "MEXICO", "MOLDAVIA", "MOLDAVIA", "MONACO", "MONTENEGRO", "NIGERIA", 
    "NORUEGA", "NUEVA ZELANDA", "PAISES BAJOS", "PAISES BAJOS", "PALESTINA", "PANAMA", 
    "PANAMA", "PARAGUAY", "PERU", "PERU", "POLONIA", "PORTUGAL", "PUERTO RICO", "QATAR", 
    "REINO UNIDO", "REPUBLICA CHECA", "REPUBLICA CHECA", "REPUBLICA DOMINICANA", 
    "REPUBLICA DOMINICANA", "REUNION", "RUANDA", "RUMANIA", "RUSIA", "RUSIA", "SAN MARTIN", 
    "SINGAPUR", "SUDAFRICA", "SUECIA", "SUIZA", "TAILANDIA", "TAIWAN", "TANZANIA", 
    "TURQUIA", "TURQUIA", "UCRANIA", "UGANDA", "URUGUAY", "VENEZUELA", "VENEZUELA", 
    "VIET NAM", "ZAMBIA", "ARMENIA", "BARRANCABERMEJA", "BARRANQUILLA", "BOGOTA", 
    "BOGOTA", "BOGOTA", "BUCARAMANGA", "CALI", "CARTAGENA", "CUCUTA", "CUCUTA", "GIRON", 
    "GIRON", "IBAGUE", "IBAGUE", "IBAGUE", "MEDELLIN", "NEIVA", "PEREIRA", "SAN GIL", 
    "VILLAVICENCIO", NA
  ),
  internacional = c(
    "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", 
    "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", 
    "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", 
    "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", 
    "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", 
    "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", 
    "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", "I", 
    "I", "I", "I", "I", "I", "I", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", 
    "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", "N", NA
  )
)

# Create mapping table in DuckDB
DBI::dbWriteTable(silver_con, "jurisdiction_mapping", jurisdiction_mapping, overwrite = TRUE)

# Initialize year information for each transaction with transformations
transaction_query <- glue::glue("
  CREATE TABLE IF NOT EXISTS silver_movimientos_raw AS
  WITH dated_transactions AS (
    SELECT 
      m.{id_col_movimiento} AS num_ident,
      {date_conversion} AS fecha_transac,
      m.tipo_transac,
      m.tipo_prod,
      m.tipo_canal,
      jm.standardized AS jurisdiccion,  -- Direct replacement with standardized version
      ABS(CAST(m.{monto_col} AS DOUBLE)) AS monto,  -- Directly use absolute value
      m.especie,
      m.nombre_prod,
      m.source_file,
      m.year,
      m.transaction_type,
      UPPER(TRIM(m.\"{id_col_movimiento}\")) AS normalized_id,
      CASE
        WHEN {date_conversion} BETWEEN '2022-01-01' AND '2022-12-31' THEN '2022'
        WHEN {date_conversion} BETWEEN '2023-01-01' AND '2023-12-31' THEN '2023'
        WHEN {date_conversion} BETWEEN '2024-01-01' AND '2024-12-31' THEN '2024'
        ELSE NULL
      END AS calculated_year,
      jm.internacional  -- Keep this as it's valuable business information
    FROM bronze_db.bronze_movimientos m
    LEFT JOIN jurisdiction_mapping jm 
      ON UPPER(TRIM(m.{jurisdiccion_col})) = UPPER(TRIM(jm.original))
    WHERE 
      m.{monto_col} IS NOT NULL 
      AND CAST(m.{monto_col} AS DOUBLE) != 0  -- Filter out NULL or zero monto
  )
  
  SELECT * FROM dated_transactions
")

tryCatch({
  DBI::dbExecute(silver_con, transaction_query)
  
  # Get counts of missing jurisdictions for diagnostics - FIXED COLUMN NAME
  jurisdiccion_stats <- DBI::dbGetQuery(silver_con, "
    SELECT 
      COUNT(*) AS total_transactions,
      SUM(CASE WHEN jurisdiccion IS NULL THEN 1 ELSE 0 END) AS missing_jurisdiccion
    FROM silver_movimientos_raw
  ")
  
  base::message(glue::glue("Successfully created silver_movimientos_raw table"))
  base::message(glue::glue("Total transactions: {jurisdiccion_stats$total_transactions}"))
  base::message(glue::glue("Missing jurisdiction mappings: {jurisdiccion_stats$missing_jurisdiccion}"))
  
}, error = function(e) {
  base::message(glue::glue("Error creating silver_movimientos_raw table: {e$message}"))
  base::message("SQL query was: ", transaction_query)
  base::stop("Failed to create silver_movimientos_raw table")
})

# Apply referential integrity (Optimized filtering using JOINs)
base::message("Applying referential integrity...")

DBI::dbExecute(silver_con, "
  CREATE TABLE silver_clientes_filtered AS
  SELECT DISTINCT c.*
  FROM silver_clientes_raw c
  INNER JOIN silver_movimientos_raw m ON m.normalized_id = c.normalized_id
")

# Fixed query - removed reference to non-existent monto_abs column
DBI::dbExecute(silver_con, "
  CREATE TABLE silver_movimientos_filtered AS
  SELECT DISTINCT m.*
  FROM silver_movimientos_raw m
  INNER JOIN silver_clientes_filtered c ON c.normalized_id = m.normalized_id
")

# Replace Original Tables
DBI::dbExecute(silver_con, "DROP TABLE IF EXISTS silver_movimientos")
DBI::dbExecute(silver_con, "ALTER TABLE silver_movimientos_filtered RENAME TO silver_movimientos")

# Now continue with the silver_clientes transformation...
DBI::dbExecute(silver_con, "DROP TABLE IF EXISTS silver_clientes")
DBI::dbExecute(silver_con, "ALTER TABLE silver_clientes_filtered RENAME TO silver_clientes")

# Apply the jurisdiction mapping as requested
base::message("Mapping jurisdiction codes...")

# First, rename the existing jurisdiccion column to jurisdiccion_o
DBI::dbExecute(silver_con, "ALTER TABLE silver_movimientos RENAME COLUMN jurisdiccion TO jurisdiccion_o")

# Then add the new jurisdiccion column
DBI::dbExecute(silver_con, "ALTER TABLE silver_movimientos ADD COLUMN jurisdiccion VARCHAR")

# Update the new column based on the mapping
jurisdiction_mapping_query <- "
UPDATE silver_movimientos
SET jurisdiccion = CASE
    WHEN internacional = 'I' THEN 'jurisdiccion_6'
    WHEN jurisdiccion_o = 'ARMENIA' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'BARRANCABERMEJA' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'BARRANQUILLA' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'BOGOTA' THEN 'jurisdiccion_5'
    WHEN jurisdiccion_o = 'BUCARAMANGA' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'CALI' THEN 'jurisdiccion_5'
    WHEN jurisdiccion_o = 'CARTAGENA' THEN 'jurisdiccion_3'
    WHEN jurisdiccion_o = 'CUCUTA' THEN 'jurisdiccion_3'
    WHEN jurisdiccion_o = 'GIRON' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'IBAGUE' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'MEDELLIN' THEN 'jurisdiccion_5'
    WHEN jurisdiccion_o = 'NEIVA' THEN 'jurisdiccion_5'
    WHEN jurisdiccion_o = 'PEREIRA' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'SAN GIL' THEN 'jurisdiccion_4'
    WHEN jurisdiccion_o = 'VILLAVICENCIO' THEN 'jurisdiccion_5'
    ELSE 'jurisdiccion_0'
END
"

tryCatch({
  DBI::dbExecute(silver_con, jurisdiction_mapping_query)
  base::message("Successfully mapped jurisdiction codes")
  
  # Get statistics on jurisdiction mappings
  jurisdiction_stats <- DBI::dbGetQuery(silver_con, "
    SELECT 
      jurisdiccion, 
      COUNT(*) as count, 
      (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_movimientos)) as percentage
    FROM silver_movimientos 
    GROUP BY jurisdiccion
    ORDER BY jurisdiccion
  ")
  
  base::message("Jurisdiction code distribution:")
  base::print(jurisdiction_stats)
  
}, error = function(e) {
  base::message(glue::glue("Error mapping jurisdiction codes: {e$message}"))
  base::message("SQL query was: ", jurisdiction_mapping_query)
})

# Apply the client transformations
base::message("Applying data transformations to client data...")

# First, identify the column names we're working with
client_columns <- DBI::dbGetQuery(silver_con, "DESCRIBE silver_clientes")  # Fixed: use silver_clientes instead
column_names <- client_columns$column_name

# Check if required columns exist
has_ingresos_mens <- "ingresos_mens" %in% column_names
has_otros_ingresos <- "otros_ingresos" %in% column_names
has_egresos_mens <- "egresos_mens" %in% column_names
has_total_activos <- "total_activos" %in% column_names
has_total_pasivos <- "total_pasivos" %in% column_names
has_valor_patrimoni <- "valor_patrimoni" %in% column_names

# Construct query based on available columns
ingresos_calculation <- if (has_ingresos_mens && has_otros_ingresos) {
  "COALESCE(ingresos_mens, 0) + COALESCE(otros_ingresos, 0) AS ingresos"
} else if (has_ingresos_mens) {
  "COALESCE(ingresos_mens, 0) AS ingresos"
} else if (has_otros_ingresos) {
  "COALESCE(otros_ingresos, 0) AS ingresos"
} else {
  "NULL AS ingresos"
}

egresos_calculation <- if (has_egresos_mens) {
  "COALESCE(egresos_mens, 0) AS egresos"
} else {
  "NULL AS egresos"
}

activos_calculation <- if (has_total_activos) {
  "COALESCE(total_activos, 0) AS activos"
} else {
  "NULL AS activos"
}

pasivos_calculation <- if (has_total_pasivos) {
  "COALESCE(total_pasivos, 0) AS pasivos"
} else {
  "NULL AS pasivos"
}

# Define columns to exclude from direct selection
columns_to_exclude <- c(
  "ingresos_mens", "otros_ingresos", "egresos_mens", 
  "total_activos", "total_pasivos", "valor_patrimoni"
)

# Create a select clause without the columns we're replacing or removing
select_clause <- base::paste0(
  base::paste(
    base::sapply(
      column_names[!column_names %in% columns_to_exclude],
      function(col) base::paste0("c.", col)
    ),
    collapse = ", "
  ),
  ", ", ingresos_calculation, 
  ", ", egresos_calculation,
  ", ", activos_calculation,
  ", ", pasivos_calculation
)

# Create a new temporary table for the transformed data
transformation_query <- glue::glue("
  CREATE TABLE silver_clientes_transformed AS
  SELECT 
    {select_clause}
  FROM silver_clientes c
")

tryCatch({
  DBI::dbExecute(silver_con, transformation_query)
  base::message("Successfully created silver_clientes_transformed table")
  
  # Replace the original table with the transformed one
  DBI::dbExecute(silver_con, "DROP TABLE silver_clientes")
  DBI::dbExecute(silver_con, "ALTER TABLE silver_clientes_transformed RENAME TO silver_clientes")
  
  # Create indexes for the final table
  DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_cliente_id ON silver_clientes(normalized_id)")
  
}, error = function(e) {
  base::message(glue::glue("Error creating transformed silver_clientes table: {e$message}"))
  base::message("SQL query was: ", transformation_query)
  base::message("Keeping the original silver_clientes table without transformations")
})

# Check if CIIU column exists in silver_clientes
base::message("Checking for CIIU column in silver_clientes...")
ciiu_col_check <- DBI::dbGetQuery(silver_con, "
  SELECT COUNT(*) as count 
  FROM information_schema.columns 
  WHERE table_name = 'silver_clientes' 
  AND column_name = 'ciiu'
")

if (ciiu_col_check$count > 0) {
  base::message("CIIU column found in silver_clientes, applying mapping...")
  
  # Add a copy of the original CIIU column
  DBI::dbExecute(silver_con, "ALTER TABLE silver_clientes ADD COLUMN ciiu_o VARCHAR")
  DBI::dbExecute(silver_con, "UPDATE silver_clientes SET ciiu_o = ciiu")
  
  # Update the CIIU column based on the mapping
  ciiu_mapping_query <- "
  UPDATE silver_clientes
  SET ciiu = CASE
      WHEN ciiu IS NULL OR TRIM(ciiu) = '' THEN NULL
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) IS NULL THEN 'No Clasificado'
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) BETWEEN 1 AND 9 THEN 'Agricultura y Minería'
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) BETWEEN 10 AND 12 THEN 'Industria de Alimentos y Bebidas'
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) BETWEEN 13 AND 33 THEN 'Industria Manufacturera'
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) BETWEEN 41 AND 43 THEN 'Construcción e Infraestructura'
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) BETWEEN 45 AND 47 THEN 'Comercio al por Mayor y por Menor'
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) BETWEEN 50 AND 82 THEN 'Servicios Profesionales y Técnicos'
      WHEN TRY_CAST(SUBSTRING(ciiu, 1, 2) AS INTEGER) BETWEEN 90 AND 99 THEN 'Otros Servicios'
      ELSE 'No Clasificado'
  END
  "
  
  tryCatch({
    DBI::dbExecute(silver_con, ciiu_mapping_query)
    base::message("Successfully mapped CIIU codes")
    
    # Get statistics on CIIU mappings
    ciiu_stats <- DBI::dbGetQuery(silver_con, "
      SELECT 
        ciiu, 
        COUNT(*) as count, 
        (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_clientes)) as percentage
      FROM silver_clientes 
      GROUP BY ciiu
      ORDER BY count DESC
    ")
    
    base::message("CIIU code distribution:")
    base::print(ciiu_stats)
    
  }, error = function(e) {
    base::message(glue::glue("Error mapping CIIU codes: {e$message}"))
    base::message("SQL query was: ", ciiu_mapping_query)
  })
} else {
  base::message("CIIU column not found in silver_clientes, skipping mapping")
}

# Indexing after Filtering
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_mov_id ON silver_movimientos(normalized_id)")
DBI::dbExecute(silver_con, glue::glue("CREATE INDEX IF NOT EXISTS idx_mov_date ON silver_movimientos({date_col_movimiento})"))
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_mov_internacional ON silver_movimientos(internacional)")
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_mov_jurisdiccion ON silver_movimientos(jurisdiccion)")
DBI::dbExecute(silver_con, "CREATE INDEX IF NOT EXISTS idx_cliente_ciiu ON silver_clientes(ciiu)")

# Optimize Database Performance
DBI::dbExecute(silver_con, "VACUUM")
DBI::dbExecute(silver_con, "ANALYZE")

# Get counts for verification
cliente_count <- DBI::dbGetQuery(silver_con, "SELECT COUNT(*) FROM silver_clientes")
movimiento_count <- DBI::dbGetQuery(silver_con, "SELECT COUNT(*) FROM silver_movimientos")
internacional_stats <- DBI::dbGetQuery(silver_con, "
  SELECT 
    internacional, 
    COUNT(*) as count, 
    (COUNT(*) * 100.0 / (SELECT COUNT(*) FROM silver_movimientos)) as percentage
  FROM silver_movimientos 
  GROUP BY internacional
  ORDER BY internacional
")

base::message(glue::glue("Silver layer created successfully with {cliente_count} clients and {movimiento_count} transactions"))
base::message("Internacional transaction distribution:")
base::print(internacional_stats)

# Clean up mapping table
DBI::dbExecute(silver_con, "DROP TABLE IF EXISTS jurisdiction_mapping")

# Close database connections
DBI::dbExecute(silver_con, "DETACH DATABASE bronze_db;")
DBI::dbDisconnect(bronze_con)
DBI::dbDisconnect(silver_con)

# Clean up environment
base::rm(list = base::ls())
