# ========================================================================
# ORGANIZATION 0: LAFT CRIMES - MS EXCEL FILES: CRIME AND POPULATION DATA
# ========================================================================

# Clear environment
base::rm(list = base::ls())

# Load required packages with explicit namespaces
required_pkgs <- c("DBI", "duckdb", "readxl", "dplyr", "glue", "stringr")
install_if_missing <- function(pkg) {
  if (!base::requireNamespace(pkg, quietly = TRUE)) utils::install.packages(pkg)
}
base::invisible(base::lapply(required_pkgs, install_if_missing))
base::sapply(required_pkgs, require, character.only = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define organization ID and paths
org_id_laft <- "0000_CrimesLAFT"
bronze_dir_laft <- base::file.path("Data", "Bronze", org_id_laft, "2022_2023_2024")
bronze_db_laft <- base::file.path(bronze_dir_laft, paste0(org_id_laft, ".duckdb"))

# Create Bronze directory structure if it doesn't exist
if (!base::dir.exists(bronze_dir_laft)) {
  base::dir.create(bronze_dir_laft, recursive = TRUE, mode = "0755")
  base::message(glue::glue("Created Bronze directory: {bronze_dir_laft}"))
}

# Initialize DuckDB connection
con_laft <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = bronze_db_laft,
  config = base::list(
    memory_limit = "8GB",
    threads = "4"
  ),
  read_only = FALSE
)

# Define schema for crimes table
DBI::dbExecute(con_laft, "DROP TABLE IF EXISTS bronze_crimes")
DBI::dbExecute(con_laft, "
  CREATE TABLE bronze_crimes (
    armas_medios VARCHAR,
    departamento VARCHAR,
    municipio VARCHAR,
    fecha DATE,
    codigo_dane VARCHAR,
    cantidad INTEGER,
    source_file VARCHAR,
    year VARCHAR
  )
")

# Define schema for population table
DBI::dbExecute(con_laft, "DROP TABLE IF EXISTS bronze_population")
DBI::dbExecute(con_laft, "
  CREATE TABLE bronze_population (
    cod_dpto VARCHAR,
    departamento VARCHAR,
    cod_dpto_mpio VARCHAR,
    municipio VARCHAR,
    anio INTEGER,
    area_geografica VARCHAR,
    total INTEGER,
    source_file VARCHAR,
    year VARCHAR
  )
")

# Function to extract year from filename
extract_year_laft <- function(filename) {
  year_pattern <- stringr::str_extract(filename, "20(22|23|24)")
  return(base::ifelse(base::is.na(year_pattern), "", year_pattern))
}

# Function to load crimes data from Excel files
load_crimes_excel <- function(file_path) {
  file_name <- base::basename(file_path)
  year <- extract_year_laft(file_name)
  
  tryCatch({
    # Read data starting from row 10 (with column names)
    crimes_data <- readxl::read_excel(
      file_path,
      skip = 9,  # Skip 9 rows to start at row 10
      col_types = c(
        "text",    # ARMAS_MEDIOS
        "text",    # DEPARTAMENTO
        "text",    # MUNICIPIO
        "date",    # FECHA (dd.mm.yyyy)
        "text",    # CODIGO_DANE
        "numeric"  # CANTIDAD
      )
    )
    
    # Ensure columns have the correct names
    base::names(crimes_data) <- c(
      "armas_medios", "departamento", "municipio", 
      "fecha", "codigo_dane", "cantidad"
    )
    
    # Add source file and year
    crimes_data <- crimes_data |>
      dplyr::mutate(
        source_file = file_name,
        year = year
      )
    
    # Write to DuckDB
    DBI::dbAppendTable(con_laft, "bronze_crimes", crimes_data)
    base::message(glue::glue("Processed crimes file: {file_name}"))
    
  }, error = function(e) {
    base::message(glue::glue("Error processing crime file {file_name}: {e$message}"))
    
    # Fallback method with more explicit control
    tryCatch({
      # Read raw data with custom settings
      df_raw <- readxl::read_excel(
        file_path,
        skip = 9,
        col_names = FALSE,
        col_types = "text"
      )
      
      # Manually create the dataframe with correct types
      crimes_df <- base::data.frame(
        armas_medios = base::as.character(df_raw[[1]]),
        departamento = base::as.character(df_raw[[2]]),
        municipio = base::as.character(df_raw[[3]]),
        fecha = base::as.Date(df_raw[[4]], format = "%d.%m.%Y"),
        codigo_dane = base::as.character(df_raw[[5]]),
        cantidad = base::suppressWarnings(base::as.integer(df_raw[[6]])),
        source_file = file_name,
        year = year
      )
      
      # Write to database
      DBI::dbAppendTable(con_laft, "bronze_crimes", crimes_df)
      base::message(glue::glue("Processed crimes file using fallback method: {file_name}"))
      
    }, error = function(e2) {
      base::message(glue::glue("All methods failed for crimes file {file_name}: {e2$message}"))
    })
  })
}

# Function to load population data from Excel files
load_population_excel <- function(file_path) {
  file_name <- base::basename(file_path)
  year <- extract_year_laft(file_name)
  
  tryCatch({
    # Read data starting from row 12 (with column names)
    population_data <- readxl::read_excel(
      file_path,
      skip = 11,
      col_names = TRUE,
      col_types = c("text", "text", "text", "text", "numeric", "text", "numeric", 
                    "guess", "guess", "guess", "guess", "guess", "guess")
    )
    
    
    # Select just the needed columns (A to G)
    population_data <- population_data[, 1:7]
    
    # Ensure columns have the correct names
    base::names(population_data) <- c(
      "cod_dpto", "departamento", "cod_dpto_mpio", 
      "municipio", "anio", "area_geografica", "total"
    )
    
    # Add source file and year
    population_data <- population_data |>
      dplyr::mutate(
        source_file = file_name,
        year = year
      )
    
    # Write to DuckDB
    DBI::dbAppendTable(con_laft, "bronze_population", population_data)
    base::message(glue::glue("Processed population file: {file_name}"))
    
  }, error = function(e) {
    base::message(glue::glue("Error processing population file {file_name}: {e$message}"))
    
    # Fallback method with more explicit control
    tryCatch({
      # Read raw data with custom settings
      df_raw <- readxl::read_excel(
        file_path,
        skip = 11,
        col_names = FALSE,
        col_types = "text",
        n_max = 1122  # Prevent reading beyond data area
      )
      
      # Manually create the dataframe with correct types
      pop_df <- base::data.frame(
        cod_dpto = base::as.character(df_raw[[1]]),
        departamento = base::as.character(df_raw[[2]]),
        cod_dpto_mpio = base::as.character(df_raw[[3]]),
        municipio = base::as.character(df_raw[[4]]),
        anio = base::suppressWarnings(base::as.integer(df_raw[[5]])),
        area_geografica = base::as.character(df_raw[[6]]),
        total = base::suppressWarnings(base::as.integer(df_raw[[7]])),
        source_file = file_name,
        year = year
      )
      
      # Write to database
      DBI::dbAppendTable(con_laft, "bronze_population", pop_df)
      base::message(glue::glue("Processed population file using fallback method: {file_name}"))
      
    }, error = function(e2) {
      base::message(glue::glue("All methods failed for population file {file_name}: {e2$message}"))
    })
  })
}

# Get all crime Excel files
crimes_files <- base::list.files(
  path = base::file.path(bronze_dir_laft, "Crimes"),
  pattern = "\\.xlsx$",
  full.names = TRUE
)

base::message(glue::glue("Found {base::length(crimes_files)} crime Excel files to process"))

# Process all crime files
base::invisible(base::lapply(crimes_files, load_crimes_excel))

# Get all population Excel files
population_files <- base::list.files(
  path = base::file.path(bronze_dir_laft, "Population"),
  pattern = "\\.xlsx$",
  full.names = TRUE
)

base::message(glue::glue("Found {base::length(population_files)} population Excel files to process"))

# Process all population files
base::invisible(base::lapply(population_files, load_population_excel))

# After processing all crime files but before creating indexes

# Delete NA department rows from crimes
base::message("Deleting rows with NA departments from crimes...")
DBI::dbExecute(con_laft, "DELETE FROM bronze_crimes WHERE departamento IS NULL")
base::message("Remaining crime records:", 
              DBI::dbGetQuery(con_laft, "SELECT COUNT(*) FROM bronze_crimes")$count)

# Create indexes for better performance
DBI::dbExecute(con_laft, "CREATE INDEX IF NOT EXISTS idx_crimes_departamento ON bronze_crimes(departamento)")
DBI::dbExecute(con_laft, "CREATE INDEX IF NOT EXISTS idx_crimes_municipio ON bronze_crimes(municipio)")
DBI::dbExecute(con_laft, "CREATE INDEX IF NOT EXISTS idx_crimes_fecha ON bronze_crimes(fecha)")
DBI::dbExecute(con_laft, "CREATE INDEX IF NOT EXISTS idx_crimes_codigo_dane ON bronze_crimes(codigo_dane)")

DBI::dbExecute(con_laft, "CREATE INDEX IF NOT EXISTS idx_population_cod_dpto ON bronze_population(cod_dpto)")
DBI::dbExecute(con_laft, "CREATE INDEX IF NOT EXISTS idx_population_cod_dpto_mpio ON bronze_population(cod_dpto_mpio)")
DBI::dbExecute(con_laft, "CREATE INDEX IF NOT EXISTS idx_population_anio ON bronze_population(anio)")

# Check results
crimes_count <- DBI::dbGetQuery(con_laft, "SELECT COUNT(*) FROM bronze_crimes")
population_count <- DBI::dbGetQuery(con_laft, "SELECT COUNT(*) FROM bronze_population")

base::message(glue::glue("Loaded {crimes_count} crime records"))
base::message(glue::glue("Loaded {population_count} population records"))

# Verify data quality with sample counts by department
dept_crime_counts <- DBI::dbGetQuery(con_laft, "
  SELECT departamento, COUNT(*) as record_count 
  FROM bronze_crimes 
  GROUP BY departamento 
  ORDER BY record_count DESC 
  LIMIT 10
")

base::message("Top 10 departments by crime count:")
base::print(dept_crime_counts)

dept_population_counts <- DBI::dbGetQuery(con_laft, "
  SELECT departamento, SUM(total) as total_population 
  FROM bronze_population 
  WHERE area_geografica = 'Total' 
  GROUP BY departamento 
  ORDER BY total_population DESC 
  LIMIT 10
")

base::message("Top 10 departments by population:")
base::print(dept_population_counts)

# Close connection
DBI::dbDisconnect(con_laft, shutdown = TRUE)
base::message(glue::glue("Bronze layer for {org_id_laft} complete"))
base::rm(list = base::ls())


# ========================================================================
# ORGANIZATION 1: FONEDH - MS EXCEL FILE PROCESSING
# ========================================================================

# Bronze Layer Initialization Script

# Clean environment
base::rm(list = base::ls())

###### Support functions to load  MS Excel files

# Function to load Excel sheets to Bronze
load_excel_to_bronze <- function(file_path) {
  tryCatch({
    # Extract semester from filename (assumes format: "filename_YYYY-I.xlsx")
    file_name <- base::basename(file_path)
    semestre <- base::sub(".*_(\\d{4}-[I|II])\\.xlsx$", "\\1", file_name)
    
    # Process client information
    clientes <- read_excel_with_types(file_path, "InformaciónCliente", column_mapping_clientes) %>%
      dplyr::mutate(source_file = file_name, semestre = semestre)
    
    # Process transactions
    movimientos <- read_excel_with_types(file_path, "Movimientos", column_mapping_movimientos) %>%
      dplyr::mutate(source_file = file_name, semestre = semestre)
    
    # Write to DuckDB
    DBI::dbWriteTable(con, "bronze_clientes", clientes, append = TRUE)
    DBI::dbWriteTable(con, "bronze_movimientos", movimientos, append = TRUE)
    
    base::message(glue::glue("Loaded {file_name} successfully"))
    
  }, error = function(e) {
    base::message(glue::glue("Error in {file_name}: {e$message}"))
  })
}

# Define column type conversion function
read_excel_with_types <- function(file, sheet, column_mapping) {
  # Read headers first
  raw_headers <- readxl::read_excel(file, sheet = sheet, n_max = 1, col_names = TRUE)
  
  # Clean column names
  colnames_clean <- janitor::make_clean_names(base::names(raw_headers))
  
  # Create type mapping vector
  col_types <- base::sapply(
    base::names(raw_headers),
    function(col) {
      if (col %in% base::names(column_mapping)) {
        base::switch(column_mapping[[col]]$type,
                     "character" = "text",
                     "numeric" = "numeric",
                     "date" = "date",
                     "guess")
      } else {
        "guess"
      }
    }
  )
  
  # Read data with enforced types
  df <- readxl::read_excel(
    file,
    sheet = sheet,
    col_types = col_types,
    col_names = TRUE
  )
  
  # Apply cleaned names
  base::names(df) <- colnames_clean
  
  return(df)
}


# Define a column name mapping dictionary
column_mapping_clientes <- list(
  "TIPO_IDENTIFICACION" = list("TIPO_IDENTIFICACION", type = "character"),
  "NUM_IDENTIFICACION" = list("NUM_IDENTIFICACION", type = "character"),
  "COD_MUNICIPIO" = list("COD_MUNICIPIO", type = "character"),
  "NOMBRE_MUNICIPIO" = list("NOMBRE_MUNICIPIO", type = "character"),
  "DEPARTAMENTO" = list("DEPARTAMENTO", type = "character"),
  "FECHA_INGRESO" = list("FECHA_INGRESO", type = "date"),
  "FECHA_NACIMIENTO" = list("FECHA_NACIMIENTO", type = "date"),
  "ROL" = list("ROL", type = "character"),
  "ACTIVO" = list("ACTIVO", type = "character"),
  "GENERO" = list("GENERO", type = "character"),
  "TIPO_CONTRATO" = list("TIPO_CONTRATO", type = "character"),
  "NIVEL_ESCOLARIDAD" = list("NIVEL_ESCOLARIDAD", type = "character"),
  "ESTRATO" = list("ESTRATO", type = "character"),
  "ESTADO_CIVIL" = list("ESTADO_CIVIL", type = "character"),
  "MUJER_CAB_FAMILIA" = list("MUJER_CAB_FAMILIA", type = "character"),
  "NUMERO_HIJOS" = list("NUMERO_HIJOS", type = "numeric"),                  
  "OCUPACION" = list("OCUPACION", type = "character"),
  "SECTOR_ECONOMICO" = list("SECTOR_ECONOMICO", type = "character"),
  "ACTIVIDAD_ECONOMICA" = list("ACTIVIDAD_ECONOMICA", type = "character"),
  "CIIU" = list("CIIU", type = "character"),
  "TRANSACCIONES_INTERNACIONALES" = list("TRANSACCIONES_INTERNACIONALES", type = "character"),
  "SALARIO_ACTUAL" = list("SALARIO_ACTUAL", type = "numeric"),
  "OTROS_INGRESOS" = list("OTROS_INGRESOS", type = "numeric"),
  "ACTIVOS" = list("ACTIVOS", type = "numeric"),
  "PASIVOS" = list("PASIVOS", type = "numeric"),
  "PATRIMONIO" = list("PATRIMONIO", type = "numeric"),
  "EGRESOS" = list("EGRESOS", type = "numeric"),
  "SEMESTRE" = list("SEMESTRE", type = "character"))

column_mapping_movimientos <- list(
  "TIPO_IDENTIFICACION" = list("TIPO_IDENTIFICACION", type = "character"),
  "NUM_IDENTIFICACION" = list("NUM_IDENTIFICACION", type = "character"),
  "FECHA_TRANSACCION" = list("FECHA_TRANSACCION", type = "date"),
  "TIPO_PRODUCTO" = list("TIPO_PRODUCTO", type = "character"),           
  "ID_PRODUCTO" = list("ID_PRODUCTO", type = "character"),
  "TIPO_TRANSACCION" = list("TIPO_TRANSACCION", type = "character"),
  "CODIGO_TRANSACCION" = list("CODIGO_TRANSACCION", type = "character"),
  "MEDIO_TRANSACCION" = list("MEDIO_TRANSACCION", type = "character"),      
  "TIPO_CANAL_TRANSACCION" = list("TIPO_CANAL_TRANSACCION", type = "character"),
  "CANAL" = list("CANAL", type = "character"),
  "MUNICIPIO_TRANSACCION" = list("MUNICIPIO_TRANSACCION", type = "character"),
  "DEPARTAMENTO_TRANSACCION" = list("DEPARTAMENTO_TRANSACCION", type = "character"),
  "MONTO_TRANSACCION" = list("MONTO_TRANSACCION", type = "numeric"),
  "PRODUCTO_DESTINO" = list("PRODUCTO_DESTINO", type = "character"),
  "TIPO_PRODUCTO_DESTINO" = list("TIPO_PRODUCTO_DESTINO", type = "character"),
  "ENTIDAD_PRODUCTO_DESTINO" = list("ENTIDAD_PRODUCTO_DESTINO", type = "character"),
  "ID_BENEFICIARIO" = list("ID_BENEFICIARIO", type = "character"),
  "NOMBRE_BENEFICIARIO" = list("NOMBRE_BENEFICIARIO", type = "character"),
  "SEMESTRE" = list("SEMESTRE", type = "character")
)

# Load required packages with explicit namespaces
if (!base::requireNamespace("DBI", quietly = TRUE)) utils::install.packages("DBI")
if (!base::requireNamespace("duckdb", quietly = TRUE)) utils::install.packages("duckdb")
if (!base::requireNamespace("readxl", quietly = TRUE)) utils::install.packages("readxl")

# Set paths
org_id <- "0001_Fonedh"
bronze_dir <- base::file.path("Data", "Bronze", org_id,"Model_2023_2024")
bronze_db <- base::file.path(bronze_dir, paste0(org_id,".duckdb"))

# Create Bronze directory structure
if (!base::dir.exists(bronze_dir)) {
  base::dir.create(bronze_dir, recursive = TRUE, mode = "0755")
  base::message(glue::glue("Created Bronze directory: {bronze_dir}"))
}

# Initialize DuckDB connection
con <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = bronze_db,
  config = base::list(
    memory_limit = "8GB",
    threads = "4"
  ),
  read_only = FALSE
)

# Process all Excel files in raw input directory
raw_files <- base::list.files(
  path = "Data/Bronze/0001_Fonedh/Model_2023_2024",
  pattern = "\\.xlsx$",
  full.names = TRUE
)

base::message(glue::glue("Found {base::length(raw_files)} Excel files to process"))

# Load files to Bronze layer
base::invisible(base::lapply(raw_files, load_excel_to_bronze))

# Verify tables
tables <- DBI::dbListTables(con)
base::message(glue::glue("Bronze layer contains tables: {base::toString(tables)}"))

# Cleanup
DBI::dbDisconnect(con, shutdown = TRUE)
base::message("Bronze layer initialization complete")
base::rm(list = base::ls())


# ========================================================================
# ORGANIZATION 2: COOPCENTRAL - PIPE-DELIMITED TEXT FILE PROCESSING
# ========================================================================

# Required packages for text file processing
if (!base::requireNamespace("data.table", quietly = TRUE)) utils::install.packages("data.table")
if (!base::requireNamespace("lubridate", quietly = TRUE)) utils::install.packages("lubridate")
if (!base::requireNamespace("stringr", quietly = TRUE)) utils::install.packages("stringr")

# Define organization ID and paths
org_id_2 <- "0002_Coopcentral"
bronze_dir_2 <- base::file.path("Data", "Bronze", org_id_2, "Model_2022_2023_2024")
bronze_db_2 <- base::file.path(bronze_dir_2, paste0(org_id_2, ".duckdb"))

# Create Bronze directory structure
if (!base::dir.exists(bronze_dir_2)) {
  base::dir.create(bronze_dir_2, recursive = TRUE, mode = "0755")
  base::message(glue::glue("Created Bronze directory: {bronze_dir_2}"))
}

# Initialize DuckDB connection
con_2 <- DBI::dbConnect(
  duckdb::duckdb(),
  dbdir = bronze_db_2,
  config = base::list(
    memory_limit = "8GB",
    threads = "4"
  ),
  read_only = FALSE
)

# Extract year from filename
extract_year <- function(filename) {
  year_pattern <- stringr::str_extract(filename, "20(22|23|24)")
  return(base::ifelse(base::is.na(year_pattern), "", year_pattern))
}

# Create schema-specific client table
DBI::dbExecute(con_2, "DROP TABLE IF EXISTS bronze_clientes")
DBI::dbExecute(con_2, "
  CREATE TABLE bronze_clientes (
    oficina VARCHAR,
    tipo_documento VARCHAR,
    documento VARCHAR,
    ciiu VARCHAR,
    ocupacion VARCHAR,
    ingresos_mens DOUBLE,
    egresos_mens DOUBLE, 
    otros_ingresos DOUBLE,
    total_activos DOUBLE,
    total_pasivos DOUBLE,
    valor_patrimoni DOUBLE,
    estado VARCHAR,
    source_file VARCHAR,
    year VARCHAR
  )
")

# Create schema-specific transaction table
DBI::dbExecute(con_2, "DROP TABLE IF EXISTS bronze_movimientos")
DBI::dbExecute(con_2, "
  CREATE TABLE bronze_movimientos (
    num_ident VARCHAR,
    fecha_transac DATE,
    tipo_transac VARCHAR,
    tipo_prod VARCHAR,
    tipo_canal VARCHAR,
    jurisdiccion VARCHAR,
    monto DOUBLE,
    especie VARCHAR,
    nombre_prod VARCHAR,
    source_file VARCHAR,
    year VARCHAR,
    transaction_type VARCHAR
  )
")


# Process client files with explicit schema
load_client_file <- function(file_path) {
  file_name <- base::basename(file_path)
  year <- extract_year(file_name)
  
  tryCatch({
    # First read just the headers to get exact column names
    headers <- data.table::fread(
      file_path,
      sep = "|",
      nrows = 0,
      header = TRUE
    )
    actual_col_names <- base::names(headers)
    
    # Build column selection part first
    col_selections <- base::paste0(
      '"', actual_col_names[1], '" AS oficina, ',
      '"', actual_col_names[2], '" AS tipo_documento, ',
      '"', actual_col_names[3], '" AS documento, ',
      '"', actual_col_names[4], '" AS ciiu, ',
      '"', actual_col_names[5], '" AS ocupacion, ',
      'TRY_CAST("', actual_col_names[6], '" AS DOUBLE) AS ingresos_mens, ',
      'TRY_CAST("', actual_col_names[7], '" AS DOUBLE) AS egresos_mens, ',
      'TRY_CAST("', actual_col_names[8], '" AS DOUBLE) AS otros_ingresos, ',
      'TRY_CAST("', actual_col_names[9], '" AS DOUBLE) AS total_activos, ',
      'TRY_CAST("', actual_col_names[10], '" AS DOUBLE) AS total_pasivos, ',
      'TRY_CAST("', actual_col_names[11], '" AS DOUBLE) AS valor_patrimoni, ',
      '"', actual_col_names[12], '" AS estado'
    )
    
    # Create final query with standard glue
    query <- glue::glue("
      INSERT INTO bronze_clientes 
      SELECT 
        {col_selections},
        '{file_name}' AS source_file,
        '{year}' AS year
      FROM read_csv_auto(
        '{file_path}', 
        delim='|', 
        header=true, 
        skip=0,
        all_varchar=true,
        ignore_errors=true
      )
    ")
    
    DBI::dbExecute(con_2, query)
    base::message(glue::glue("Loaded client data from {file_name}"))
    
  }, error = function(e) {
    base::message(glue::glue("Error processing client file {file_name}: {e$message}"))
    
    # Alternative direct approach without SQL
    tryCatch({
      # Read data using data.table
      df <- data.table::fread(
        file_path,
        sep = "|",
        header = TRUE,
        colClasses = "character",
        encoding = "UTF-8",
        data.table = FALSE
      )
      
      # Create a mapping of existing columns to desired columns
      if (base::ncol(df) >= 12) {
        result <- base::data.frame(
          oficina = df[[1]],
          tipo_documento = df[[2]],
          documento = df[[3]],
          ciiu = df[[4]],
          ocupacion = df[[5]],
          ingresos_mens = base::suppressWarnings(base::as.numeric(df[[6]])),
          egresos_mens = base::suppressWarnings(base::as.numeric(df[[7]])),
          otros_ingresos = base::suppressWarnings(base::as.numeric(df[[8]])),
          total_activos = base::suppressWarnings(base::as.numeric(df[[9]])),
          total_pasivos = base::suppressWarnings(base::as.numeric(df[[10]])),
          valor_patrimoni = base::suppressWarnings(base::as.numeric(df[[11]])),
          estado = df[[12]],
          source_file = file_name,
          year = year
        )
        
        # Write directly to database
        DBI::dbWriteTable(con_2, "bronze_clientes", result, append = TRUE)
        base::message(glue::glue("Loaded client data from {file_name} using direct method"))
      } else {
        base::message(glue::glue("File {file_name} has insufficient columns: {base::ncol(df)} found, 12+ needed"))
      }
    }, error = function(e2) {
      base::message(glue::glue("All import methods failed for {file_name}: {e2$message}"))
    })
  })
}

# Process transaction files with explicit schema
load_transaction_file <- function(file_path) {
  file_name <- base::basename(file_path)
  year <- extract_year(file_name)
  tx_type <- stringr::str_extract(file_name, "^[^_]+")
  
  tryCatch({
    # Use DuckDB's native CSV reader with safe conversions
    query <- glue::glue("
    INSERT INTO bronze_movimientos
    SELECT 
      CAST(num_ident AS VARCHAR),
      TRY_STRPTIME(fecha_transac, '%d/%m/%Y'),
      CAST(tipo_transac AS VARCHAR),
      CAST(tipo_prod AS VARCHAR),
      CAST(tipo_canal AS VARCHAR),
      CAST(jurisdiccion AS VARCHAR),
      TRY_CAST(monto AS DOUBLE),
      CAST(especie AS VARCHAR),
      CAST(nombre_prod AS VARCHAR),
      '{file_name}' AS source_file,
      '{year}' AS year,
      '{tx_type}' AS transaction_type
    FROM read_csv_auto(
      '{file_path}', 
      delim='|', 
      header=true, 
      nullstr='',
      auto_detect=true,
      sample_size=1000,
      all_varchar=true
    )
  ")
    
    DBI::dbExecute(con_2, query)
    base::message(glue::glue("Loaded transaction data from {file_name}"))
    
  }, error = function(e) {
    base::message(glue::glue("Error processing transaction file {file_name}: {e$message}"))
    
    # Fallback method for problematic files
    tryCatch({
      # Try with relaxed settings
      fallback_query <- glue::glue("
       PRAGMA copy_atomicity='statement';
       PRAGMA ignore_errors_in_dates=true;
        
       INSERT INTO bronze_movimientos
       SELECT 
         CAST(c1 AS VARCHAR) AS num_ident,
         NULL AS fecha_transac,
         CAST(c3 AS VARCHAR) AS tipo_transac,
         CAST(c4 AS VARCHAR) AS tipo_prod,
         CAST(c5 AS VARCHAR) AS tipo_canal,
         CAST(c6 AS VARCHAR) AS jurisdiccion,
         TRY_CAST(c7 AS DOUBLE) AS monto,
         CAST(c8 AS VARCHAR) AS especie,
         CAST(c9 AS VARCHAR) AS nombre_prod,
         '{file_name}' AS source_file,
         '{year}' AS year,
         '{tx_type}' AS transaction_type
       FROM read_csv(
         '{file_path}', 
         delim='|', 
         header=true,
         columns={'c1': 'VARCHAR', 'c2': 'VARCHAR', 'c3': 'VARCHAR', 'c4': 'VARCHAR',
                  'c5': 'VARCHAR', 'c6': 'VARCHAR', 'c7': 'VARCHAR', 'c8': 'VARCHAR',
                  'c9': 'VARCHAR'},
         null_padding=true,
         ignore_errors=true
      )
  ")
      
      DBI::dbExecute(con_2, fallback_query)
      base::message(glue::glue("Loaded transaction data from {file_name} using fallback method"))
    }, error = function(e2) {
      base::message(glue::glue("Fallback import failed for {file_name}: {e2$message}"))
    })
  })
}

# Get all text files
txt_files <- base::list.files(
  path = bronze_dir_2,
  pattern = "\\.txt$",
  full.names = TRUE
)

base::message(glue::glue("Found {base::length(txt_files)} text files to process"))

# Process each file based on its type
for (file in txt_files) {
  file_name <- base::basename(file)
  
  # Identify file type
  if (base::grepl("Base Clientes", file_name)) {
    load_client_file(file)
  } else {
    load_transaction_file(file)
  }
}

# Check results
cliente_count <- DBI::dbGetQuery(con_2, "SELECT COUNT(*) FROM bronze_clientes")
movimiento_count <- DBI::dbGetQuery(con_2, "SELECT COUNT(*) FROM bronze_movimientos")

base::message(glue::glue("Loaded {cliente_count} client records"))
base::message(glue::glue("Loaded {movimiento_count} transaction records"))

# Close connection
DBI::dbDisconnect(con_2, shutdown = TRUE)
base::message(glue::glue("Bronze layer for {org_id_2} complete"))
base::rm(list = base::ls())
