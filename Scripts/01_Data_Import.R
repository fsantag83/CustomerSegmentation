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