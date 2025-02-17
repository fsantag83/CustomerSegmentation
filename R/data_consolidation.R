# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
set.seed(444)

# Ensure required packages are installed
required_pkgs <- c("fastDummies", "magrittr", "readxl", "tidyverse", "duckdb", "janitor", "lubridate","writexl")
install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) install.packages(pkg)
}
invisible(lapply(required_pkgs, install_if_missing))
sapply(required_pkgs, require, character = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define file path
excel_files <- "/Users/fsanta/Library/CloudStorage/OneDrive-Personal/SARLAFT_Risk/Fonedh/InputFiles"

# List all Excel files in the directory
files <- list.files(path = excel_files, pattern = ".xlsx", full.names = TRUE)
rm(excel_files)

# Define a function to enforce data types during import
read_excel_with_types <- function(file, sheet, column_mapping) {
  # Read only the first row to get actual column names
  raw_headers <- readxl::read_excel(file, sheet = sheet, n_max = 1, col_names = TRUE)
  
  # Standardize column names
  colnames_standardized <- janitor::make_clean_names(names(raw_headers))
  
  # Match column types dynamically
  actual_cols <- names(raw_headers)
  col_types <- sapply(actual_cols, function(col) {
    if (col %in% names(column_mapping)) {
      switch(column_mapping[[col]]$type,
             "character" = "text",
             "numeric" = "numeric",
             "factor" = "text",  # Read factors as character initially
             "date" = "date",
             "guess")  # Default to guessing if not mapped
    } else {
      "guess"  # Fallback to guessing for unexpected columns
    }
  })
  
  # Read the full dataset with dynamically assigned column types
  df <- readxl::read_excel(file, sheet = sheet, col_names = TRUE, col_types = col_types)
  
  # Rename columns
  names(df) <- colnames_standardized
  
  # Convert factor columns
  factor_cols <- names(column_mapping)[sapply(column_mapping, function(x) x$type == "factor")]
  df[factor_cols] <- lapply(df[factor_cols], as.factor)
  
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


# Define semester start-end dates
semester_dates <- list(
  "2023-I" = c("2023-01-01", "2023-06-30"),
  "2023-II" = c("2023-07-01", "2023-12-31"),
  "2024-I" = c("2024-01-01", "2024-06-30"),
  "2024-II" = c("2024-07-01", "2024-12-31")
)

# Initialize lists to store data
info_cliente_list <- list()
movimientos_list <- list()

# Read and process files
for (file in files) {
  # Extract semester name from file name
  semester <- tools::file_path_sans_ext(basename(file))
  date_range <- as.Date(semester_dates[[semester]])
  
  # Read and standardize "InformaciónCliente"
  info_cliente <- read_excel_with_types(file, "InformaciónCliente", column_mapping_clientes) %>%
    dplyr::rename_with(~ toupper(.)) %>%  # Convert column names to uppercase for consistency
    dplyr::mutate(SEMESTRE = semester)
  
  # Read and standardize "Movimientos"
  movimientos <- read_excel_with_types(file, "Movimientos", column_mapping_movimientos) %>%
    dplyr::rename_with(~ toupper(.))  # Convert column names to uppercase for consistency
  
  # Ensure FECHA_TRANSACCION exists before processing
  if (!"FECHA_TRANSACCION" %in% names(movimientos)) {
    movimientos$FECHA_TRANSACCION <- NA  # Create column with NA if missing
  }
  
  # Apply transformations safely
  movimientos <- movimientos %>%
    dplyr::mutate(
      SEMESTRE = semester
    ) %>%
    dplyr::filter(FECHA_TRANSACCION >= date_range[1] & FECHA_TRANSACCION <= date_range[2])  # Ensure valid semester dates
  
  # Store in lists
  info_cliente_list[[semester]] <- info_cliente
  movimientos_list[[semester]] <- movimientos
  
  # Cleanup
  rm(info_cliente, movimientos)
}

# Function to add missing columns to ensure proper merging
add_missing_columns <- function(df, expected_cols) {
  missing_cols <- setdiff(expected_cols, names(df))
  df[missing_cols] <- NA
  return(df)
}

# Standardize and merge "InformaciónCliente"
expected_columns_clientes <- names(column_mapping_clientes)
info_cliente_list <- lapply(info_cliente_list, add_missing_columns, expected_columns_clientes)
info_cliente_full <- dplyr::bind_rows(info_cliente_list) %>%
  dplyr::select(dplyr::any_of(expected_columns_clientes))
info_cliente_final <- info_cliente_full %>%
  dplyr::arrange(NUM_IDENTIFICACION, desc(SEMESTRE)) %>%
  dplyr::distinct(NUM_IDENTIFICACION, .keep_all = TRUE)

# Standardize and merge "Movimientos"
expected_columns_movimientos <- names(column_mapping_movimientos)
movimientos_list <- lapply(movimientos_list, add_missing_columns, expected_columns_movimientos)
movimientos_full <- dplyr::bind_rows(movimientos_list) %>%
  dplyr::select(dplyr::any_of(expected_columns_movimientos))

clientes <- info_cliente_final
movimientos <- movimientos_full

rm(list = ls()[-c(2,13)])

clientes <- clientes %>%
  dplyr::semi_join(movimientos, by = "NUM_IDENTIFICACION")

movimientos <- movimientos %>%
  dplyr::semi_join(clientes, by = "NUM_IDENTIFICACION")

# Define the output file path
output_file <- "/Users/fsanta/Library/CloudStorage/OneDrive-Personal/SARLAFT_Risk/Fonedh/InputFiles/Plantilla_EstructuraBase para modelo de segmentacion INF. 2023 - 2024 - Fonedh.xlsx"

# Create a list of dataframes to export
data_to_export <- list(
  "InformaciónCliente" = clientes,
  "Movimientos" = movimientos
)

# Write to Excel with multiple sheets
# writexl::write_xlsx(data_to_export, path = output_file)

rm(data_to_export,output_file)

saveRDS(clientes, "Data/clientes.rds")
saveRDS(movimientos, "Data/movimientos.rds")

rm(list =ls())
