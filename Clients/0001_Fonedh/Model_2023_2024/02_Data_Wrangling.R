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

clientes <- DBI::dbGetQuery(gold_con, "SELECT * FROM silver_db.silver_clientes") 

movimientos <- DBI::dbGetQuery(gold_con, "SELECT * FROM silver_db.silver_movimientos")


# Data preparation
clientes <- clientes %>%
  dplyr::filter(rol == "Asociado") # Seleccionando solo asociados (vigentes)

movimientos <- movimientos %>%
  dplyr::semi_join(clientes, by = "num_identificacion") # Seleccionando solo transacciones con asociado

clientes <- clientes %>%
  dplyr::semi_join(movimientos, by = "num_identificacion") # Seleccionando solo asociados con transacciones

reference_date <- as.Date("2024-12-31")

clientes <- clientes %>%
  dplyr::mutate(
    edad = as.numeric(difftime(reference_date, fecha_nacimiento, units = "days")) %/% 365, #EDAD al 2024-12-31
    antiguedad = as.numeric(difftime(reference_date, fecha_ingreso, units = "days")) %/% 365, #ANTIGUEDAD al 2024-12-31
    tipo_contrato = dplyr::if_else(actividad_economica == "Independiente","Prestación Servicios",tipo_contrato), #Independiente no tiene contrato indefinido
    tipo_contrato = factor(dplyr::if_else(tipo_contrato == "Término Indefinido","Indefinido","Otro")), # Otro es: Otro, Prestación Servicios, Temporal, Término Fijo
    actividad_economica = factor(dplyr::if_else(actividad_economica == "Asalariado","Asalariado","Otro")), # Otro es Independiente o pensionado 
    nivel_escolaridad = factor(dplyr::if_else(nivel_escolaridad %in% c("Primaria","Bachillerato"), "01_Basico",
                                              dplyr::if_else(nivel_escolaridad %in% c("Tecnico","Tecnologia"), "02_Tecnologico",
                                                             dplyr::if_else(nivel_escolaridad == "Universitario", "03_Universitario","04_Posgrado"))), ordered = TRUE),
    sector_economico = factor(dplyr::if_else(sector_economico %in% c("Sector Privado Esal","Sector Privado Financiero Solidario"), "Sector Privado",sector_economico)),
    estrato = factor(dplyr::if_else(estrato == "1", "1",
                                    dplyr::if_else(estrato == "2", "2",
                                                   dplyr::if_else(estrato == "3", "3","4-5-6"))), ordered = TRUE),
    ingresos = salario_actual + otros_ingresos
  ) %>%
  dplyr::select(c(num_identificacion,
                  tipo_contrato,
                  sector_economico,
                  actividad_economica,
                  ingresos,
                  egresos,
                  activos,
                  pasivos))

rm(reference_date)

# Identify factor columns 
factor_columns <- names(clientes)[sapply(clientes,is.factor)]

clientes <- fastDummies::dummy_cols(
  clientes,
  select_columns = factor_columns,
  remove_first_dummy = FALSE,
  remove_selected_columns = FALSE,
  omit_colname_prefix = FALSE
)

clientes <- clientes %>%
  dplyr::select(-dplyr::all_of(factor_columns)) 

rm(factor_columns)

movimientos <- movimientos %>%
  dplyr::select(c(num_identificacion,
                  fecha_transaccion,
                  monto_transaccion,
                  tipo_producto,
                  tipo_transaccion,
                  tipo_canal_transaccion)) %>%
  dplyr::rename(
    fecha_transac = fecha_transaccion,
    tipo_prod = tipo_producto,
    tipo_transac = tipo_transaccion,
    tipo_canal = tipo_canal_transaccion,
    monto = monto_transaccion
  ) %>%
  dplyr::mutate(
    month_yr = base::format(base::as.Date(fecha_transac), "%Y-%m"),
    tipo_canal = dplyr::if_else(tipo_canal == "Físico", "Fisico", "Electronico")
  ) %>%
  dplyr::filter(monto > 0)


# Calculate transactions per day in a single step
clientes_summary <- movimientos %>% 
  dplyr::group_by(num_identificacion) %>%
  dplyr::summarise(
    transaction_count = n(), 
    num_months = dplyr::n_distinct(month_yr),
    num_transa = round(transaction_count / num_months,0)
  ) %>%
  dplyr::ungroup()

# Prepare data for pivot - create all metrics in one step
clientes_metrics <- movimientos %>% 
  dplyr::group_by(num_identificacion, tipo_prod, tipo_transac, tipo_canal) %>%
  dplyr::summarise(total_amount = sum(monto)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    prod_transac = paste0(tipo_prod, "_", tipo_transac),
    canal_transac = paste0(tipo_canal, "_", tipo_transac)
  )

# Create separate pivots more efficiently
prod_metrics <- clientes_metrics %>%
  dplyr::select(num_identificacion, prod_transac, total_amount) %>%
  dplyr::group_by(num_identificacion, prod_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = prod_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

canal_metrics <- clientes_metrics %>%
  dplyr::select(num_identificacion, canal_transac, total_amount) %>%
  dplyr::group_by(num_identificacion, canal_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = canal_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

# Join all data in one step and normalize by transaction days
gold_affiliates <- clientes %>% 
  dplyr::left_join(clientes_summary, by = "num_identificacion") %>%
  dplyr::left_join(prod_metrics, by = "num_identificacion") %>%
  dplyr::left_join(canal_metrics, by = "num_identificacion") %>%
  dplyr::mutate(across(
    .cols = c(Ahorro_Egreso, Ahorro_Ingreso, Credito_Egreso, Credito_Ingreso,       
              Electronico_Egreso, Electronico_Ingreso, Fisico_Egreso, Fisico_Ingreso),
    .fns = ~ . / num_months
  )) %>%
  dplyr::select(
    num_identificacion,
    tipo_contrato_Indefinido,
    tipo_contrato_Otro,
    `sector_economico_Sector Privado`,
    `sector_economico_Sector Publico Administrativo`,
    `sector_economico_Sector Publico Educacion`,
    `sector_economico_Sector Publico Pensionado`,
    `sector_economico_Sector Publico Salud`,
    actividad_economica_Asalariado,
    actividad_economica_Otro,
    num_transa,
    ingresos,
    egresos,
    activos,
    pasivos,
    Ahorro_Egreso,
    Ahorro_Ingreso,
    Credito_Egreso,
    Credito_Ingreso,
    Electronico_Egreso,
    Electronico_Ingreso,
    Fisico_Egreso,
    Fisico_Ingreso
  ) %>%
  dplyr::rename(
    T_Contr_Indef = tipo_contrato_Indefinido,
    T_Contr_Otro = tipo_contrato_Otro,
    Sec_Econ_Priv = `sector_economico_Sector Privado`,
    Sec_Econ_Pub_Admon = `sector_economico_Sector Publico Administrativo`,
    Sec_Econ_Pub_Edu = `sector_economico_Sector Publico Educacion`,
    Sec_Econ_Pub_Pens = `sector_economico_Sector Publico Pensionado`,
    Sec_Econ_Pub_Salud = `sector_economico_Sector Publico Salud`,
    Act_Eco_Asalar = actividad_economica_Asalariado,
    Act_Eco_Otro = actividad_economica_Otro
  )

# Write to DuckDB with consistent naming
DBI::dbWriteTable(gold_con, "gold_affiliates", gold_affiliates, overwrite = TRUE)

# Clean up
rm(movimientos, clientes, clientes_summary, clientes_metrics, prod_metrics, canal_metrics, gold_affiliates)

# Create indexes for better performance (only if tables exist)
tryCatch({
  DBI::dbExecute(gold_con, "CREATE INDEX IF NOT EXISTS idx_gold_affiliates_id ON gold_affiliates(num_identificacion)")
  base::message("Created index on gold_affiliates")
}, error = function(e) {
  base::message("Skipping index creation for gold_affiliates table, table does not exist")
})


# Optimize Database Performance
DBI::dbExecute(gold_con, "VACUUM")
DBI::dbExecute(gold_con, "ANALYZE")

# Display table statistics
base::message("Checking tables and their row counts:")
tables <- DBI::dbListTables(gold_con)
tables_with_gold_prefix <- tables[base::grepl("^gold_", tables)]

if (base::length(tables_with_gold_prefix) > 0) {
  table_stats <- base::data.frame(table_name=character(), row_count=integer())
  
  for (table in tables_with_gold_prefix) {
    count_query <- glue::glue("SELECT COUNT(*) AS row_count FROM {table}")
    count_result <- DBI::dbGetQuery(gold_con, count_query)
    table_stats <- base::rbind(table_stats, base::data.frame(table_name=table, row_count=count_result$row_count))
  }
  
  base::message("Gold tables created:")
  base::print(table_stats)
} else {
  base::message("No gold tables were created successfully")
}

# Close database connections
DBI::dbExecute(gold_con, "DETACH DATABASE silver_db;")
DBI::dbDisconnect(silver_con)
DBI::dbDisconnect(gold_con)

# Clean up environment
base::rm(list = base::ls())