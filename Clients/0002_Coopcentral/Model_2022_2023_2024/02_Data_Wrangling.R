# Gold Layer Creation Script with Expanded Metrics
# Creates gold tables by merging silver_clientes and silver_movimientos

# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
base::set.seed(444)

# Load required packages with explicit namespaces
required_pkgs <- c("DBI", "duckdb", "dplyr", "glue")
install_if_missing <- function(pkg) {
  if (!base::requireNamespace(pkg, quietly = TRUE)) utils::install.packages(pkg)
}
base::invisible(base::lapply(required_pkgs, install_if_missing))
base::sapply(required_pkgs, require, character.only = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define paths for Silver and Gold layers
org_id <- "0002_Coopcentral"
silver_path <- base::file.path("Data", "Silver", org_id, "Model_2022_2023_2024", paste0(org_id, ".duckdb"))
gold_path <- base::file.path("Data", "Gold", org_id, "Model_2022_2023_2024", paste0(org_id, ".duckdb"))

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

clientes <- DBI::dbGetQuery(gold_con, "SELECT * FROM silver_db.silver_clientes") %>%
  dplyr::rename(num_ident = documento)

movimientos <- DBI::dbGetQuery(gold_con, "SELECT * FROM silver_db.silver_movimientos")

# 1. GOLD_BAJO_MONTO TABLE
base::message("Creating gold_bajo_monto table using improved approach...")

# Calculate transactions per day in a single step
clientes_bm_summary <- movimientos %>% 
  dplyr::filter(nombre_prod == "8") %>% 
  dplyr::group_by(num_ident) %>%
  dplyr::summarise(
    transaction_count = n(), 
    num_days = n_distinct(fecha_transac),
    num_transa = round(transaction_count / num_days,0)
  ) %>%
  dplyr::ungroup()

# Prepare data for pivot - create all metrics in one step
clientes_bm_metrics <- movimientos %>% 
  dplyr::filter(nombre_prod == "8") %>% 
  dplyr::group_by(num_ident, tipo_prod, tipo_transac, tipo_canal, jurisdiccion) %>%
  dplyr::summarise(total_amount = sum(monto)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    prod_transac = paste0(tipo_prod, "_", tipo_transac),
    canal_transac = paste0(tipo_canal, "_", tipo_transac),
    juris_transac = paste0(jurisdiccion, "_", tipo_transac)
  )

# Create separate pivots more efficiently
prod_metrics <- clientes_bm_metrics %>%
  dplyr::select(num_ident, prod_transac, total_amount) %>%
  dplyr::group_by(num_ident, prod_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = prod_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

canal_metrics <- clientes_bm_metrics %>%
  dplyr::select(num_ident, canal_transac, total_amount) %>%
  dplyr::group_by(num_ident, canal_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = canal_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

juris_metrics <- clientes_bm_metrics %>%
  dplyr::select(num_ident, juris_transac, total_amount) %>%
  dplyr::group_by(num_ident, juris_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = juris_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

# Join all data in one step and normalize by transaction days
gold_bajo_monto <- clientes_bm_summary %>%
  dplyr::left_join(prod_metrics, by = "num_ident") %>%
  dplyr::left_join(canal_metrics, by = "num_ident") %>%
  dplyr::left_join(juris_metrics, by = "num_ident") %>%
  dplyr::mutate(across(
    .cols = -c(num_ident, transaction_count, num_days, num_transa),
    .fns = ~ . / num_days
  )) %>%
  dplyr::select(
    num_ident, num_transa, 
    everything(), 
    -transaction_count, -num_days
  )

# Write to DuckDB with consistent naming
DBI::dbWriteTable(gold_con, "gold_bajo_monto", gold_bajo_monto, overwrite = TRUE)

movimientos <- movimientos %>% 
  dplyr::filter(nombre_prod != "8") %>%
  dplyr::mutate(tipo_prod = dplyr::if_else(tipo_prod == "CDT", "Ahorro", tipo_prod))

clientes <- clientes %>%
  anti_join(gold_bajo_monto, by = "num_ident")

# Clean up
rm(clientes_bm_summary, clientes_bm_metrics, prod_metrics, canal_metrics, juris_metrics, gold_bajo_monto)

# 2. Gold Table for "persona_juridica" segmentation
base::message("Creating gold_persona_juridica table...")

pj <- clientes %>% 
    dplyr::filter(tipo_documento == "Nit") %>%
    dplyr::select(num_ident,ciiu,estado,ingresos,egresos,activos,pasivos) %>%
    dplyr::mutate(
      ciiu = factor(ciiu),
      estado = factor(estado),
      comercio = dplyr::if_else(ciiu == "Comercio al por Mayor y por Menor", 1, 0),
      construccion = dplyr::if_else(ciiu == "Construcción e Infraestructura", 1, 0),
      ind_alimentaria = dplyr::if_else(ciiu == "Industria de Alimentos y Bebidas", 1, 0),
      ind_manufacturera = dplyr::if_else(ciiu == "Industria Manufacturera", 1, 0),
      no_clasificado = dplyr::if_else(ciiu == "No Clasificado", 1, 0),
      otros_servicios = dplyr::if_else(ciiu == "Otros Servicios", 1, 0),
      servicios_prof_tecn = dplyr::if_else(ciiu == "Servicios Profesionales y Técnicos", 1, 0),
      activo = dplyr::if_else(estado == "A", 1, 0),
      inactivo = dplyr::if_else(estado == "I", 1, 0)
    ) %>%
   dplyr::select(num_ident,comercio,construccion,ind_alimentaria,ind_manufacturera,no_clasificado,otros_servicios,servicios_prof_tecn,activo,inactivo,ingresos,egresos,activos,pasivos)

movimientos_pj <- movimientos %>%
  filter(num_ident %in% pj$num_ident)

# Calculate transactions per day in a single step
clientes_pj_summary <- movimientos_pj %>% 
  dplyr::group_by(num_ident) %>%
  dplyr::summarise(
    transaction_count = n(), 
    num_days = n_distinct(fecha_transac),
    num_transa = round(transaction_count / num_days,0)
  ) %>%
  dplyr::ungroup()

# Prepare data for pivot - create all metrics in one step
clientes_pj_metrics <- movimientos %>% 
  dplyr::group_by(num_ident, tipo_prod, tipo_transac, tipo_canal, jurisdiccion) %>%
  dplyr::summarise(total_amount = sum(monto)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    prod_transac = paste0(tipo_prod, "_", tipo_transac),
    canal_transac = paste0(tipo_canal, "_", tipo_transac),
    juris_transac = paste0(jurisdiccion, "_", tipo_transac)
  )

# Create separate pivots more efficiently
prod_metrics <- clientes_pj_metrics %>%
  dplyr::select(num_ident, prod_transac, total_amount) %>%
  dplyr::group_by(num_ident, prod_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = prod_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

canal_metrics <- clientes_pj_metrics %>%
  dplyr::select(num_ident, canal_transac, total_amount) %>%
  dplyr::group_by(num_ident, canal_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = canal_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

juris_metrics <- clientes_pj_metrics %>%
  dplyr::select(num_ident, juris_transac, total_amount) %>%
  dplyr::group_by(num_ident, juris_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = juris_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

# Join all data in one step and normalize by transaction days
gold_persona_juridica <- pj %>% 
  dplyr::left_join(clientes_pj_summary, by = "num_ident") %>%
  dplyr::left_join(prod_metrics, by = "num_ident") %>%
  dplyr::left_join(canal_metrics, by = "num_ident") %>%
  dplyr::left_join(juris_metrics, by = "num_ident") %>%
  dplyr::mutate(across(
    .cols = c(Ahorro_Egreso, Ahorro_Ingreso, Credito_Egreso, Credito_Ingreso,       
              Fisico_Ingreso, Virtual_Egreso, Virtual_Ingreso, Fisico_Egreso, jurisdiccion_4_Egreso, 
              jurisdiccion_4_Ingreso, jurisdiccion_5_Ingreso, jurisdiccion_5_Egreso, jurisdiccion_6_Egreso, jurisdiccion_0_Egreso,  
              jurisdiccion_3_Ingreso, jurisdiccion_3_Egreso),
    .fns = ~ . / num_days
  )) %>%
  dplyr::select(
    num_ident, 
    everything(), 
    -transaction_count, -num_days
  )

# Write to DuckDB with consistent naming
DBI::dbWriteTable(gold_con, "gold_persona_juridica", gold_persona_juridica, overwrite = TRUE)

movimientos <- movimientos %>%
  anti_join(gold_persona_juridica, by = "num_ident")

clientes <- clientes %>%
  anti_join(gold_persona_juridica, by = "num_ident")

# Clean up
rm(pj, movimientos_pj, clientes_pj_summary, clientes_pj_metrics, prod_metrics, canal_metrics, juris_metrics, gold_persona_juridica)


# 3. Gold Table for "persona_natural" segmentation
base::message("Creating gold_persona_natural table...")

pn <- clientes %>% 
  dplyr::select(num_ident,ciiu,estado,ingresos,egresos,activos,pasivos) %>%
  dplyr::mutate(
    ciiu = factor(ciiu),
    estado = factor(estado),
    comercio = dplyr::if_else(ciiu == "Comercio al por Mayor y por Menor" & !is.na(ciiu), 1, 0),
    construccion = dplyr::if_else(ciiu == "Construcción e Infraestructura" & !is.na(ciiu), 1, 0),
    ind_alimentaria = dplyr::if_else(ciiu == "Industria de Alimentos y Bebidas" & !is.na(ciiu), 1, 0),
    ind_manufacturera = dplyr::if_else(ciiu == "Industria Manufacturera" & !is.na(ciiu), 1, 0),
    no_clasificado = dplyr::if_else(ciiu == "No Clasificado" | is.na(ciiu), 1, 0),
    otros_servicios = dplyr::if_else(ciiu == "Otros Servicios" & !is.na(ciiu), 1, 0),
    servicios_prof_tecn = dplyr::if_else(ciiu == "Servicios Profesionales y Técnicos" & !is.na(ciiu), 1, 0),
    activo = dplyr::if_else(estado == "A", 1, 0),
    inactivo = dplyr::if_else(estado == "I", 1, 0)
  ) %>%
  dplyr::select(num_ident,comercio,construccion,ind_alimentaria,ind_manufacturera,no_clasificado,otros_servicios,servicios_prof_tecn,activo,inactivo,ingresos,egresos,activos,pasivos)

movimientos_pn <- movimientos %>%
  filter(num_ident %in% pn$num_ident)

# Calculate transactions per day in a single step
clientes_pn_summary <- movimientos_pn %>% 
  dplyr::group_by(num_ident) %>%
  dplyr::summarise(
    transaction_count = n(), 
    num_days = n_distinct(fecha_transac),
    num_transa = round(transaction_count / num_days,0)
  ) %>%
  dplyr::ungroup()

# Prepare data for pivot - create all metrics in one step
clientes_pn_metrics <- movimientos %>% 
  dplyr::group_by(num_ident, tipo_prod, tipo_transac, tipo_canal, jurisdiccion) %>%
  dplyr::summarise(total_amount = sum(monto)) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(
    prod_transac = paste0(tipo_prod, "_", tipo_transac),
    canal_transac = paste0(tipo_canal, "_", tipo_transac),
    juris_transac = paste0(jurisdiccion, "_", tipo_transac)
  )

# Create separate pivots more efficiently
prod_metrics <- clientes_pn_metrics %>%
  dplyr::select(num_ident, prod_transac, total_amount) %>%
  dplyr::group_by(num_ident, prod_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = prod_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

canal_metrics <- clientes_pn_metrics %>%
  dplyr::select(num_ident, canal_transac, total_amount) %>%
  dplyr::group_by(num_ident, canal_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = canal_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

juris_metrics <- clientes_pn_metrics %>%
  dplyr::select(num_ident, juris_transac, total_amount) %>%
  dplyr::group_by(num_ident, juris_transac) %>%
  dplyr::summarise(amount = sum(total_amount)) %>%
  tidyr::pivot_wider(
    names_from = juris_transac,
    values_from = amount,
    values_fill = list(amount = 0)
  )

# Join all data in one step and normalize by transaction days
gold_persona_natural <- pn %>% 
  dplyr::left_join(clientes_pn_summary, by = "num_ident") %>%
  dplyr::left_join(prod_metrics, by = "num_ident") %>%
  dplyr::left_join(canal_metrics, by = "num_ident") %>%
  dplyr::left_join(juris_metrics, by = "num_ident") %>%
  dplyr::mutate(across(
    .cols = c(Ahorro_Egreso, Ahorro_Ingreso, Credito_Egreso, Credito_Ingreso,       
              Fisico_Ingreso, Virtual_Egreso, Virtual_Ingreso, Fisico_Egreso, jurisdiccion_4_Egreso, 
              jurisdiccion_4_Ingreso, jurisdiccion_5_Ingreso, jurisdiccion_5_Egreso, jurisdiccion_6_Egreso, jurisdiccion_0_Egreso,  
              jurisdiccion_3_Ingreso),
    .fns = ~ . / num_days
  )) %>%
  dplyr::select(
    num_ident, 
    everything(), 
    -transaction_count, -num_days
  )

# Write to DuckDB with consistent naming
DBI::dbWriteTable(gold_con, "gold_persona_natural", gold_persona_natural, overwrite = TRUE)

# Clean up
rm(movimientos, clientes, pn, movimientos_pn, clientes_pn_summary, clientes_pn_metrics, prod_metrics, canal_metrics, juris_metrics, gold_persona_natural)

# Create indexes for better performance (only if tables exist)
tryCatch({
  DBI::dbExecute(gold_con, "CREATE INDEX IF NOT EXISTS idx_bajo_monto_id ON gold_bajo_monto(num_ident)")
  base::message("Created index on gold_bajo_monto")
}, error = function(e) {
  base::message("Skipping index creation for gold_bajo_monto table, table does not exist")
})

tryCatch({
  DBI::dbExecute(gold_con, "CREATE INDEX IF NOT EXISTS idx_persona_juridica_id ON gold_persona_juridica(num_ident)")
  base::message("Created index on gold_persona_juridica")
}, error = function(e) {
  base::message("Skipping index creation for gold_persona_juridica table, table does not exist")
})

tryCatch({
  DBI::dbExecute(gold_con, "CREATE INDEX IF NOT EXISTS idx_persona_natural_id ON gold_persona_natural(num_ident)")
  base::message("Created index on gold_persona_natural")
}, error = function(e) {
  base::message("Skipping index creation for gold_persona_natural table, table does not exist")
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

