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

# Reading tables

clientes <- readRDS("Data/clientes.rds")
movimientos <- readRDS("Data/movimientos.rds")

# Data preparation
clientes <- clientes %>%
  dplyr::filter(ROL == "Asociado") # Seleccionando solo asociados (vigentes)

movimientos <- movimientos %>%
  dplyr::semi_join(clientes, by = "NUM_IDENTIFICACION") # Seleccionando solo transacciones con asociado

clientes <- clientes %>%
  dplyr::semi_join(movimientos, by = "NUM_IDENTIFICACION") # Seleccionando solo asociados con transacciones

reference_date <- as.Date("2024-12-31")

clientes <- clientes %>%
  dplyr::mutate(
    EDAD = as.numeric(difftime(reference_date, FECHA_NACIMIENTO, units = "days")) %/% 365, #EDAD al 2024-12-31
    ANTIGUEDAD = as.numeric(difftime(reference_date, FECHA_INGRESO, units = "days")) %/% 365, #ANTIGUEDAD al 2024-12-31
    TIPO_CONTRATO = dplyr::if_else(ACTIVIDAD_ECONOMICA == "Independiente","Prestación Servicios",TIPO_CONTRATO), #Independiente no tiene contrato indefinido
    TIPO_CONTRATO = factor(dplyr::if_else(TIPO_CONTRATO == "Término Indefinido","Indefinido","Otro")), # Otro es: Otro, Prestación Servicios, Temporal, Término Fijo
    ACTIVIDAD_ECONOMICA = factor(dplyr::if_else(ACTIVIDAD_ECONOMICA == "Asalariado","Asalariado","Otro")), # Otro es Independiente o pensionado 
    NIVEL_ESCOLARIDAD = factor(dplyr::if_else(NIVEL_ESCOLARIDAD %in% c("Primaria","Bachillerato"), "01_Basico",
                                     dplyr::if_else(NIVEL_ESCOLARIDAD %in% c("Tecnico","Tecnologia"), "02_Tecnologico",
                                           dplyr::if_else(NIVEL_ESCOLARIDAD == "Universitario", "03_Universitario","04_Posgrado"))), ordered = TRUE),
    SECTOR_ECONOMICO = factor(dplyr::if_else(SECTOR_ECONOMICO %in% c("Sector Privado Esal","Sector Privado Financiero Solidario"), "Sector Privado",SECTOR_ECONOMICO)),
    ESTRATO = factor(dplyr::if_else(ESTRATO == "1", "1",
                                    dplyr::if_else(ESTRATO == "2", "2",
                                                   dplyr::if_else(ESTRATO == "3", "3","4-5-6"))), ordered = TRUE),
    INGRESOS = SALARIO_ACTUAL + OTROS_INGRESOS
    ) %>%
  dplyr::select(-c(TIPO_IDENTIFICACION,
                   COD_MUNICIPIO,
                   NOMBRE_MUNICIPIO,
                   DEPARTAMENTO,
                   FECHA_INGRESO,
                   FECHA_NACIMIENTO,
                   ROL,
                   ACTIVO,
                   GENERO,
                   ESTADO_CIVIL,
                   MUJER_CAB_FAMILIA,
                   OCUPACION,
                   CIIU,
                   TRANSACCIONES_INTERNACIONALES, 
                   SALARIO_ACTUAL,
                   OTROS_INGRESOS,
                   PATRIMONIO,
                   SEMESTRE))

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
  dplyr::select(c(NUM_IDENTIFICACION,
                  FECHA_TRANSACCION,
                  TIPO_PRODUCTO,
                  TIPO_TRANSACCION,
                  TIPO_CANAL_TRANSACCION,
                  MONTO_TRANSACCION
                  )) %>%
  dplyr::rename(
    PRODUCTO = TIPO_PRODUCTO,
    TIPO = TIPO_TRANSACCION,
    CANAL = TIPO_CANAL_TRANSACCION,
    MONTO = MONTO_TRANSACCION
  ) %>%
  dplyr::mutate(
    Month_Yr = base::format(base::as.Date(FECHA_TRANSACCION), "%Y-%m"),
    CANAL = dplyr::if_else(CANAL == "Físico", "Fisico", "Electronico")
  )

# Create transaction type and channel-based columns
movimientos <- movimientos %>%
  dplyr::mutate(
    ahorro_egreso = dplyr::if_else(PRODUCTO == "Ahorro" & TIPO == "Egreso", MONTO, 0),
    ahorro_ingreso = dplyr::if_else(PRODUCTO == "Ahorro" & TIPO == "Ingreso", MONTO, 0),
    credito_egreso = dplyr::if_else(PRODUCTO == "Credito" & TIPO == "Egreso", MONTO, 0),
    credito_ingreso = dplyr::if_else(PRODUCTO == "Credito" & TIPO == "Ingreso", MONTO, 0),
    fisico_egreso = dplyr::if_else(CANAL == "Fisico" & TIPO == "Egreso", MONTO, 0),
    fisico_ingreso = dplyr::if_else(CANAL == "Fisico" & TIPO == "Ingreso", MONTO, 0),
    electronico_egreso = dplyr::if_else(CANAL == "Electronico" & TIPO == "Egreso", MONTO, 0),
    electronico_ingreso = dplyr::if_else(CANAL == "Electronico" & TIPO == "Ingreso", MONTO, 0)
  )

# Summarize transaction data
pivot2 <- movimientos %>%
  dplyr::group_by(NUM_IDENTIFICACION, Month_Yr) %>%
  dplyr::summarise(n_tran = dplyr::n(), .groups = 'drop')

pivot3 <- pivot2 %>%
  dplyr::group_by(NUM_IDENTIFICACION) %>%
  dplyr::summarise(n_mes = dplyr::n(), .groups = 'drop')

# Summarize by NUM_IDENTIFICACION, i.e., clients (customers)
movimientos <- movimientos %>%
  dplyr::group_by(NUM_IDENTIFICACION) %>%
  dplyr::summarise(
    ahorro_egreso = base::sum(ahorro_egreso),
    ahorro_ingreso = base::sum(ahorro_ingreso),
    credito_egreso = base::sum(credito_egreso),
    credito_ingreso = base::sum(credito_ingreso),
    fisico_egreso = base::sum(fisico_egreso),
    fisico_ingreso = base::sum(fisico_ingreso),
    electronico_egreso = base::sum(electronico_egreso),
    electronico_ingreso = base::sum(electronico_ingreso),
    num_transacciones = dplyr::n(),
    .groups = 'drop'
  )

# Merge summarized data
movimientos <- dplyr::left_join(movimientos, pivot3, by = "NUM_IDENTIFICACION")

# Normalize transaction metrics by the number of months
movimientos <- movimientos %>%
  dplyr::mutate(
    dplyr::across(
      c(ahorro_egreso:electronico_ingreso, num_transacciones),
      ~ .x / n_mes
    )
  ) %>%
  dplyr::select(-n_mes)

base::rm(pivot2,pivot3)

# Merge client information with transaction summary (transacciones)
affiliates <- dplyr::inner_join(clientes, movimientos, by = "NUM_IDENTIFICACION") %>% 
  dplyr::select(c(NUM_IDENTIFICACION,
                  ACTIVOS,
                  PASIVOS,
                  INGRESOS,
                  EGRESOS,
                  ahorro_egreso, 
                  ahorro_ingreso,
                  credito_egreso,
                  credito_ingreso,
                  fisico_egreso,
                  fisico_ingreso,
                  electronico_egreso,
                  electronico_ingreso,
                  num_transacciones,                  
                  TIPO_CONTRATO_Indefinido,
                  TIPO_CONTRATO_Otro,
                  ACTIVIDAD_ECONOMICA_Asalariado,
                  ACTIVIDAD_ECONOMICA_Otro,
                  `SECTOR_ECONOMICO_Sector Privado`,
                  `SECTOR_ECONOMICO_Sector Publico Administrativo`,
                  `SECTOR_ECONOMICO_Sector Publico Educacion`,
                  `SECTOR_ECONOMICO_Sector Publico Pensionado`,
                  `SECTOR_ECONOMICO_Sector Publico Salud`)) %>%
  dplyr::rename(
    Activos               = ACTIVOS,
    Pasivos               = PASIVOS,
    Ingresos              = INGRESOS,
    Egresos               = EGRESOS,
    "Ahorro Egr."         = ahorro_egreso,
    "Ahorro Ing."         = ahorro_ingreso,
    "Credito Egr."        = credito_egreso,
    "Credito Ing."        = credito_ingreso,
    "Fisico Egr."         = fisico_egreso,
    "Fisico Ing."         = fisico_ingreso,
    "Elec. Egr."          = electronico_egreso,
    "Elec. Ing."          = electronico_ingreso,
    "Num. Trans."         = num_transacciones,
    "T.Contr. Indef."     = TIPO_CONTRATO_Indefinido,
    "T.Contr. Otro"       = TIPO_CONTRATO_Otro,
    "Act. Eco. Asalar."   = ACTIVIDAD_ECONOMICA_Asalariado,
    "Act. Eco. Otro"      = ACTIVIDAD_ECONOMICA_Otro,
    "S.Econ. Priv."       = "SECTOR_ECONOMICO_Sector Privado",
    "S.Econ. Pub. Admon." = "SECTOR_ECONOMICO_Sector Publico Administrativo",
    "S.Econ. Pub. Edu."   = "SECTOR_ECONOMICO_Sector Publico Educacion",
    "S.Econ. Pub. Pens."  = "SECTOR_ECONOMICO_Sector Publico Pensionado",
    "S.Econ. Pub. Salud"  = "SECTOR_ECONOMICO_Sector Publico Salud"
  )

saveRDS(affiliates, "Data/affiliates.rds")

rm(list =ls())
