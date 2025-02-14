rm(list = ls())
sessionInfo()


library(rlang)
sessionInfo()


library(readxl)
library(writexl)
library(sf)
library(RColorBrewer)
library(cartogram)
library(plyr)
library(dplyr)
library(reshape2)
library(fastmap)
library(FactoMineR)
library(car)
library(plotrix)
library(cluster)
library(ggdendro)
library(factoextra)
library(d3heatmap)
library(RColorBrewer)
library(DT)
library(corrplot)
library(rpart,quietly = TRUE)
#library(caret,quietly = TRUE)
library(rpart.plot,quietly = TRUE)
library(rattle)
library(abind, pos=32)
library(e1071, pos=33)
library(randomForest)
library(foreach)
library(iterators)
library(parallel)
library(doParallel)
library(sampling)
#cl <- makeCluster(12)
#registerDoParallel(cl)
#registerDoMC(12)
set.seed(1)


getwd()
setwd("D:/adr98/Documents/ASORIESGO/FONEDH/Segmentacion/Segmentacion junio2024")
dir()

transacciones=as.data.frame(read_excel("Plantilla_EstructuraBase para modelo de segmentacion INF. 1 SEMESTRE  2024.xlsx",sheet = "Movimientos"))
summary(transacciones)
names(transacciones)
# Eliminar las columnas "columna_a_eliminar1" y "columna_a_eliminar2"
transacciones <- transacciones[, -which(names(transacciones) %in% c("TIPO_IDENTIFICACION" , "ID_PRODUCTO", "CODIGO_TRANSACCION",
                                                                    "MEDIO_TRANSACCION", "CANAL", "MUNICIPIO_TRANSACCION",
                                                                    "PRODUCTO_DESTINO","TIPO_PRODUCTO_DESTINO", "ENTIDAD_PRODUCTO_DESTINO", 
                                                                    "ID_BENEFICIARIO","NOMBRE_BENEFICIARIO" ))]



# Reorganizar las columnas en el orden deseado
transacciones <- transacciones[, c("NUM_IDENTIFICACION","FECHA_TRANSACCION","TIPO_TRANSACCION","TIPO_PRODUCTO","TIPO_CANAL_TRANSACCION","DEPARTAMENTO_TRANSACCION","MONTO_TRANSACCION")]
names(transacciones)

class(transacciones$FECHA_TRANSACCION)
transacciones$FECHA_TRANSA=as.POSIXct(transacciones$FECHA_TRANSACCION)
summary(transacciones$FECHA_TRANSA)
summary(transacciones)
names(transacciones)


df=transacciones[,-2]
names(df)


df$Month_Yr <- format(as.Date(df$FECHA_TRANSA), "%Y-%m")
df$D_VALOR=as.double(abs(df$MONTO_TRANSACCION))

names(df)
summary(df)


names(df)[2]="Tipo.Transaccion"
names(df)[3]="producto"
names(df)[4]="canal"
names(df)[5]="Depto"
names(df)[6] = 'Monto'
names(df)


summary(factor(df$producto))

df <- df %>%
  mutate(producto = ifelse(producto == "Credito", "Cartera", producto))

summary(factor(df$producto))

summary(factor(df$Tipo.Transaccion))
df$Tipo.Transaccion=factor(ifelse(df$Tipo.Transaccion=='Egreso','EGRESO','INGRESO'))
summary((df$Tipo.Transaccion))


df$ahorro_e=ifelse(df$producto=='Ahorro' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$ahorro_i=ifelse(df$producto=='Ahorro' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)
df$cartera_e=ifelse(df$producto=='Cartera' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$cartera_i=ifelse(df$producto=='Cartera' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)

summary(factor(df$canal))
df$canal=factor(ifelse(df$canal=='Electronico','ELECTRONICO','FISICO'))
summary((df$canal))


df$fisico_e=ifelse(df$canal=='FISICO' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$fisico_i=ifelse(df$canal=='FISICO' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)
df$virtual_e=ifelse(df$canal=='ELECTRONICO' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$virtual_i=ifelse(df$canal=='ELECTRONICO' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)

summary(factor(df$Depto))

df$Depto=factor(ifelse(df$Depto=="ANTIOQUIA","jurisdiccion_4",
                       ifelse(df$Depto=="ATLANTICO","jurisdiccion_4",
                              ifelse(df$Depto=="BOLIVAR","jurisdiccion_3",
                                     ifelse(df$Depto=="BOYACA","jurisdiccion_3",
                                            ifelse(df$Depto=="CALDAS","jurisdiccion_2",
                                                   ifelse(df$Depto=="CAQUETA","jurisdiccion_4",
                                                          ifelse(df$Depto=="CAUCA","jurisdiccion_4",
                                                                 ifelse(df$Depto=="CESAR","jurisdiccion_5",
                                                                        ifelse(df$Depto=="CORDOBA","jurisdiccion_2",
                                                                               ifelse(df$Depto=="CUNDINAMARCA","jurisdiccion_6",
                                                                                      ifelse(df$Depto=="HUILA","jurisdiccion_4",
                                                                                             ifelse(df$Depto=="MAGDALENA","jurisdiccion_3",
                                                                                                    ifelse(df$Depto=="META","jurisdiccion_5",
                                                                                                           ifelse(df$Depto=="NARINO","jurisdiccion_3",
                                                                                                                  ifelse(df$Depto=="NORTE DE SANTANDER","jurisdiccion_3",
                                                                                                                         ifelse(df$Depto=="QUINDIO","jurisdiccion_5",
                                                                                                                                ifelse(df$Depto=="RISARALDA","jurisdiccion_4",
                                                                                                                                       ifelse(df$Depto=="ISLAS","jurisdiccion_5",
                                                                                                                                              ifelse(df$Depto=="SANTANDER","jurisdiccion_5",
                                                                                                                                                     ifelse(df$Depto=="SUCRE","jurisdiccion_3",
                                                                                                                                                            ifelse(df$Depto=="NO_REPORTA","jurisdiccion_0",
                                                                                                                                                                   ifelse(df$Depto=="TOLIMA","jurisdiccion_4","jurisdiccion_5")))))))))))))))))))))))

df$jurisdiccion_0_e=ifelse(df$Depto=='jurisdiccion_0' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$jurisdiccion_0_i=ifelse(df$Depto=='jurisdiccion_0' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)
df$jurisdiccion_2_e=ifelse(df$Depto=='jurisdiccion_2' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$jurisdiccion_2_i=ifelse(df$Depto=='jurisdiccion_2' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)
df$jurisdiccion_3_e=ifelse(df$Depto=='jurisdiccion_3' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$jurisdiccion_3_i=ifelse(df$Depto=='jurisdiccion_3' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)
df$jurisdiccion_4_e=ifelse(df$Depto=='jurisdiccion_4' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$jurisdiccion_4_i=ifelse(df$Depto=='jurisdiccion_4' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)
df$jurisdiccion_5_e=ifelse(df$Depto=='jurisdiccion_5' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$jurisdiccion_5_i=ifelse(df$Depto=='jurisdiccion_5' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)
df$jurisdiccion_6_e=ifelse(df$Depto=='jurisdiccion_6' & df$Tipo.Transaccion=='EGRESO',df$D_VALOR,0)
df$jurisdiccion_6_i=ifelse(df$Depto=='jurisdiccion_6' & df$Tipo.Transaccion=='INGRESO',df$D_VALOR,0)


summary(df)

pivot2 <- df %>%
  select(NUM_IDENTIFICACION, Month_Yr,D_VALOR) %>%
  group_by(NUM_IDENTIFICACION,Month_Yr) %>%
  summarise(n_tran=length(D_VALOR))

summary(pivot2)

pivot3 <- pivot2 %>% group_by(NUM_IDENTIFICACION) %>% summarise(n_mes=length(Month_Yr))

test=data.frame(df %>% group_by(NUM_IDENTIFICACION) %>% summarise(jur_0_egreso=sum(jurisdiccion_0_e),
                                                                  jur_0_ingreso=sum(jurisdiccion_0_i),
                                                                  jur_2_egreso=sum(jurisdiccion_2_e),
                                                                  jur_2_ingreso=sum(jurisdiccion_2_i),
                                                                  jur_3_egreso=sum(jurisdiccion_3_e),
                                                                  jur_3_ingreso=sum(jurisdiccion_3_i),
                                                                  jur_4_egreso=sum(jurisdiccion_4_e),
                                                                  jur_4_ingreso=sum(jurisdiccion_4_i),
                                                                  jur_5_egreso=sum(jurisdiccion_5_e),
                                                                  jur_5_ingreso=sum(jurisdiccion_5_i),
                                                                  jur_6_egreso=sum(jurisdiccion_6_e),
                                                                  jur_6_ingreso=sum(jurisdiccion_6_i),
                                                                  ahorro_egreso=sum(ahorro_e),
                                                                  ahorro_ingreso=sum(ahorro_i),
                                                                  cartera_egreso=sum(cartera_e),
                                                                  cartera_ingreso=sum(cartera_i),
                                                                  fisico_egreso=sum(fisico_e),
                                                                  fisico_ingreso=sum(fisico_i),
                                                                  virtual_egreso=sum(virtual_e),
                                                                  virtual_ingreso=sum(virtual_i),
                                                                  num_transacciones=length(D_VALOR)
))

summary(test)

test=merge(test,pivot3,by=c('NUM_IDENTIFICACION','NUM_IDENTIFICACION'))
summary(test)
names(test)
test[,2:22]=test[,2:22]/test[,23]
summary(test)
test=test[,-23]
summary(test)

dir()

###Clientes

asociados=as.data.frame(read_excel("Plantilla_EstructuraBase para modelo de segmentacion INF. 1 SEMESTRE  2024.xlsx",sheet = "InformaciónCliente"))
names(asociados)

asociados$ingresos= asociados$SALARIO_ACTUAL+asociados$OTROS_INGRESOS
names(asociados)


asociados <- asociados %>%
  select(NUM_IDENTIFICACION, ACTIVIDAD_ECONOMICA, ACTIVOS, PASIVOS, ingresos, EGRESOS )
names(asociados)

#names(clientes)
#[1] "NUM_IDENTIFICACION"  "actividad_economica" "activos"            
#[4] "pasivos"             "ingresos"            "egresos"            
#[7] "jur_0_egreso"        "jur_0_ingreso"       "jur_2_egreso"       
#[10] "jur_2_ingreso"       "jur_3_egreso"        "jur_3_ingreso"      
#[13] "jur_4_egreso"        "jur_4_ingreso"       "jur_5_egreso"       
#[16] "jur_5_ingreso"       "jur_6_egreso"        "jur_6_ingreso"      
#[19] "ahorro_egreso"       "ahorro_ingreso"      "cartera_egreso"     
#[22] "cartera_ingreso"     "fisico_egreso"       "fisico_ingreso"     
#[25] "virtual_egreso"      "virtual_ingreso"     "num_transacciones"  
#[28] "Estudiante"          "Independiente"       "No_reporta"         
#[31] "Otro"                "Pensionado"          "Salario"


names(asociados)[c(1,6)]=c("NUM_IDENTIFICACION","egresos")
names(asociados)

names(asociados)[c(2,3,4)]=c("actividad_economica","activos","pasivos")
names(asociados)

clientes=merge(asociados,test,by=c('NUM_IDENTIFICACION','NUM_IDENTIFICACION'))
names(clientes)
summary(clientes)
names(clientes)

# Reemplazar los valores faltantes (NA) en la columna 'ingresos' con 0
clientes$ingresos[is.na(clientes$ingresos)] <- 0
summary(clientes)

l=levels(factor(clientes$actividad_economica))
l
for(i in 28:(27+length(l))) clientes[,i]=ifelse(clientes$actividad_economica==l[i-27],1,0)
names(clientes)[28:30]=l
summary(clientes)
names(clientes)

names(clientes)[28]=c("Salario")
names(clientes)


#clientes=clientes[,c(1,2,8:10,4:7)]
#names(clientes)

#names(clientes)=c("NUM_IDENTIFICACION",	"ACTIVIDAD_ECONOMICA",	"Salario","Independiente", 'Pensionado', "activos",	"pasivos",	"ingresos",	"egresos")
#names(clientes)

clientes$Estudiante <- 0
clientes$No_reporta <- 0
clientes$Otro <- 0

#NUM_IDENTIFICACION	ACTIVIDAD_ECONOMICA	Emp.Pub	Pensionado	Otro.Tipo.Emp	Independiente	activos	pasivos	ingresos	egresos


names(clientes)


clientes_actual = clientes
summary(clientes_actual)
names(clientes_actual)
names(clientes)
##################Ejecucion del Modelo

load("D:/adr98/Documents/ASORIESGO/FONEDH/Segmentacion/todos_los_objetos.RData")

pred1 <- predict(object=model_rf,clientes_actual,type="raw")

nrow(clientes_actual)
length(pred1)

any(is.na(clientes_actual))

clientes_actual$clust=pred1
summary(clientes_actual)

library(writexl)
write_xlsx(clientes_actual,"resultado_eval_segmentacion_jun24.xlsx")
