# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
base::set.seed(444)

# Ensure required packages are installed
required_pkgs <- c("magrittr", "tidyverse", "FactoMineR", "factoextra", "cluster", 
                   "car", "fpc", "ggpubr", "plotrix", "writexl", "fmsb", "clValid", 
                   "DBI", "duckdb")  # Added DBI and duckdb packages
install_if_missing <- function(pkg) {
  if (!base::requireNamespace(pkg, quietly = TRUE)) 
    utils::install.packages(pkg, dependencies = TRUE)
}
base::invisible(base::lapply(required_pkgs, install_if_missing))
base::sapply(required_pkgs, require, character.only = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define color palette function
gg_color <- function(n) {
  hues <- base::seq(15, 375, length = n + 1)
  grDevices::hcl(h = hues, l = 65, c = 100)[1:n]
}

# Define organization and database path
org_id <- "0000_CrimesLAFT"
gold_path <- base::file.path("Data", "Gold", org_id, "2022_2023_2024", paste0(org_id, ".duckdb"))


# Connect to Gold database with proper error handling
con <- base::tryCatch({
  DBI::dbConnect(
    duckdb::duckdb(),
    dbdir = gold_path,
    config = base::list(threads = "4", memory_limit = "8GB"),
    read_only = TRUE   # Read-only mode for analytics
  )
}, error = function(e) {
  base::message(glue::glue("Failed to connect to {gold_path}: {e$message}"))
  base::stop("Database connection error")
})

# 1. Read processed delictividad data
base::tryCatch({
  delictividad <- DBI::dbGetQuery(con, "SELECT * FROM gold_delictividad")
  base::message(glue::glue("Successfully loaded {base::nrow(delictividad)} delictividad"))
}, error = function(e) {
  base::message(glue::glue("Failed to query gold_delictividad: {e$message}"))
  base::stop("Data query error")
}, finally = {
  # Always close the connection (executes regardless of success/error)
  if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con, shutdown = TRUE)
    base::message("Database connection closed")
  }
})

base::rm(con,gold_path,org_id)

delictividad <- delictividad %>% 
  dplyr::select(c(departamento,rate22,rate23,rate24)) %>%
  tibble::column_to_rownames(var = "departamento")

# 2. Copy dataset for transformation
delictividad_t <- log(delictividad)

# 3. Perform PCA 
pca_results <- FactoMineR::PCA(delictividad_t, scale.unit = TRUE, ncp = 2, graph = FALSE)
# Optional scree plot:
factoextra::fviz_eig(pca_results, addlabels = TRUE, barfill = "#8e0152", barcolor = "#8e0152")
scores <- base::as.data.frame(pca_results$ind$coord)


# 4. Choosing the best clustering algorithm 

clmethods <- c("hierarchical", "kmeans", "pam", "clara")

validation <- clValid::clValid(obj = scores[,-3], 
                               nClust = 2:6,
                               clMethods = clmethods,
                               validation = "internal",
                               metric = "euclidean",
                               method = "ward",
                               maxitems = 10000)

optimal_scores <- clValid::optimalScores(validation)

base::rm(clmethods)

# 5. Cluster stability analysis via bootstrap

cluster_stability_hier1 <- fpc::clusterboot(scores, B = 100, clustermethod = fpc::hclustCBI, method = "ward.D2", k = 2)
cluster_stability_hier2 <- fpc::clusterboot(scores, B = 100, clustermethod = fpc::hclustCBI, method = "ward.D2", k = 3)
cluster_stability_hier3 <- fpc::clusterboot(scores, B = 100, clustermethod = fpc::hclustCBI, method = "ward.D2", k = 5)


# 6. Creating summary table for clustering method selection 
# Create helper function to process cluster stability objects
process_stability <- function(stability, method, k) {
  data.frame(
    Method = paste0(method, " (k = ",k,")"),
    Connectivity = if(method == "hierarchical" && k == 2) 
      round(optimal_scores["Connectivity", "Score"], 2) else NA,
    Dunn = if(method == "kmeans" && k == 4) 
      round(optimal_scores["Dunn", "Score"], 2) else NA,
    Silhouette = if(method == "kmeans" && k == 3) 
      round(optimal_scores["Silhouette", "Score"], 2) else NA,
    Jaccard_min = round(min(stability$bootmean), 2),
    Jaccard_prom = round(mean(stability$bootmean), 2),
    Jaccard_max = round(max(stability$bootmean), 2),
    Dissolved = paste(stability$bootbrd, collapse = ", "),
    Recovered = paste(stability$bootrecover, collapse = ", "),
    stringsAsFactors = FALSE
  )
}

# Create summary table combining internal validation and stability analysis
cluster_summary <- t(rbind(
  process_stability(cluster_stability_hier1, "hierarchical", 2),
  process_stability(cluster_stability_hier2, "hierarchical", 3),
  process_stability(cluster_stability_hier3, "hierarchical", 4),
)) %>% 
  janitor::row_to_names(row_number = 1) %>%
  base::data.frame() %>%
  tibble::rownames_to_column(var = "Medida")

names(cluster_summary)[-1] <- c("Jerarquico (k=2)","Jerarquico (k=3)","Jerarquico (k=4)") 


# 9. Set number of clusters and perform clustering
k <- 5

clus <- cluster::agnes(scores[,-3], method='ward')
hc <- as.dendrogram(clus)
plot(hc)


fviz_dend(hc, cex = 0.5, k = 5, k_colors = gg_color(k), rect = TRUE)


hc_cut <- hcut(scores[,-3], k = 5, hc_func = "agnes", hc_method = "ward.D2")

delictividad <- delictividad %>%
  dplyr::mutate(cluster = base::as.factor(hc_cut$cluster),
                nivel_jurisdiccion = base::factor(
                  dplyr::if_else(cluster == "3","Muy Bajo",
                        dplyr::if_else(cluster == "1","Bajo",
                              dplyr::if_else(cluster == "5","Moderado",
                                    dplyr::if_else(cluster == "4","Alto","Muy Alto")))),
                  ordered = TRUE,
                  levels = c("Muy Bajo","Bajo","Moderado","Alto","Muy Alto"))) %>%
  tibble::rownames_to_column("departamento")


# 10. Visualize cluster results

# 10a. Scatter Plot: First 2 principal components with clusters
clus_res <- base::data.frame(scores[, 1:2], cluster = delictividad$cluster)
ggpubr::ggscatter(clus_res,
                  x = "Dim.1", y = "Dim.2", color = "cluster",
                  repel = FALSE, ellipse = TRUE, ellipse.type = "convex",
                  shape = 19, size = 1, ggtheme = ggplot2::theme_minimal()) + 
  ggplot2::geom_hline(ggplot2::aes(yintercept = 0), color = "black", linetype = "dashed") +
  ggplot2::geom_vline(ggplot2::aes(xintercept = 0), color = "black", linetype = "dashed") +
  ggplot2::theme(legend.position = "bottom")

org_id <- "0002_Coopcentral"
# Conectar en modo de escritura (read_only = FALSE)
silver_con <- DBI::dbConnect(duckdb::duckdb(), dbdir = silver_path)

# Guardar el data.frame en la base de datos
DBI::dbWriteTable(silver_con, "delictividad", delictividad, overwrite = TRUE)
# 13. Save results: Export full segmentation to Excel and save workspace objects
writexl::write_xlsx(delictividad, "Clients/0000_CrimesLAFT/2022_2023_2024/Results/SARLAFT_Jurisdicciones_Segmentacion_Reporte.xlsx")
base::save.image("Clients/0000_CrimesLAFT/2022_2023_2024/Results/results.RData")

# Clear environment
base::rm(list = base::ls())
