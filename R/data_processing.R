# Clear environment
base::rm(list = base::ls())

# Set seed for reproducibility
set.seed(444)

# Ensure required packages are installed
required_pkgs <- c("magrittr", "tidyverse", "FactoMineR", "factoextra", "cluster", "car", "fpc", "ggpubr", "plotrix", "writexl", "fmsb", "clValid")
install_if_missing <- function(pkg) {
  if (!requireNamespace(pkg, quietly = TRUE)) base::install.packages(pkg, dependencies = TRUE)
}
invisible(lapply(required_pkgs, install_if_missing))
sapply(required_pkgs, require, character = TRUE)
base::rm(install_if_missing, required_pkgs)

# Define color palette function
gg_color <- function(n) {
  hues <- base::seq(15, 375, length = n + 1)
  grDevices::hcl(h = hues, l = 65, c = 100)[1:n]
}

# 1. Read processed affiliates data
affiliates <- base::readRDS("Data/affiliates.rds")

# 2. Copy dataset for transformation
affiliates_t <- affiliates

# 3. Identify columns for Box-Cox transformation (from "Activos" to "num_transacciones")
boxcox_columns <- base::seq(from = base::which(base::names(affiliates_t) == "Activos"),
                            to = base::which(base::names(affiliates_t) == "Num. Trans."))

# 4. Apply Box-Cox transformation to each selected column
lambda_values <- base::numeric(length(boxcox_columns))
for (i in base::seq_along(boxcox_columns)) {
  col_index <- boxcox_columns[i]
  shift_value <- base::abs(min(affiliates_t[[col_index]], na.rm = TRUE)) + 1
  x <- affiliates_t[[col_index]] + shift_value
  lambda <- base::as.numeric(summary(car::powerTransform(x))$result[, 2])
  if (lambda == 0) {
    affiliates_t[[col_index]] <- base::log(x)
  } else {
    affiliates_t[[col_index]] <- (x^lambda - 1) / lambda
  }
  lambda_values[i] <- lambda
}
base::rm(x, lambda, col_index, shift_value, i, boxcox_columns)

# 5. Perform PCA (exclude first column if it's an identifier)
pca_results <- FactoMineR::PCA(affiliates_t[, -1], scale.unit = TRUE, ncp = 10, graph = FALSE)
# Optional scree plot:
factoextra::fviz_eig(pca_results, addlabels = TRUE, barfill = "#8e0152", barcolor = "#8e0152")
scores <- base::as.data.frame(pca_results$ind$coord)


# 6. Choosing the best clustering algorithm 

clmethods <- c("hierarchical", "kmeans", "pam", "clara")

validation <- clValid::clValid(obj = scores, 
                               nClust = 2:6,
                               clMethods = clmethods,
                               validation = "internal",
                               metric = "euclidean",
                               method = "ward",
                               maxitems = 10000)

optimal_scores <- clValid::optimalScores(validation)

base::rm(clmethods)

# 7. Cluster stability analysis via bootstrap

cluster_stability_hier <- fpc::clusterboot(scores, B = 100, clustermethod = fpc::hclustCBI, method = "ward.D2", k = 2)
cluster_stability_kmeans1 <- fpc::clusterboot(scores, B = 100, clustermethod = kmeansCBI, k = 3, seed = 444)
cluster_stability_kmeans2 <- fpc::clusterboot(scores, B = 100, clustermethod = kmeansCBI, k = 4, seed = 444)

# 8. Creating summary table for clustering method selection 
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
  process_stability(cluster_stability_hier, "hierarchical", 2),
  process_stability(cluster_stability_kmeans1, "kmeans", 3),
  process_stability(cluster_stability_kmeans2, "kmeans", 4)
)) %>% 
  janitor::row_to_names(row_number = 1) %>%
  base::data.frame() %>%
  tibble::rownames_to_column(var = "Medida")

names(cluster_summary)[-1] <- c("Jerarquico (k=2)","kmedias (k=3)", "kmedias (k=4)") 


# 9. Set number of clusters and perform clustering
k <- 3

hc <- fpc::kmeansCBI(data = scores,
                     k = k,
                     scaling = FALSE)

centroids <- hc$result$centers

# Compute the Euclidean distance matrix among the centroids
centroid_dist <- stats::dist(centroids, method = "euclidean")

# Perform hierarchical clustering on the centroid distance matrix
hc_centroids <- stats::hclust(centroid_dist, method = "ward.D2")

# Visualize the dendrogram of the centroids
factoextra::fviz_dend(hc_centroids, cex = 1, k = k, k_colors = gg_color(k), rect = TRUE)

affiliates$cluster <- base::as.factor(hc$result$cluster)

# 10. Visualize cluster results

# 10a. Scatter Plot: First 2 principal components with clusters
clus_res <- base::data.frame(scores[, 1:2], cluster = affiliates$cluster)
ggpubr::ggscatter(clus_res,
                  x = "Dim.1", y = "Dim.2", color = "cluster",
                  repel = FALSE, ellipse = TRUE, ellipse.type = "convex",
                  shape = 19, size = 1, ggtheme = ggplot2::theme_minimal()) + 
  ggplot2::geom_hline(ggplot2::aes(yintercept = 0), color = "black", linetype = "dashed") +
  ggplot2::geom_vline(ggplot2::aes(xintercept = 0), color = "black", linetype = "dashed") +
  ggplot2::theme(legend.position = "bottom")

# 10b. Boxplot: For key variable "Ingresos" by cluster (if available)
if ("Ingresos" %in% base::names(affiliates)) {
  ggplot2::ggplot(affiliates, ggplot2::aes(x = cluster, y = Ingresos, fill = cluster)) +
    ggplot2::geom_boxplot() +
    ggplot2::labs(title = "Distribución de Ingresos por Clúster", x = "Clúster", y = "Ingresos") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
} else {
  base::message("Variable 'Ingresos' no encontrada; no se crea boxplot.")
}

# 11. Create summary_table: average of key features per cluster
# Define key features (adjust as needed)
key_features <- names(affiliates)[2:14]
existing_features <- key_features[key_features %in% base::names(affiliates)]
if (base::length(existing_features) > 0) {
  summary_table <- affiliates %>%
    dplyr::group_by(cluster) %>%
    dplyr::summarise(dplyr::across(dplyr::all_of(existing_features), ~ base::mean(.x, na.rm = TRUE))) %>%
    base::as.data.frame()
} else {
  summary_table <- base::data.frame(cluster = NA)
}

# 11. Radar Chart: Plot cluster profiles using summary_table (if available)
if (base::nrow(summary_table) > 0 && base::ncol(summary_table) > 1) {
  feature_data <- base::as.matrix(summary_table[ , -1])  # Exclude cluster column
  max_vals <- base::apply(feature_data, 2, base::max)
  min_vals <- base::apply(feature_data, 2, base::min)
  radar_data <- base::data.frame(base::rbind(max_vals, min_vals, feature_data))
  base::rownames(radar_data)[1:2] <- base::c("Max", "Min")
  fmsb::radarchart(radar_data, axistype = 1, seg = 5,
                   pcol = grDevices::rainbow(base::nrow(summary_table)),
                   pfcol = scales::alpha(grDevices::rainbow(base::nrow(summary_table)), 0.5),
                   plwd = 2, title = "Perfil de Clústeres")
} else {
  base::message("No se pudo crear el gráfico radar: summary_table vacío.")
}

# 12. (Optional) Pie Chart of Cluster Proportions
cluster_counts <- base::table(affiliates$cluster)
labels <- base::paste0("Cl. ", base::names(cluster_counts), " ", cluster_counts, " ", base::round(100 * base::prop.table(cluster_counts), 1), "%")
# Save pie chart as PNG
plotrix::pie3D(cluster_counts,
               labels = labels, explode = 0.1, radius = 0.9, height = 0.05,
               col = gg_color(k), labelcex = 0.7, main = "Proporción de Clústeres"
)

# 13. Save results: Export full segmentation to Excel and save workspace objects
writexl::write_xlsx(affiliates, "/Users/fsanta/Library/CloudStorage/OneDrive-Personal/SARLAFT_Risk/Fonedh/ROutputs/SARLAFT_Fonedh_Segmentacion_Reporte.xlsx")
base::save.image("Data/results.RData")

# Clear environment
base::rm(list = base::ls())
