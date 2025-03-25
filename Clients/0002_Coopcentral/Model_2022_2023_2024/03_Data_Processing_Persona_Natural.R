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
org_id <- "0002_Coopcentral"
gold_path <- base::file.path("Data", "Gold", org_id, "Model_2022_2023_2024", paste0(org_id, ".duckdb"))


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

# 1. Read processed affiliates data
base::tryCatch({
  affiliates <- DBI::dbGetQuery(con, "SELECT * FROM gold_persona_natural")
  base::message(glue::glue("Successfully loaded {base::nrow(affiliates)} affiliates"))
}, error = function(e) {
  base::message(glue::glue("Failed to query gold_affiliates: {e$message}"))
  base::stop("Data query error")
}, finally = {
  # Always close the connection (executes regardless of success/error)
  if (DBI::dbIsValid(con)) {
    DBI::dbDisconnect(con, shutdown = TRUE)
    base::message("Database connection closed")
  }
})

base::rm(con,gold_path,org_id)

# 2. Copy dataset for transformation

zero_counts <- colSums(affiliates[,-1] == 0)
(zero_percentages <- round((zero_counts / nrow(affiliates[,-1])) * 100,2))

affiliates <- affiliates %>%
  dplyr::select(-jurisdiccion_3_Ingreso)

affiliates_t <- affiliates

# 3. Identify columns for Box-Cox transformation (from "ingresos" to "jurisdiccion_0_Egreso")
boxcox_columns <- base::seq(from = base::which(base::names(affiliates_t) == "ingresos"),
                            to = base::which(base::names(affiliates_t) == "jurisdiccion_0_Egreso"))

# 4. Apply Box-Cox transformation to each selected column
lambda_values <- base::numeric(length(boxcox_columns))
for (i in base::seq_along(boxcox_columns)) {
  col_index <- boxcox_columns[i]
  temp <- base::min(affiliates_t[[col_index]], na.rm = TRUE)
  shift_value <- base::ifelse(temp <= 0, temp + 1, 0)
  x <- affiliates_t[[col_index]] + shift_value
  lambda <- base::as.numeric(summary(car::powerTransform(x))$result[, 2])
  if (lambda == 0) {
    affiliates_t[[col_index]] <- base::log(x)
  } else {
    affiliates_t[[col_index]] <- (x^lambda - 1) / lambda
  }
  lambda_values[i] <- lambda
  base::rm(temp,shift_value,lambda,col_index,x)
}

lambda_values

base::rm(i, boxcox_columns)

# Load required packages
if (!base::requireNamespace("ggplot2", quietly = TRUE)) utils::install.packages("ggplot2")
if (!base::requireNamespace("gridExtra", quietly = TRUE)) utils::install.packages("gridExtra")

# Function to create histogram plots
create_histogram_matrix <- function(original_df, transformed_df) {
  # Get common variable names (excluding any ID or non-numeric columns)
  var_names <- base::intersect(
    base::names(original_df),
    base::names(transformed_df)
  )
  
  # Filter out potential non-numeric columns
  numeric_vars <- base::sapply(original_df[var_names], base::is.numeric)
  var_names <- var_names[numeric_vars]
  
  # Initialize plot list
  plot_list <- base::list()
  plot_count <- 1
  
  # Create histograms for each variable
  for (var in var_names) {
    # Original data histogram
    orig_hist <- ggplot2::ggplot(original_df, ggplot2::aes_string(x = var)) +
      ggplot2::geom_histogram(fill = "steelblue", color = "white", bins = 20) +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        axis.title.x = ggplot2::element_blank(),
        axis.title.y = ggplot2::element_blank(),
        plot.title = ggplot2::element_blank(),
        plot.margin = ggplot2::unit(c(0.1, 0.1, 0.1, 0.1), "cm")
      ) +
      ggplot2::annotate("text", x = Inf, y = Inf, 
                        label = "Original", hjust = 1.1, vjust = 1.5,
                        size = 3)
    
    # Transformed data histogram
    trans_hist <- ggplot2::ggplot(transformed_df, ggplot2::aes_string(x = var)) +
      ggplot2::geom_histogram(fill = "coral", color = "white", bins = 20) +
      ggplot2::theme_minimal() +
      ggplot2::theme(
        axis.title.x = ggplot2::element_blank(),
        axis.title.y = ggplot2::element_blank(),
        plot.title = ggplot2::element_blank(),
        plot.margin = ggplot2::unit(c(0.1, 0.1, 0.1, 0.1), "cm")
      ) +
      ggplot2::annotate("text", x = Inf, y = Inf, 
                        label = "Transformed", hjust = 1.1, vjust = 1.5,
                        size = 3)
    
    # Add variable name as small text in the top-left corner of original plot
    orig_hist <- orig_hist + 
      ggplot2::annotate("text", x = -Inf, y = Inf, 
                        label = var, hjust = -0.1, vjust = 1.5,
                        size = 3, fontface = "bold")
    
    # Add plots to list
    plot_list[[plot_count]] <- orig_hist
    plot_list[[plot_count + length(var_names)]] <- trans_hist
    
    plot_count <- plot_count + 1
  }
  
  # Arrange plots in a matrix (original variables in first row, transformed in second)
  n_vars <- base::length(var_names)
  gridExtra::grid.arrange(
    base::do.call(gridExtra::arrangeGrob, 
                  c(plot_list, 
                    list(ncol = n_vars, nrow = 2))),
    top = ""
  )
}

create_histogram_matrix(affiliates[,c(11:15)], affiliates_t[,c(11:15)])
create_histogram_matrix(affiliates[,c(16:19)], affiliates_t[,c(16:19)])
create_histogram_matrix(affiliates[,c(20:23)], affiliates_t[,c(20:23)])
create_histogram_matrix(affiliates[,c(24:29)], affiliates_t[,c(24:29)])


# 5. Perform PCA (exclude first column if it's an identifier)
pca_results <- FactoMineR::PCA(affiliates_t[, -1], scale.unit = TRUE, ncp = 13, graph = FALSE)
pca_results$eig
# Optional scree plot:
factoextra::fviz_eig(pca_results, addlabels = TRUE)
factoextra::fviz_pca_var(pca_results, col.var = "cos2",
                         gradient.cols = gg_color(5), 
                         repel = TRUE # Avoid text overlapping
)

factoextra::fviz_pca_ind(pca_results, 
                         geom.ind = "point",   # Show only points
                         labelsize = 0,        # Remove labels
                         addEllipses = FALSE,  # Disable ellipses
                         mean.point = FALSE,   # Remove mean points
                         alpha.ind = 0.8,      # Point transparency (0-1)
                         pointsize = 0.5         # Point size
)

scores <- base::as.data.frame(pca_results$ind$coord)


# 6. Choosing the best clustering algorithm 

set.seed(444)  # For reproducibility

k_clusters <- 3000

centers_sample <- stats::kmeans(scores, centers = k_clusters, iter.max = 100, nstart = 25)
centers_sample$ifault
centers <- centers_sample$centers

k_range <- 2:6  # Adjust based on your n1:n2
B <- 100

clmethods <- c("hierarchical", "kmeans", "pam", "clara")

validation <- clValid::clValid(
  obj = centers,
  nClust = k_range,  # Use the same k_range (2:6) as in clusterboot
  clMethods = clmethods,  # Use the same methods
  validation = "internal",  # Only need internal metrics
  maxitems = 3000,  # Adjust based on your centers matrix size
  method = "ward"
)

# Load required packages
library(foreach)
library(doParallel)

# Register parallel backend
cl <- parallel::makeCluster(parallel::detectCores() - 1)
doParallel::registerDoParallel(cl)


run_clusterboot <- function(data, methods, k_range, B) {
  cl <- parallel::makeCluster(parallel::detectCores()-1)
  doParallel::registerDoParallel(cl)
  
  results <- foreach::foreach(method = rep(methods, each=base::length(k_range)), 
                              k = rep(k_range, base::length(methods)),
                              .combine = base::c, 
                              .packages = c("fpc", "clValid")) %dopar% {
                                # Method-specific parameters
                                params <- base::switch(method,
                                                       "hierarchical" = base::list(clustermethod = fpc::hclustCBI, method = "ward.D2"),
                                                       "kmeans" = base::list(clustermethod = fpc::kmeansCBI),
                                                       "pam" = base::list(clustermethod = fpc::pamkCBI),
                                                       "clara" = base::list(clustermethod = fpc::claraCBI)
                                )
                                
                                # Run clusterboot
                                base::set.seed(444)
                                cb <- base::tryCatch({
                                  base::do.call(fpc::clusterboot, c(base::list(
                                    data = data,
                                    B = B,
                                    bootmethod = "boot",
                                    k = k
                                  ), params))
                                }, error = function(e) NULL)
                                
                                base::list(base::list(method = method, k = k, result = cb))
                              }
  parallel::stopCluster(cl)
  return(results)
}

# 3. Process stability results with integrated clValid metrics
process_stability_enhanced <- function(results, clvalid_obj) {
  purrr::map_dfr(results, function(res) {
    if(base::is.null(res$result)) return(NULL)
    
    # Extract clusterboot metrics
    cb_metrics <- base::with(res$result, {
      base::data.frame(
        Jaccard_min = base::min(bootmean),
        Jaccard_mean = base::mean(bootmean),
        Jaccard_max = base::max(bootmean),
        Dissolved = base::paste(bootbrd, collapse = ", "),
        Recovered = base::paste(bootrecover, collapse = ", ")
      )
    })
    
    # Extract clValid metrics
    clv_metrics <- base::tryCatch({
      idx <- base::which(clvalid_obj@clMethods == res$method)
      k_idx <- base::which(clvalid_obj@nClust == res$k)
      
      base::data.frame(
        Connectivity = clvalid_obj@measures["Connectivity", k_idx, idx],
        Dunn = clvalid_obj@measures["Dunn", k_idx, idx],
        Silhouette = clvalid_obj@measures["Silhouette", k_idx, idx]
      )
    }, error = function(e) base::data.frame(Connectivity = NA, Dunn = NA, Silhouette = NA))
    
    # Combine all metrics
    base::cbind(
      base::data.frame(
        Method = base::paste0(base::toupper(base::substr(res$method, 1, 1)), 
                              base::substr(res$method, 2, base::nchar(res$method))),
        K = res$k
      ),
      clv_metrics,
      cb_metrics
    )
  })
}

# 4. Generate final summary table
create_summary_table <- function(processed_data) {
  # Order columns
  processed_data %>%
    dplyr::select(Method, K, Connectivity, Dunn, Silhouette,
                  Jaccard_min, Jaccard_mean, Jaccard_max,
                  Dissolved, Recovered) %>%
    dplyr::arrange(Method, K) %>%
    dplyr::mutate(dplyr::across(c(Connectivity, Dunn, Silhouette, 
                                  Jaccard_min, Jaccard_mean, Jaccard_max),
                                ~ base::round(., 3))) %>%
    dplyr::rename_with(~ base::gsub("_", " ", .x)) 
}

# Usage example -----------------------------------------------------------
# Assuming 'centers' is your data and 'validation' is clValid object

# Run clusterboot for all method/k combinations
cb_results <- run_clusterboot(centers, clmethods, k_range, B)

# Process results with clValid integration
processed_data <- process_stability_enhanced(cb_results, validation)

# Create final formatted table
final_summary <- create_summary_table(processed_data)

# Print formatted table
knitr::kable(final_summary, caption = "Comprehensive Clustering Evaluation")

# 1. Prepare normalized data for radar chart
# Select relevant metrics
radar_data <- final_summary %>%
  dplyr::select(Method, K, Connectivity, Dunn, Silhouette, `Jaccard mean`)

# Normalize metrics (0-1 scale)
for (metric in c("Connectivity", "Dunn", "Silhouette", "Jaccard mean")) {
  radar_data[[metric]] <- scales::rescale(radar_data[[metric]], to = c(0, 1))
}

# 2. Create radar chart function with fmsb
create_fmsb_radar <- function(data, method_name, k_value) {
  # Filter data for specific method and k
  plot_data <- data %>%
    dplyr::filter(Method == method_name & K == k_value) %>%
    dplyr::select(-Method, -K)
  
  # Add max and min rows required by fmsb
  plot_data <- rbind(
    rep(1, ncol(plot_data)),  # Max values
    rep(0, ncol(plot_data)),  # Min values
    plot_data
  )
  
  # Create color palette
  colors <- c("#1B9E77", "#D95F02", "#7570B3", "#E7298A")  # ColorBrewer Set2
  
  # Create radar chart
  fmsb::radarchart(
    plot_data,
    axistype = 1,
    pcol = colors[which(unique(data$Method) == method_name)],
    pfcol = scales::alpha(colors[which(unique(data$Method) == method_name)], 0.3),
    plwd = 2,
    cglcol = "grey",
    cglty = 1,
    axislabcol = "grey30",
    vlcex = 0.8,
    title = paste("Method:", method_name, "| Clusters:", k_value)
  )
}

# 3. Create comparison grid of radar charts
# Set up plotting grid
graphics::par(mfrow = c(2, 2), mar = c(1, 1, 2, 1))

# Generate charts for key configurations
create_fmsb_radar(radar_data, "Pam", 5)
create_fmsb_radar(radar_data, "Clara", 5)
create_fmsb_radar(radar_data, "Hierarchical", 5)
create_fmsb_radar(radar_data, "Hierarchical", 6)

graphics::par(mfrow = c(1, 1), mar = c(1, 1, 2, 1))


# 9. Set number of clusters and perform clustering
k <- 6

# Perform pam clustering on the centers using fcp
hc_result <- fpc::hclustCBI(centers, k = k, method = "ward.D2",scaling = FALSE)

# Define specific colors for clusters 1, 2, and 3
cluster_colors <- c("1" = "#F8766D", "2" = "#B79F00", "3" = "#00BA38", "4" = "#00BFC4", "5" = "#619CFF", "6" = "#F564E3")


# Create a color palette that respects the original cluster numbering
color_palette <- cluster_colors[base::as.character(base::sort(base::unique(hc_result$partition)))]


# Extract the hclust object from hc_result
hclust_obj <- hc_result$result

# Create the dendrogram plot with proper color mapping
factoextra::fviz_dend(
  hclust_obj,
  cex = 0.5,
  k = 6,
  k_colors = cluster_colors,
  rect = TRUE
)

factoextra::fviz_dend(hclust_obj, cex = 0.5, k = k, k_colors = cluster_colors, rect = TRUE)

centers_dist <- stats::dist(centers)

sil_widths <- cluster::silhouette(
  hc_result$partition,
  centers_dist
)

# Visualize silhouette scores
factoextra::fviz_silhouette(sil_widths) +
  ggplot2::scale_fill_manual(values = gg_color(6)) +
  ggplot2::labs(
    title = "Cluster Quality Assessment"
  ) + ggplot2::theme(legend.position = "bottom")

# 10. Assign clusters to original data
# Map back to original data points
# Each original point inherits the cluster of its k-means center
affiliates <- affiliates %>%
  dplyr::mutate(cluster = base::factor(hc_result$partition[centers_sample$cluster]))


# 10. Visualize cluster results

# 10a. Scatter Plot: First 2 principal components with clusters
clus_res <- base::data.frame(scores[, 1:2], cluster = factor(affiliates$cluster))
ggpubr::ggscatter(clus_res,
                  x = "Dim.1", y = "Dim.2", color = "cluster",
                  repel = FALSE, ellipse = TRUE, ellipse.type = "none",
                  shape = 19, size = 1, 
                  xlab = paste0("Dim.1"," (", round(pca_results$eig[1,2],1),"%)"), 
                  ylab = paste0("Dim.2"," (", round(pca_results$eig[2,2],1),"%)"), 
                  ggtheme = ggplot2::theme_minimal()) + 
  ggplot2::geom_hline(ggplot2::aes(yintercept = 0), color = "black", linetype = "dashed") +
  ggplot2::geom_vline(ggplot2::aes(xintercept = 0), color = "black", linetype = "dashed") +
  ggplot2::theme(legend.position = "bottom")

# 12. (Optional) Pie Chart of Cluster Proportions
cluster_counts <- base::table(affiliates$cluster)
labels <- base::paste0("Cl. ", base::names(cluster_counts), ": ", cluster_counts, "; ", base::round(100 * base::prop.table(cluster_counts), 1), "%")
# Save pie chart as PNG
plotrix::pie3D(cluster_counts,
               labels = labels, explode = 0.1, radius = 0.9, height = 0.05,
               col = gg_color(k), labelcex = 0.7, main = "Proporción de Clústeres"
)


# Function to create radar charts displayed in RStudio
create_cluster_radar_charts <- function(data, features_to_plot) {
  # Exclude identifier and group by cluster
  features <- data[, c(features_to_plot, "cluster")]
  
  # Calculate means by cluster
  cluster_means <- stats::aggregate(. ~ cluster, data = features, FUN = base::mean)
  
  # Set row names to cluster and remove cluster column
  base::rownames(cluster_means) <- base::paste0("Cluster ", cluster_means$cluster)
  cluster_means$cluster <- NULL
  
  # Normalize data to 0-1 scale
  normalize <- function(x) {
    return((x - base::min(x)) / (base::max(x) - base::min(x)))
  }
  
  normalized_means <- base::as.data.frame(
    base::apply(cluster_means, 2, normalize)
  )
  
  # Add max and min rows required by fmsb
  radar_data <- base::rbind(
    base::rep(1, base::ncol(normalized_means)),  # Max values
    base::rep(0, base::ncol(normalized_means)),  # Min values
    normalized_means
  )
  
  # Set color palette
  colors <- gg_color(k)
  
  # Set up plotting layout for RStudio
  graphics::par(mfrow = c(2, 3), mar = c(1, 1, 3, 1))
  
  # Plot each cluster
  for (i in 1:k) {
    cluster_name <- base::paste0("Cluster ", i)
    fmsb::radarchart(
      radar_data[c(1, 2, i + 2), ],  # Select max, min, and cluster data
      pcol = colors[i],
      pfcol = grDevices::adjustcolor(colors[i], alpha.f = 0.5),
      plwd = 3,
      cglcol = "grey",
      cglty = 1,
      axislabcol = "grey30",
      vlcex = 1.0,
      title = base::paste0(cluster_name, " Feature Profile")
    )
  }
  
  # Reset plotting parameters
  graphics::par(mfrow = c(1, 1))
}

# Execute function with your data

create_cluster_radar_charts(
  data = affiliates,
  features_to_plot = names(affiliates)[2:8]
)

create_cluster_radar_charts(
  data = affiliates,
  features_to_plot = names(affiliates)[11:15]
)

create_cluster_radar_charts(
  data = affiliates,
  features_to_plot = names(affiliates)[16:19]
)

create_cluster_radar_charts(
  data = affiliates,
  features_to_plot = names(affiliates)[20:23]
)

create_cluster_radar_charts(
  data = affiliates,
  features_to_plot = names(affiliates)[24:29]
)

# 10b. Boxplot: For key variable "num_transa" by cluster (if available)
if ("num_transa" %in% base::names(affiliates)) {
  ggplot2::ggplot(affiliates, ggplot2::aes(x = cluster, y = num_transa, fill = cluster)) +
    ggplot2::geom_boxplot() +
    ggplot2::labs(title = "Distribución de Num. Transacciones por Clúster", x = "Clúster", y = "Num. Trans.") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
} else {
  base::message("Variable 'num_transa' no encontrada; no se crea boxplot.")
}

if ("Ahorro_Egreso" %in% base::names(affiliates)) {
  ggplot2::ggplot(affiliates, ggplot2::aes(x = cluster, y = Ahorro_Egreso, fill = cluster)) +
    ggplot2::geom_boxplot() +
    ggplot2::labs(title = "Distribución de Transacciones de Ahorro Egreso por Clúster", x = "Clúster", y = "Trans. Ahorro Egreso") +
    ggplot2::theme_minimal() +
    ggplot2::theme(legend.position = "bottom")
} else {
  base::message("Variable 'num_transa' no encontrada; no se crea boxplot.")
}

# Define dichotomous variables
binary_vars <- c("comercio", "construccion", "ind_alimentaria",
                 "ind_manufacturera", "no_clasificado", "otros_servicios",
                 "servicios_prof_tecn", "activo", "inactivo")

# 1. Table for binary variables (means)
binary_table <- affiliates %>%
  dplyr::select(dplyr::all_of(c("cluster", binary_vars))) %>%
  dplyr::group_by(cluster) %>%
  dplyr::summarize(
    dplyr::across(dplyr::everything(),
                  ~ base::mean(.x, na.rm = TRUE))
  ) %>%
  tidyr::pivot_longer(
    cols = -cluster,
    names_to = "variable",
    values_to = "value"
  ) %>%
  tidyr::pivot_wider(
    names_from = cluster,
    values_from = value
  )

# 2. Table for continuous variables (medians)
continuous_table <- affiliates %>%
  dplyr::select(-num_ident) %>%
  dplyr::select(-dplyr::all_of(binary_vars)) %>%
  dplyr::group_by(cluster) %>%
  dplyr::summarize(
    dplyr::across(dplyr::everything(),
                  ~ stats::median(.x, na.rm = TRUE))
  ) %>%
  tidyr::pivot_longer(
    cols = -cluster,
    names_to = "variable",
    values_to = "value"
  ) %>%
  tidyr::pivot_wider(
    names_from = cluster,
    values_from = value
  )

# 3. Combine tables
final_table <- dplyr::bind_rows(binary_table, continuous_table) 

# 4. Format output
knitr::kable(final_table, 
             caption = "Cluster Statistics: Means for Binary Variables, Medians for Others")

# Calculate median, IQR, and upper-sided 95% CI for each variable by cluster
alerts <- affiliates %>%
  dplyr::select(-num_ident) %>%
  dplyr::group_by(cluster) %>%
  dplyr::summarize(
    dplyr::across(
      names(affiliates)[11:29],
      list(
        xq3 = ~stats::quantile(.x, probs = 0.75, type = 6, na.rm = TRUE),
        xiqr = ~stats::IQR(.x, type = 6, na.rm = TRUE),
        xalert = ~stats::quantile(.x, probs = 0.75, type = 6, na.rm = TRUE) + (1.5 * stats::IQR(.x, type = 6, na.rm = TRUE))
      )
    )
  ) %>%
  dplyr::ungroup()

# Reshape to have variables as rows and cluster statistics as columns
alerts <- alerts %>%
  tidyr::pivot_longer(
    cols = -cluster,
    names_to = c("variable", "stat_type"),
    names_sep = "_x",
    values_to = "value"
  ) %>%
  tidyr::pivot_wider(
    names_from = c(cluster, stat_type),
    values_from = value,
    names_glue = "{cluster}_{stat_type}"
  )


# Print as a formatted table
knitr::kable(alerts, 
             caption = "Early Alert System by Cluster")


# 13. Save results: Export full segmentation to Excel and save workspace objects
writexl::write_xlsx(affiliates, "Clients/0002_Coopcentral/Model_2022_2023_2024/Results/SARLAFT_Coopcentral_Segmentacion_Persona_Natural.xlsx")
base::save.image("Clients/0002_Coopcentral/Model_2022_2023_2024/Results/results_Persona_Natural.RData")

# Clear environment
base::rm(list = base::ls())
