library(sf)
library(ggplot2)

args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 3) {
  stop("Usage: Rscript analysis_script.R [csv_dir] [gpkg_dir] [output_dir]")
}
csv_dir   <- args[1]
gpkg_dir  <- args[2]
output_dir <- args[3]

# Ensure output directory exists:
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

csv_files <- list.files(csv_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
gpkg_files <- list.files(gpkg_dir, pattern = "\\.gpkg$", full.names = TRUE, recursive = TRUE)

extract_key <- function(filepath) {
  parts <- strsplit(basename(filepath), "_")[[1]]
  if (length(parts) < 3) {
    stop(paste("Filename", filepath, "does not have enough parts"))
  }
  paste(parts[length(parts) - 2], parts[length(parts) - 1], sep = "_")
}

csv_keys  <- sapply(csv_files, extract_key)
gpkg_keys <- sapply(gpkg_files, extract_key)
common_keys <- intersect(csv_keys, gpkg_keys)
if (length(common_keys) == 0) {
  stop("No matching file pairs found based on keys.")
}

for (key in common_keys) {
  # Get the first matching file for each type (if multiples, adjust as needed)
  csv_file  <- csv_files[which(csv_keys == key)[1]]
  gpkg_file <- gpkg_files[which(gpkg_keys == key)[1]]
  
  # Read CSV and rename 'biwf' column
  csv_df <- read.csv(csv_file, stringsAsFactors = FALSE)
  names(csv_df)[names(csv_df) == "biwf"] <- "BIWF (AR estimate)"
  
  # Read GPKG (using sf) and drop geometry if not required; rename 'biwf'
  gpkg_df <- st_read(gpkg_file, quiet = TRUE)
  gpkg_df <- st_drop_geometry(gpkg_df)
  names(gpkg_df)[names(gpkg_df) == "biwf"] <- "BIWF (IG estimate)"
  
  # Merge on shot_number (assumes both files contain a column named 'shot_number')
  merged_df <- merge(csv_df, gpkg_df, by = "shot_number")
  
  if (nrow(merged_df) == 0) {
    warning(paste("No matching shot_number data found for key", key))
    next
  }
  
  # Create scatterplot using ggplot2
  p <- ggplot(merged_df, aes(x = `BIWF (IG estimate)`, y = `BIWF (AR estimate)`)) +
    geom_point() +
    geom_abline(intercept = 0, slope = 1, color = "red", linetype = "dashed") +
    ggtitle(paste("Scatterplot for key:", key)) +
    xlab("BIWF (IG estimate)") +
    ylab("BIWF (AR estimate)")
  
  # Save the plot to the output directory with a filename based on the key
  ggsave(filename = file.path(output_dir, paste0(key, ".png")), plot = p)
}
