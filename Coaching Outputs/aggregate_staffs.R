#!/usr/bin/env Rscript
# NFL Coaching Staff Aggregator - R Version
# Combines multiple staff recommendation CSVs to compare which head coaches
# have the strongest potential staffs available.

library(tidyverse)

# ============================================================================
# CONFIGURATION
# ============================================================================

# Directory containing staff recommendation CSVs
INPUT_DIRECTORY <- "C:/staff_recommendations"  # UPDATE THIS

# Output files
OUTPUT_FILE <- "staff_comparison.csv"
SUMMARY_FILE <- "staff_rankings.csv"

# Minimum connection score to be considered a "quality" candidate
MIN_QUALITY_SCORE <- 0.5

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

extract_hc_name_from_filename <- function(filename) {
  # Extract head coach name from filename
  # Expected format: staff_recommendations_FirstName_LastName.csv
  basename <- basename(filename)
  
  # Remove .csv extension
  name_part <- str_replace(basename, "\\.csv$", "")
  
  # Remove common prefixes/suffixes
  name_part <- name_part %>%
    str_replace("staff_recommendations_", "") %>%
    str_replace("_staff_recommendations", "") %>%
    str_replace("staff_recs_", "") %>%
    str_replace("_staff_recs", "")
  
  # Replace underscores with spaces
  name_part <- str_replace_all(name_part, "_", " ")
  
  return(str_trim(name_part))
}

calculate_staff_metrics <- function(df) {
  # Get top candidate for each position
  top_candidates <- df %>%
    group_by(target_position, target_side) %>%
    arrange(desc(connection_score)) %>%
    slice(1) %>%
    ungroup()
  
  # Calculate metrics
  metrics <- tibble(
    total_positions = nrow(top_candidates),
    avg_connection_score = mean(top_candidates$connection_score, na.rm = TRUE),
    median_connection_score = median(top_candidates$connection_score, na.rm = TRUE),
    avg_coach_value = mean(top_candidates$coach_value, na.rm = TRUE),
    avg_years_together = mean(top_candidates$years_together, na.rm = TRUE),
    pct_direct_connections = sum(top_candidates$degree == 1) / nrow(top_candidates) * 100,
    pct_quality_candidates = sum(top_candidates$connection_score >= MIN_QUALITY_SCORE) / nrow(top_candidates) * 100,
    total_years_experience = sum(top_candidates$years_together, na.rm = TRUE),
    top_3_avg_score = mean(top_n(top_candidates, 3, connection_score)$connection_score, na.rm = TRUE)
  )
  
  # Coordinator average
  coordinator_scores <- top_candidates %>%
    filter(str_detect(target_position, "Coordinator"))
  
  if (nrow(coordinator_scores) > 0) {
    metrics$coordinator_avg_score <- mean(coordinator_scores$connection_score, na.rm = TRUE)
  } else {
    metrics$coordinator_avg_score <- NA_real_
  }
  
  return(metrics)
}

create_position_key <- function(target_position, target_side) {
  paste(target_position, target_side, sep = "_")
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

cat("NFL Coaching Staff Aggregator\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Find all CSV files in the input directory
csv_files <- list.files(
  path = INPUT_DIRECTORY,
  pattern = "\\.csv$",
  full.names = TRUE
)

if (length(csv_files) == 0) {
  stop(sprintf("ERROR: No CSV files found in %s\nPlease update INPUT_DIRECTORY in the script configuration.", INPUT_DIRECTORY))
}

cat(sprintf("Found %d CSV files to process\n\n", length(csv_files)))

all_staffs <- list()
staff_summaries <- list()

# Process each CSV file
for (csv_file in csv_files) {
  tryCatch({
    # Extract HC name from filename
    hc_name <- extract_hc_name_from_filename(csv_file)
    
    # Read the staff recommendations
    df <- read_csv(csv_file, show_col_types = FALSE)
    
    # Add HC name and position key
    df <- df %>%
      mutate(
        target_hc = hc_name,
        position_key = create_position_key(target_position, target_side)
      )
    
    # Calculate staff metrics
    metrics <- calculate_staff_metrics(df)
    
    # Add to summary
    summary <- metrics %>%
      mutate(
        target_hc = hc_name,
        filename = basename(csv_file)
      ) %>%
      select(target_hc, filename, everything())
    
    staff_summaries[[length(staff_summaries) + 1]] <- summary
    
    # Add to combined data
    all_staffs[[length(all_staffs) + 1]] <- df
    
    cat(sprintf("✓ Processed: %s (%d candidates)\n", hc_name, nrow(df)))
    
  }, error = function(e) {
    cat(sprintf("✗ Error processing %s: %s\n", csv_file, e$message))
  })
}

if (length(all_staffs) == 0) {
  stop("\nNo valid data processed. Exiting.")
}

# Combine all staffs into single DataFrame
combined_df <- bind_rows(all_staffs)

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("AGGREGATING BY POSITION\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Aggregate by HC and position to get best candidate for each position
position_aggregates <- combined_df %>%
  group_by(target_hc, position_key) %>%
  summarise(
    avg_connection_score = mean(connection_score, na.rm = TRUE),
    max_connection_score = max(connection_score, na.rm = TRUE),
    num_candidates = n(),
    avg_coach_value = mean(coach_value, na.rm = TRUE),
    avg_years_together = mean(years_together, na.rm = TRUE),
    pct_direct_connections = sum(degree == 1) / n() * 100,
    target_position = first(target_position),
    target_side = first(target_side),
    .groups = "drop"
  )

# Create staff rankings
staff_rankings <- bind_rows(staff_summaries) %>%
  arrange(desc(avg_connection_score)) %>%
  mutate(
    overall_rank = row_number(),
    coordinator_rank = rank(desc(coordinator_avg_score), na.last = TRUE, ties.method = "min"),
    experience_rank = rank(desc(total_years_experience), na.last = TRUE, ties.method = "min")
  )

# ============================================================================
# OUTPUT RESULTS
# ============================================================================

cat(paste(rep("=", 70), collapse = ""), "\n")
cat("TOP 10 COACHING STAFFS BY AVERAGE CONNECTION SCORE\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

top_10 <- head(staff_rankings, 10)

for (i in 1:nrow(top_10)) {
  row <- top_10[i, ]
  cat(sprintf("%d. %s\n", row$overall_rank, row$target_hc))
  cat(sprintf("   Avg Score: %.3f\n", row$avg_connection_score))
  cat(sprintf("   Top 3 Positions Avg: %.3f\n", row$top_3_avg_score))
  cat(sprintf("   Coordinator Avg: %.3f\n", ifelse(is.na(row$coordinator_avg_score), 0, row$coordinator_avg_score)))
  cat(sprintf("   Direct Connections: %.1f%%\n", row$pct_direct_connections))
  cat(sprintf("   Total Years Together: %.0f\n\n", row$total_years_experience))
}

# Save detailed results
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("SAVING RESULTS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

# Save position-level aggregates
write_csv(position_aggregates, OUTPUT_FILE)
cat(sprintf("✓ Position aggregates saved to: %s\n", OUTPUT_FILE))

# Save staff rankings
write_csv(staff_rankings, SUMMARY_FILE)
cat(sprintf("✓ Staff rankings saved to: %s\n", SUMMARY_FILE))

# Save combined raw data
combined_file <- str_replace(OUTPUT_FILE, "\\.csv$", "_all_candidates.csv")
write_csv(combined_df, combined_file)
cat(sprintf("✓ All candidates saved to: %s\n", combined_file))

cat("\n")
cat(paste(rep("=", 70), collapse = ""), "\n")
cat("SUMMARY STATISTICS\n")
cat(paste(rep("=", 70), collapse = ""), "\n\n")

cat(sprintf("Total Head Coaches Analyzed: %d\n", nrow(staff_rankings)))
cat(sprintf("Total Positions Analyzed: %d\n", n_distinct(combined_df$position_key)))
cat(sprintf("Total Candidates Evaluated: %d\n", nrow(combined_df)))
cat(sprintf("Average Staff Score: %.3f\n", mean(staff_rankings$avg_connection_score, na.rm = TRUE)))
cat(sprintf("Best Staff Score: %.3f (%s)\n", 
            max(staff_rankings$avg_connection_score, na.rm = TRUE),
            staff_rankings$target_hc[1]))
cat(sprintf("Worst Staff Score: %.3f\n\n", min(staff_rankings$avg_connection_score, na.rm = TRUE)))

# Create a comparison matrix (top candidates by position for each HC)
cat("Creating position comparison matrix...\n")

pivot_scores <- position_aggregates %>%
  select(position_key, target_hc, max_connection_score) %>%
  pivot_wider(
    names_from = target_hc,
    values_from = max_connection_score,
    values_fill = 0
  )

pivot_file <- str_replace(OUTPUT_FILE, "\\.csv$", "_position_matrix.csv")
write_csv(pivot_scores, pivot_file)
cat(sprintf("✓ Position matrix saved to: %s\n", pivot_file))

cat("\nDone!\n")



coach_and_staff <- filtered_list %>% 
  left_join(staff_rankings, by = c('Name' = 'target_hc')) %>% 
  #filter(last_role %in% c('Head Coach', 'Offensive Coordinator', 'Defensive Coordinator'))
  na.omit()

staff_clusters <-  coach_and_staff %>% 
  select(CompositeValue, 
         avg_coach_value)

# Function to calculate total within-cluster sum of squares for different k
wss <- function(data, k) {
  kmeans(data, k, nstart = 25)$tot.withinss
}

# Calculate WSS for k = 1 to k = 10
k_values <- 1:20
wss_values <- sapply(k_values, wss, data = staff_clusters)

# Plot the Elbow Method
elbow_plot <- data.frame(k = k_values, wss = wss_values)
ggplot(elbow_plot, aes(x = k, y = wss)) +
  geom_point() +
  geom_line() +
  labs(title = "Elbow Method for Finding Optimal k",
       x = "Number of Clusters (k)",
       y = "Total Within-Cluster Sum of Squares")




staff_kmeans <- kmeans(staff_clusters, centers = 3, nstart = 25)

coach_and_staff$cluster = staff_kmeans$cluster



ggplot(coach_and_staff, aes(x = CompositeValue, y = avg_coach_value, color = cluster))+
  geom_point()+
  ggrepel::geom_text_repel(aes(label = Name), size = 4, box.padding = 0.2,
                         force = 30, max.overlaps = Inf,
                         min.segment.length = 0) +
  labs(
    y= "Average Staff Value",
    x= "Personal Composite Value",
    title= "Comparing HC Candidates Personal Composite Value vs their expected staff's value",
    caption = "@SethDataScience"
  ) +
  theme_reach()
