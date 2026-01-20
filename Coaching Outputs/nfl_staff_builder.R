#!/usr/bin/env Rscript
# NFL Coaching Staff Builder
# Identifies potential coaching candidates based on network connections,
# performance metrics, and tenure together

library(tidyverse)
library(igraph)

# ============================================================================
# CONFIGURATION
# ============================================================================


# Target head coach to build staff for
TARGET_HC <- "Jim Schwartz"  # Change this to the desired head coach

# Maximum network degree to search (2 = friends of friends)
MAX_DEGREE <- 2

# ============================================================================
# LOAD AND PREPARE DATA
# ============================================================================

cat("Loading data...\n")
network_data <- base_df_pos

updated_value_mapping <- closeness_mapping_manual %>% 
  filter(role_category.y == 'Head Coach', side_of_ball.x != 'Both' | role_category.x == 'Head Coach') %>% 
  select(role_category.x, side_of_ball.x, closeness)

role_mapping <- updated_value_mapping

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

#' Calculate weighted connection score between two coaches
#' @param coach_value Performance value of the candidate coach
#' @param years_together Number of years worked together
#' @param degree Network distance (1 = direct, 2 = indirect)
#' @return Weighted connection score
calculate_connection_score <- function(coach_value, years_together, degree) {
  # Base score from coach performance (0-1 range assumed)
  performance_weight <- ifelse(is.na(coach_value), 0.3, coach_value)
  
  # Tenure bonus: exponential scaling for years together
  # 1 year = 1.0x, 2 years = 1.5x, 3 years = 2.0x, 4+ years = 2.5x
  tenure_multiplier <- case_when(
    years_together >= 4 ~ 2.5,
    years_together == 3 ~ 2.0,
    years_together == 2 ~ 1.5,
    TRUE ~ 1.0
  )
  
  # Degree penalty: prefer direct connections
  degree_multiplier <- case_when(
    degree == 1 ~ 1.0,
    degree == 2 ~ 0.5,
    TRUE ~ 0.1
  )
  
  # Final weighted score
  score <- performance_weight * tenure_multiplier * degree_multiplier
  return(score)
}

#' Get promotion target role for a coach
#' @param current_role Current role category
#' @param current_side Current side of ball
#' @param role_map Role mapping dataframe
#' @return List of potential promotion roles (same level or one step up)
get_promotion_targets <- function(current_role, current_side, role_map) {
  # Find current role's closeness score
  current_closeness <- role_map %>%
    filter(role_category.x == current_role, 
           side_of_ball.x == current_side | side_of_ball.x == "Both") %>%
    pull(closeness) %>%
    first()
  
  if (is.na(current_closeness) || length(current_closeness) == 0) {
    return(data.frame(role = current_role, side = current_side))
  }
  
  # Find roles at same level or one step up
  # Maintain side of ball (except "Both" can go to either side)
  target_roles <- role_map %>%
    filter(closeness >= current_closeness,
           closeness <= current_closeness + 0.4) %>%  # One step up max
    {
      if (current_side == "Both") {
        .
      } else {
        filter(., side_of_ball.x == current_side | side_of_ball.x == "Both")
      }
    } %>%
    select(role = role_category.x, side = side_of_ball.x) %>%
    distinct()
  
  return(target_roles)
}

# ============================================================================
# BUILD NETWORK GRAPH
# ============================================================================

cat("Building network graph...\n")

# Get most recent role for each coach (by coach_id only, not by team)
most_recent_from <- network_data %>%
  arrange(desc(year)) %>%
  group_by(from_coach_id) %>%
  slice(1) %>%
  ungroup() %>%
  filter(year >= 2024) %>% 
  select(coach_id = from_coach_id, coach_name = coach.x, 
         most_recent_role = from_role.x, most_recent_side = from_side,
         coach_value = Value.x)

most_recent_to <- network_data %>%
  arrange(desc(year)) %>%
  group_by(to_coach_id) %>%
  slice(1) %>%
  ungroup() %>%
  filter(year >= 2024) %>% 
  select(coach_id = to_coach_id, coach_name = coach.y,
         most_recent_role = to_role.x, most_recent_side = to_side,
         coach_value = Value.y)

# Combine and keep most recent (first occurrence)
vertices_df <- bind_rows(most_recent_from, most_recent_to) %>%
  arrange(coach_id) %>%
  distinct(coach_id, .keep_all = TRUE)

# Create edge list with aggregated years together ACROSS ALL TEAMS
edges <- network_data %>%
  group_by(to_coach_id) %>%
  mutate(to_max_year = max(year)) %>% 
  group_by(from_coach_id) %>%
  mutate(from_max_year = max(year)) %>% 
  filter(from_coach_id != to_coach_id, to_max_year >= 2024, from_max_year >= 2024) %>%
  mutate(
    coach_id_1 = pmin(from_coach_id, to_coach_id),  # Smaller ID
    coach_id_2 = pmax(from_coach_id, to_coach_id),  # Larger ID
    
    # Preserve correct names based on which ID is which
    coach_1_name = if_else(from_coach_id < to_coach_id, coach.x, coach.y),
    coach_2_name = if_else(from_coach_id < to_coach_id, coach.y, coach.x)
  ) %>%
  group_by(coach_id_1, coach_id_2) %>% 
  summarise(
    years_together = n_distinct(year),  # Total unique years across all teams
    coach_from = first(if_else(from_coach_id < to_coach_id, coach.x, coach.y)),
    coach_to = first(if_else(from_coach_id < to_coach_id, coach.y, coach.x)),
    avg_from_value = mean(if_else(from_coach_id < to_coach_id, Value.x, Value.y), na.rm = TRUE),
    avg_to_value = mean(if_else(from_coach_id < to_coach_id, Value.y, Value.x), na.rm = TRUE),
    .groups = "drop"
  )

# Create igraph object
g <- graph_from_data_frame(
  d = edges %>% select(from = coach_id_1, to = coach_id_2),
  directed = FALSE,
  vertices = vertices_df
)

# ============================================================================
# FIND CANDIDATES
# ============================================================================

cat(sprintf("Finding candidates within %d degrees of %s...\n", MAX_DEGREE, TARGET_HC))

# Find target HC's vertex ID
target_vertex <- which(V(g)$coach_name == TARGET_HC)

if (length(target_vertex) == 0) {
  stop(sprintf("Head coach '%s' not found in network!", TARGET_HC))
}

# Find all coaches within MAX_DEGREE
dist_matrix <- distances(g, v = target_vertex, mode = "all")
coach_distances <- as.numeric(dist_matrix[1,])
names(coach_distances) <- V(g)$name

# Get indices of nearby coaches (excluding the HC themselves)
nearby_indices <- which(coach_distances <= MAX_DEGREE & coach_distances > 0)

# Get candidate information
candidates <- data.frame(
  coach_id = V(g)$name[nearby_indices],
  coach_name = V(g)$coach_name[nearby_indices],
  current_role = V(g)$most_recent_role[nearby_indices],
  current_side = V(g)$most_recent_side[nearby_indices],
  coach_value = V(g)$coach_value[nearby_indices],
  degree = coach_distances[nearby_indices],
  stringsAsFactors = FALSE
)

# Calculate years together with target HC
target_id <- V(g)$name[target_vertex]

candidate_connections <- edges %>%
  filter((coach_id_1 == target_id & coach_id_2 %in% candidates$coach_id) |
         (coach_id_2 == target_id & coach_id_1 %in% candidates$coach_id)) %>%
  mutate(
    candidate_id = as.character(ifelse(coach_id_1 == target_id, coach_id_2, coach_id_1)),
    candidate_value = ifelse(coach_id_1 == target_id, avg_to_value, avg_from_value)
  ) %>%
  select(candidate_id, years_together, candidate_value)

candidates <- candidates %>%
  left_join(candidate_connections, by = c("coach_id" = "candidate_id")) %>%
  mutate(
    years_together = replace_na(years_together, 0),
    candidate_value = if_else(is.na(candidate_value), coach_value, candidate_value)
  )

# ============================================================================
# GENERATE RECOMMENDATIONS BY POSITION
# ============================================================================

cat("Generating position recommendations...\n")

# Define staff positions needed (can be customized)
staff_positions <- role_mapping %>%
  filter(role_category.x != "Head Coach") %>%
  select(position = role_category.x, side = side_of_ball.x)

recommendations <- list()
assigned_candidates <- character(0)  # Track candidates already assigned

for (i in 1:nrow(staff_positions)) {
  target_pos <- staff_positions$position[i]
  target_side <- staff_positions$side[i]
  
  cat(sprintf("  Finding candidates for: %s (%s)\n", target_pos, target_side))
  
  # Find coaches who could be promoted to this position FROM THEIR MOST RECENT ROLE
  position_candidates <- candidates %>%
    filter(!coach_id %in% assigned_candidates) %>%  # Exclude already assigned
    rowwise() %>%
    filter({
      promotion_targets <- get_promotion_targets(current_role, current_side, role_mapping)
      any(promotion_targets$role == target_pos & 
          (promotion_targets$side == target_side | target_side == "Both"))
    }) %>%
    ungroup() %>%
    mutate(
      target_position = target_pos,
      target_side = target_side,
      connection_score = calculate_connection_score(
        coach_value, 
        years_together, 
        degree
      )
    ) %>%
    arrange(desc(connection_score)) %>%
    select(
      coach_id,
      candidate_name = coach_name,
      current_role,
      current_side,
      target_position,
      target_side,
      degree,
      years_together,
      coach_value,
      connection_score
    )
  
  if (nrow(position_candidates) > 0) {
    # Mark top candidate as assigned
    assigned_candidates <- c(assigned_candidates, position_candidates$coach_id[1])
    recommendations[[paste(target_pos, target_side, sep = "_")]] <- position_candidates
  }
}

# ============================================================================
# OUTPUT RESULTS
# ============================================================================
# 
# cat("\n")
# cat(paste(rep("=", 70), collapse = ""), "\n")
# cat(sprintf("COACHING STAFF RECOMMENDATIONS FOR: %s\n", TARGET_HC))
# cat(paste(rep("=", 70), collapse = ""), "\n")
# cat("\nNOTE: ⭐ indicates the top candidate assigned to each position\n")
# cat("(Assigned candidates won't appear as options for other positions)\n\n")
# 
# for (position_key in names(recommendations)) {
#   position_data <- recommendations[[position_key]]
#   position_name <- position_data$target_position[1]
#   position_side <- position_data$target_side[1]
#   
#   cat(sprintf("\n%s - %s\n", position_name, position_side))
#   cat(paste(rep("-", 70), collapse = ""), "\n")
#   
#   # Show top 5 candidates
#   top_candidates <- head(position_data, 5)
#   
#   for (j in 1:nrow(top_candidates)) {
#     candidate <- top_candidates[j, ]
#     
#     # Top candidate is assigned
#     assigned_marker <- ifelse(j == 1, " ⭐ ASSIGNED", "")
#     
#     cat(sprintf(
#       "%d. %s%s\n   Current: %s (%s)\n   Connection: Degree %d, %d years together\n   Score: %.3f (Value: %.3f)\n\n",
#       j,
#       candidate$candidate_name,
#       assigned_marker,
#       candidate$current_role,
#       candidate$current_side,
#       candidate$degree,
#       candidate$years_together,
#       candidate$connection_score,
#       ifelse(is.na(candidate$coach_value), 0, candidate$coach_value)
#     ))
#   }
# }

# ============================================================================
# SAVE DETAILED RESULTS
# ============================================================================

cat("\nSaving detailed results...\n")

# Combine all recommendations
all_recommendations <- bind_rows(recommendations, .id = "position_key")

all_recommendations <- all_recommendations %>% 
  group_by(position_key) %>% 
  arrange(desc(connection_score)) %>% 
  slice_head(n = 5)

output_file <- paste0(TARGET_HC, "_staff_recommendations.csv")
write_csv(all_recommendations, output_file)

cat(sprintf("Results saved to: %s\n", output_file))
cat("\nDone!\n")
