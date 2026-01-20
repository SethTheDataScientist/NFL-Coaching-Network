library(igraph)

costaff_df <- read_csv("NFL Coaching Scraper/costaff_df_v3.csv")
edges_df <- read_csv("NFL Coaching Scraper/edges_df_v3.csv")
nodes <- read_csv("NFL Coaching Scraper/nodes_v3.csv")
nodes$name <- as.character(nodes$coach_id)

edges_df <- edges_df %>% 
  filter(year >= 2011)
costaff_df <- costaff_df %>% 
  filter(year >= 2011)

edges_hier <- edges_df
colnames(edges_hier)[1:2] <- c("from", "to")

edges_costaff <- base_df_pos
colnames(edges_costaff)[1:2] <- c("from", "to")

edges_hier$from <- as.character(edges_hier$from)
edges_hier$to   <- as.character(edges_hier$to)

edges_costaff$from <- as.character(edges_costaff$from)
edges_costaff$to   <- as.character(edges_costaff$to)

edges_hier_min <- edges_hier[, c("from", "to")]
edges_costaff_min <- edges_costaff[, c("from", "to")]

nodes_min <- nodes[, c("name", "coach")]



g_tree <- graph_from_data_frame(
  d = edges_hier_min,
  vertices = nodes_min,
  directed = TRUE
)

g_costaff <- graph_from_data_frame(
  d = edges_costaff_min,
  vertices = nodes_min,
  directed = FALSE
)


# Basic graphing ----------------------------------------------------------



# Small induced subgraph to avoid hairball
set.seed(42)
sample_nodes <- sample(V(g_tree), 500)

g_sub <- induced_subgraph(g_tree, sample_nodes)

plot(
  g_sub,
  vertex.size = 4,
  vertex.label = NA,
  edge.arrow.size = 0.03,
  main = "Sample Coaching Tree (Directed)"
)

sample_nodes <- sample(V(g_costaff), 500)
g_sub2 <- induced_subgraph(g_costaff, sample_nodes)

plot(
  g_sub2,
  vertex.size = 4,
  vertex.label = NA,
  main = "Sample Co-Staff Network"
)


# more complicated graphing -----------------------------------------------



library(tidygraph)
library(ggraph)

g_tbl <- as_tbl_graph(g_tree)

g_tbl %>%
  activate(nodes) %>%
  slice_sample(n = 500) %>%
  ggraph(layout = "fr") +
  geom_edge_link(alpha = 0.3) +
  geom_node_point(size = 2) +
  theme_void() +
  ggtitle("Coaching Tree (Sample)")

as_tbl_graph(g_costaff) %>%
  activate(nodes) %>%
  slice_sample(n = 500) %>%
  ggraph(layout = "fr") +
  geom_edge_link(alpha = 0.3) +
  geom_node_point(size = 2) +
  theme_void() +
  ggtitle("Co-Staff Network (Sample)")



# Interactive graph -------------------------------------------------------

library(visNetwork)

keep <- V(g_tree)$out_degree >= 3
g_sparse <- induced_subgraph(g_tree, keep)

nodes_vis <- data.frame(
  id    = V(g_sparse)$name,
  label = V(g_sparse)$coach
)


edges_vis <- data.frame(
  from  = ends(g_sparse, E(g_sparse))[,1],
  to    = ends(g_sparse, E(g_sparse))[,2],
  arrows = "to"
)

visNetwork(nodes_vis, edges_vis) %>%
  visOptions(
    highlightNearest = list(
      enabled = TRUE,
      degree = 0,
      hover = TRUE
    ),
    nodesIdSelection = TRUE
  ) %>%
  visInteraction(
    hover = TRUE,
    navigationButtons = TRUE
  ) %>%
  visEdges(smooth = FALSE) %>%
  visPhysics(
    solver = "forceAtlas2Based",
    forceAtlas2Based = list(
      gravitationalConstant = -500,
      centralGravity = 0.05,
      springLength = 50,
      springConstant = 0.05,
      avoidOverlap = 1
    ),
    stabilization = list(
      iterations = 200)
  ) %>%
  visEvents(
    stabilizationIterationsDone = "function () {
      this.setOptions({ physics: false });
    }"
  ) %>%
  visLegend()


# Basic statistics --------------------------------------------------------


vcount(g_costaff)
ecount(g_costaff)

edge_density(g_costaff, loops = FALSE)

V(g_tree)$in_degree  <- degree(g_tree, mode = "in")
V(g_tree)$out_degree <- degree(g_tree, mode = "out")
V(g_costaff)$degree <- degree(g_costaff)

out_deg <- degree(g_costaff, mode = "out")
top_out_degree <- sort(out_deg, decreasing = TRUE)
top_out_degree <- data.frame(
  coach_id = names(top_out_degree),
  coach_name = V(g_costaff)$coach[match(names(top_out_degree), V(g_costaff)$name)],
  out_degree = as.numeric(top_out_degree)
)

in_deg <- degree(g_costaff, mode = "in")
top_in_degree <- sort(in_deg, decreasing = TRUE)
top_in_degree <- data.frame(
  coach_id = names(top_in_degree),
  coach_name = V(g_costaff)$coach[match(names(top_in_degree), V(g_costaff)$name)],
  in_degree = as.numeric(top_in_degree)
)


V(g_costaff)$eigen <- eigen_centrality(g_costaff, directed = TRUE)$vector
V(g_costaff)$pagerank <- page_rank(g_costaff, directed = TRUE)$vector

page_rank  <- page_rank(g_costaff, directed = TRUE)$vector

top_pr <- sort(page_rank, decreasing = TRUE)

top_pr <- data.frame(
  coach = V(g_costaff)$coach[match(names(top_pr), V(g_costaff)$name)],
  pagerank = top_pr
)


diameter(g_costaff, directed = TRUE)
diam_path <- get_diameter(g_costaff, directed = TRUE)

diam_coaches <- data.frame(
  coach_id = as.double(names(diam_path)),
  coach_name = V(g_costaff)$coach[match(names(diam_path), V(g_costaff)$name)]
)


diam_coaches_full <- diam_coaches %>% 
  left_join(edges_df, by = c('coach_id' = 'from_coach_id'))%>% 
  left_join(nodes, by = c('to_coach_id' = 'coach_id'))


paste(diam_coaches$coach_name, collapse = " → ")
E(g_costaff, path = diam_path)


clust <- cluster_louvain(g_costaff)
V(g_costaff)$community <- membership(clust)

comm_id = 86

V(g_costaff)$coach[V(g_costaff)$community == comm_id]

V(g_costaff)$community[V(g_costaff)$coach == 'Kyle Shanahan']


