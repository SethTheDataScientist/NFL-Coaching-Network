import streamlit as st
import pandas as pd
import networkx as nx
from pyvis.network import Network
import streamlit.components.v1 as components
from io import StringIO

# Set page config
st.set_page_config(page_title="NFL Coaching Network Analysis", layout="wide")

st.title("NFL Coaching Network Analysis (1980-2025)")

# Load all data files
nodes_df = pd.read_csv("Network Vis/nodes_costaff.csv")
edges_df = pd.read_csv("Network Vis/edges_df.csv")
edges_df_full = edges_df.copy()  # Keep a copy of the full edges for connection tables
centrality_df = pd.read_csv("Network Vis/centrality_measures.csv")
community_summary_df = pd.read_csv("Network Vis/community_summary.csv")
avg_downstream_by_year_df = pd.read_csv("Network Vis/avg_downstream_by_year.csv")
avg_downstream_overall_df = pd.read_csv("Network Vis/avg_downstream_overall.csv")
influence_scores_df = pd.read_csv("Network Vis/influence_scores.csv")

# Create tabs for different sections
tab1, tab3 = st.tabs(["Network Visualization", "Coaching Tree Analysis"])

# Tab 1: Network Visualization
with tab1:
    
    # Network visualization settings
    st.sidebar.header("Visualization Settings")
    height = st.sidebar.slider("Graph Height (px)", 400, 1000, 600)
    physics = st.sidebar.checkbox("Enable Physics", value=True)
    
    # Advanced physics controls
    with st.sidebar.expander("Advanced Layout Settings"):
        spring_length = st.slider(
            "Node Spacing",
            min_value=50,
            max_value=500,
            value=250,
            step=25,
            help="Higher = more space between nodes"
        )
        spring_strength = st.slider(
            "Spring Strength", 
            min_value=0.001,
            max_value=0.1,
            value=0.001,
            step=0.005,
            format="%.3f",
            help="Lower = less springy/bouncy"
        )
        repulsion = st.slider(
            "Repulsion Force",
            min_value=-20000,
            max_value=-1000,
            value=-18000,
            step=1000,
            help="More negative = nodes push apart more"
        )
    
    # Detect column names for edges (handle different formats)
    if 'from' in edges_df.columns and 'to' in edges_df.columns:
        source_col, target_col = 'from', 'to'
    elif 'source' in edges_df.columns and 'target' in edges_df.columns:
        source_col, target_col = 'source', 'target'
    else:
        st.error("Edges file must have either 'from'/'to' or 'source'/'target' columns")
        st.stop()
    
    # Detect column names for nodes (handle different formats)
    if 'coach_id' in nodes_df.columns:
        id_col = 'coach_id'
        label_col = 'coach' if 'coach' in nodes_df.columns else 'coach_id'
    elif 'id' in nodes_df.columns:
        id_col = 'id'
        label_col = 'label' if 'label' in nodes_df.columns else 'id'
    else:
        st.error("Nodes file must have either 'id' or 'coach_id' column")
        st.stop()
    
    # Node selection for filtering - MOVED TO TOP
    st.subheader("Select Node (Coach) or Louvain Community to Visualize")
    
    # Save original nodes_df before any filtering
    nodes_df_original = nodes_df.copy()
    
    # Add community filter option
    filter_mode = st.radio(
        "Filter by:",
        options=["Individual Node", "Community"],
        horizontal=True,
        help="Choose to view a single node's connections or an entire community"
    )
    
    if filter_mode == "Community":
        # Community selection
        if 'community' in nodes_df.columns:
            # Get unique communities (excluding NA)
            communities = nodes_df['community'].dropna().unique()
            communities = sorted([c for c in communities if str(c) != 'NA'])
            
            if len(communities) > 0:
                selected_community = st.selectbox(
                    "Select a community:",
                    options=communities,
                    help="Select a community to visualize all nodes within it"
                )
                
                # Get all nodes in the selected community
                community_nodes = nodes_df[nodes_df['community'] == selected_community][id_col].tolist()
                selected_node = None  # We'll use community_nodes instead
            else:
                st.warning("No valid communities found in the data")
                filter_mode = "Individual Node"  # Fallback
        else:
            st.warning("No 'community' column found in nodes data")
            filter_mode = "Individual Node"  # Fallback
    
    if filter_mode == "Individual Node":
        # Create searchable node list
        node_options = nodes_df[id_col].tolist()
        if label_col in nodes_df.columns:
            node_labels = [f"{row[label_col]} (ID: {row[id_col]})" for _, row in nodes_df.iterrows()]
            node_lookup = {label: node_id for label, node_id in zip(node_labels, node_options)}
        else:
            node_labels = [str(x) for x in node_options]
            node_lookup = {str(x): x for x in node_options}
        
        selected_label = st.selectbox(
            "Search and select a node:",
            options=node_labels,
            help="Select a node to visualize its incoming and outgoing connections"
        )
        selected_node = node_lookup[selected_label]
        community_nodes = None  # Not using community mode
    
    # Optional: Add connection depth (only for individual node mode)
    if filter_mode == "Individual Node":
        connection_depth = st.sidebar.radio(
            "Connection Depth",
            options=[1, 2],
            index=0,
            help="1 = Direct connections only, 2 = Include connections of connections"
        )
    else:
        connection_depth = None  # Not applicable for community mode
    
    # Optional filters for the selected node's connections
    st.sidebar.header("Filter Connections")
    
    # Create full graph first to get connections
    G_full = nx.from_pandas_edgelist(edges_df, source_col, target_col, 
                                      edge_attr=True, create_using=nx.DiGraph())
    
    # Add node attributes to G_full (important for table lookups)
    node_attrs_full = {}
    for _, row in nodes_df_original.iterrows():
        node_id = row[id_col]
        attrs = row.to_dict()
        attrs['_id'] = node_id
        attrs['_label'] = row[label_col]
        node_attrs_full[node_id] = attrs
    
    nx.set_node_attributes(G_full, node_attrs_full)
    
    # Initialize filter variables
    selected_years = None
    selected_teams = None
    
    # Filter edges based on mode (individual node or community)
    if filter_mode == "Individual Node" and selected_node in G_full.nodes():
        # Get nodes within specified depth
        if connection_depth == 1:
            # Direct connections only
            connected_nodes = set([selected_node])
            connected_nodes.update(G_full.successors(selected_node))  # Outgoing
            connected_nodes.update(G_full.predecessors(selected_node))  # Incoming
        else:
            # 2-hop connections
            connected_nodes = set([selected_node])
            # First hop
            first_hop = set(G_full.successors(selected_node)) | set(G_full.predecessors(selected_node))
            connected_nodes.update(first_hop)
            # Second hop
            for node in first_hop:
                connected_nodes.update(G_full.successors(node))
                connected_nodes.update(G_full.predecessors(node))
    
    elif filter_mode == "Community" and community_nodes:
        # For community mode, show all nodes in the community
        connected_nodes = set(community_nodes)
        
        # Optionally include connections to/from community nodes
        include_external = st.sidebar.checkbox(
            "Include External Connections",
            value=False,
            help="Show connections to nodes outside the community"
        )
        
        if include_external:
            # Add all nodes connected to community nodes
            for node in community_nodes:
                if node in G_full.nodes():
                    connected_nodes.update(G_full.successors(node))
                    connected_nodes.update(G_full.predecessors(node))
    
    else:
        # Fallback: show all nodes
        connected_nodes = set(G_full.nodes())
        if filter_mode == "Individual Node":
            st.warning(f"Selected node {selected_node} not found in the network. Showing full network.")
    
    # Continue with existing filtering logic
    # Filter edges to only those connecting the selected nodes
    filtered_edges = edges_df[
        (edges_df[source_col].isin(connected_nodes)) & 
        (edges_df[target_col].isin(connected_nodes))
    ]
    
    # Additional filters if available
    if 'year' in filtered_edges.columns:
            years = sorted(filtered_edges['year'].dropna().unique())
            if len(years) > 0:
                selected_years = st.sidebar.multiselect(
                    "Filter by Year",
                    options=years,
                    default=years,
                    help="Filter connections by specific years"
                )
                if selected_years:
                    filtered_edges = filtered_edges[filtered_edges['year'].isin(selected_years)]
        
    if 'team' in filtered_edges.columns:
        teams = sorted(filtered_edges['team'].dropna().unique())
        if len(teams) > 0:
            selected_teams = st.sidebar.multiselect(
                "Filter by Team",
                options=teams,
                default=teams,
                help="Filter connections by specific teams"
            )
            if selected_teams:
                filtered_edges = filtered_edges[filtered_edges['team'].isin(selected_teams)]
        
    # Use filtered edges
    edges_df = filtered_edges
    
    # Filter nodes to only those in the filtered edges
    nodes_in_edges = set(edges_df[source_col]) | set(edges_df[target_col])
    nodes_df = nodes_df[nodes_df[id_col].isin(nodes_in_edges)]
    
    # Display metrics
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Nodes in View", len(nodes_df))
    with col2:
        st.metric("Edges in View", len(edges_df))
    with col3:
        if filter_mode == "Individual Node":
            st.metric("Connection Depth", connection_depth)
        else:
            st.metric("Community", selected_community if 'selected_community' in locals() else "N/A")
    
    # Create NetworkX graph
    G = nx.from_pandas_edgelist(edges_df, source_col, target_col, 
                                 edge_attr=True, create_using=nx.DiGraph())
    
    # Add node attributes
    node_attrs = {}
    for _, row in nodes_df.iterrows():
        node_id = row[id_col]
        attrs = row.to_dict()
        attrs['_id'] = node_id  # Store original ID
        attrs['_label'] = row[label_col]  # Store label
        node_attrs[node_id] = attrs
    
    nx.set_node_attributes(G, node_attrs)
    
    # Create Pyvis network
    net = Network(height=f"{height}px", width="100%", directed=True, 
                  notebook=False, bgcolor="#ffffff", font_color="black")
    
    # Don't use default barnes_hut, we'll configure it manually
    if not physics:
        net.toggle_physics(False)
    
    # Add nodes with styling
    for node in G.nodes():
        node_data = G.nodes[node]
        
        # Build hover info from all attributes
        title = " ".join([f"{k}: {v}" for k, v in node_data.items() 
                            if not k.startswith('_')])
        
        # Get label from stored attribute
        label = node_data.get('_label', str(node))
        
        # Determine node styling based on filter mode
        if filter_mode == "Individual Node" and node == selected_node:
            # Highlight selected node in individual mode
            size = 30
            color = '#ff0000'  # Red for selected node
            border_width = 4
        elif filter_mode == "Community":
            # In community mode, use community as group for coloring
            size = node_data.get('value', 15)
            color = None
            border_width = 2
        else:
            size = node_data.get('value', 15)
            color = None
            border_width = 2
        
        # Get group for coloring
        if 'community' in node_data and filter_mode == "Community":
            group = node_data.get('community', 0)
        else:
            group = node_data.get('group', node_data.get('team', node_data.get('name', 0)))
        
        if color:
            net.add_node(node, label=str(label), title=title, size=size, 
                        color=color, borderWidth=border_width)
        else:
            net.add_node(node, label=str(label), title=title, size=size, 
                        group=str(group), borderWidth=border_width)
    
    # Add edges with styling
    for edge in G.edges(data=True):
        source, target, data = edge
        
        # Get weight (try different possible column names)
        weight = data.get('weight', data.get('closeness', data.get('hierarchy', 1)))
        
        # Build edge hover info
        edge_info = [f"{k}: {v}" for k, v in data.items() if k not in ['weight', 'closeness', 'hierarchy']]
        title = f"Weight: {weight} " + " ".join(edge_info[:5])  # Limit to 5 attributes
        
        net.add_edge(source, target, value=float(weight) if weight else 1, title=title)
    
    # Set options for interactivity
    net.set_options(f"""
    {{
      "nodes": {{
        "borderWidth": 2,
        "borderWidthSelected": 4,
        "font": {{
          "size": 18,
          "face": "arial",
          "bold": {{
            "color": "#000000"
          }},
          "background": "rgba(255, 255, 255, 0.9)",
          "strokeWidth": 0
        }},
        "scaling": {{
          "label": {{
            "enabled": true,
            "min": 14,
            "max": 24
          }}
        }}
      }},
      "edges": {{
        "arrows": {{
          "to": {{
            "enabled": true,
            "scaleFactor": 0.3
          }}
        }},
        "width": 1,
        "smooth": {{
          "type": "continuous"
        }},
        "length": {spring_length}
      }},
      "interaction": {{
        "hover": true,
        "navigationButtons": true,
        "keyboard": true
      }},
      "physics": {{
        "enabled": {str(physics).lower()},
        "barnesHut": {{
          "gravitationalConstant": {repulsion},
          "centralGravity": 0.1,
          "springLength": {spring_length},
          "springConstant": {spring_strength},
          "damping": 0.5,
          "avoidOverlap": 0.5
        }},
        "stabilization": {{
          "enabled": true,
          "iterations": 1000,
          "updateInterval": 25
        }}
      }}
    }}
    """)
    
    # Save and read the network HTML
    import os
    import tempfile
    
    # Create a temporary file in a directory that exists
    temp_dir = tempfile.gettempdir()
    html_path = os.path.join(temp_dir, "network.html")
    net.save_graph(html_path)
    
    # Read the HTML file and add click event handling
    with open(html_path, 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Add JavaScript for node click handling
    click_script = """
    <script type="text/javascript">
        network.on("click", function(params) {
            if (params.nodes.length > 0) {
                var nodeId = params.nodes[0];
                // Send node ID to Streamlit
                window.parent.postMessage({
                    type: 'streamlit:setComponentValue',
                    value: nodeId
                }, '*');
            }
        });
    </script>
    """
    
    html_content = html_content.replace('</body>', click_script + '</body>')
    
    # Display the network
    if filter_mode == "Individual Node":
        st.subheader("Network Graph - Connections for Selected Node")
    else:
        st.subheader(f"Network Graph - Community {selected_community if 'selected_community' in locals() else 'View'}")
    components.html(html_content, height=height+50)
    
    # Node details and connections table
    st.subheader("Connection Details")
    
    # Only show individual node connection details in Individual Node mode
    if filter_mode == "Individual Node" and selected_node in G_full.nodes():
        # Get connections from the ORIGINAL edges dataframe (not the graph)
        # This preserves all team-year combinations
        
        # Detect column names again for the full edges df
        if 'from' in edges_df_full.columns and 'to' in edges_df_full.columns:
            source_col_full, target_col_full = 'from', 'to'
        elif 'source' in edges_df_full.columns and 'target' in edges_df_full.columns:
            source_col_full, target_col_full = 'source', 'target'
        else:
            source_col_full, target_col_full = source_col, target_col
        
        # Get all outgoing edges (all rows where source is the selected node)
        out_edges_filtered = edges_df_full[edges_df_full[source_col_full] == selected_node].copy()
        
        # Get all incoming edges (all rows where target is the selected node)
        in_edges_filtered = edges_df_full[edges_df_full[target_col_full] == selected_node].copy()
        
        # Filter based on year/team if applied
        if 'year' in out_edges_filtered.columns and selected_years:
            out_edges_filtered = out_edges_filtered[out_edges_filtered['year'].isin(selected_years)]
            in_edges_filtered = in_edges_filtered[in_edges_filtered['year'].isin(selected_years)]
        
        if 'team' in out_edges_filtered.columns and selected_teams:
            out_edges_filtered = out_edges_filtered[out_edges_filtered['team'].isin(selected_teams)]
            in_edges_filtered = in_edges_filtered[in_edges_filtered['team'].isin(selected_teams)]
        
        # Display node information
        st.write("**Node Information:**")
        node_info = G_full.nodes[selected_node]
        info_df = pd.DataFrame([node_info])
        info_df = info_df.drop(columns=['_id', '_label'], errors='ignore')
        st.dataframe(info_df, use_container_width=True)
        
        # Create connection tables
        col1, col2 = st.columns(2)
        
        with col1:
            st.write(f"**Outgoing Connections ({len(out_edges_filtered)}):**")
            if len(out_edges_filtered) > 0:
                # Build display dataframe with labels
                out_display = out_edges_filtered.copy()
                out_display['Target_Label'] = out_display[target_col].map(
                    lambda x: G_full.nodes[x].get('_label', x) if x in G_full.nodes else x
                )
                # Reorder columns to show label first
                display_cols = ['Target_Label', target_col]
                for col in out_display.columns:
                    if col not in display_cols and col != source_col:
                        display_cols.append(col)
                out_display = out_display[display_cols]
                out_display = out_display.rename(columns={'Target_Label': 'Label', target_col: 'Target ID'})
                out_display = out_display.drop(columns=['edge_weight', 'edge_type'], errors='ignore')
                out_display['year'] = out_display['year'].astype(str)
                out_display = out_display[['Target ID', 'Label', 'year'] + [col for col in out_display.columns if col not in ['Target ID', 'Label', 'year']]]
                out_display = out_display.sort_values(by=['year'], ascending=False)
                st.dataframe(out_display, use_container_width=True)
            else:
                st.info("No outgoing connections")
        
        with col2:
            st.write(f"**Incoming Connections ({len(in_edges_filtered)}):**")
            if len(in_edges_filtered) > 0:
                # Build display dataframe with labels
                in_display = in_edges_filtered.copy()
                in_display['Source_Label'] = in_display[source_col].map(
                    lambda x: G_full.nodes[x].get('_label', x) if x in G_full.nodes else x
                )
                # Reorder columns to show label first
                display_cols = ['Source_Label', source_col]
                for col in in_display.columns:
                    if col not in display_cols and col != target_col:
                        display_cols.append(col)
                in_display = in_display[display_cols]
                in_display = in_display.rename(columns={'Source_Label': 'Label', source_col: 'Source ID'})
                in_display = in_display.drop(columns=['edge_weight', 'edge_type'], errors='ignore')
                in_display['year'] = in_display['year'].astype(str)
                in_display = in_display[['Source ID', 'Label', 'year'] + [col for col in in_display.columns if col not in ['Source ID', 'Label', 'year']]]
                in_display = in_display.sort_values(by=['year'], ascending=False)
                st.dataframe(in_display, use_container_width=True)
            else:
                st.info("No incoming connections")
        
        # Combined connection table
        st.write("**All Connections (Combined):**")
        
        # Combine outgoing and incoming connections
        combined_out = out_edges_filtered.copy()
        combined_out['Direction'] = 'Outgoing'
        combined_out['Connected_Node_ID'] = combined_out[target_col_full]
        combined_out['Label'] = combined_out[target_col_full].map(
            lambda x: G_full.nodes[x].get('_label', x) if x in G_full.nodes else x
        )
        
        combined_in = in_edges_filtered.copy()
        combined_in['Direction'] = 'Incoming'
        combined_in['Connected_Node_ID'] = combined_in[source_col_full]
        combined_in['Label'] = combined_in[source_col_full].map(
            lambda x: G_full.nodes[x].get('_label', x) if x in G_full.nodes else x
        )
        
        # Concatenate and select relevant columns
        all_connections = pd.concat([combined_out, combined_in], ignore_index=True)
        
        # Select and reorder columns
        display_cols = ['Direction', 'Label', 'Connected_Node_ID']
        for col in all_connections.columns:
            if col not in display_cols and col not in [source_col_full, target_col_full]:
                display_cols.append(col)
        
        all_connections = all_connections[display_cols]
        all_connections = all_connections.rename(columns={'Connected_Node_ID': 'Node ID'})
        
        all_connections = all_connections.drop(columns=['edge_weight', 'edge_type'], errors='ignore')
        all_connections['year'] = all_connections['year'].astype(str)
        all_connections = all_connections[['Direction', 'Node ID', 'Label', 'year'] + [col for col in all_connections.columns if col not in ['Direction', 'Node ID', 'Label', 'year']]]
        all_connections = all_connections.sort_values(by=['Direction', 'year'], ascending=[True, False])
        st.dataframe(all_connections, use_container_width=True)
    
    elif filter_mode == "Community":
        
        # Add community summary statistics from loaded CSV
        st.write("**Community Statistics:**")
        st.write('_More information about how promotion value is calculated can be found in the Coaching Tree Analysis tab._')
        if 'selected_community' in locals() and selected_community in community_summary_df['community'].values:
            comm_stats = community_summary_df[community_summary_df['community'] == selected_community].iloc[0]
            
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Edges", f"{int(comm_stats['Count']):,}")
            with col2:
                st.metric("Total Promotion Value", f"{comm_stats['Total_oe_promotions']:.1f}")
            with col3:
                st.metric("Mean Promotion Value", f"{comm_stats['Mean_oe_promotions']:.4f}")
        
        # Show nodes in the community
        st.write("**Nodes in Community:**")
        community_details = nodes_df[[id_col, label_col]].copy()
        community_details.columns = ['ID', 'Name']
        st.dataframe(community_details, use_container_width=True)


# Tab 3: Downstream Analysis
with tab3:
    st.markdown("""
#### 🏈 Coaching Tree Impact Analysis

This analysis evaluates the **impact of each coach’s coaching tree** by measuring how far their mentees advance after working with them.

The value is calculated by assigning a weight to each role in the coaching hierarchy, with higher roles receiving higher weights. The total weighted value of all promotions achieved by a coach's reports is then compared to their current role's value. So if a coach was a Position Coach (weight 0.4) and eventually became a Head Coach (weight 1.0), that would contribute 0.6 (1.0 - 0.4) to the coach's future coaching value.

##### 📊 Role Weighting System

Each role is assigned a value according to its level of responsibility:

| Role | Weight |
|------|--------|
| **Head Coach** | **1.0** |
| Offensive / Defensive Coordinator | 0.8 |
| Special Teams Coordinator | 0.6 |
| Specialist Coordinator (Passing Game / Run Game) | 0.6 |
| Position Coach (Offense / Defense) | 0.4 |
| Position Coach (Special Teams) | 0.3 |
| Specialist Coach (Assistant, Quality Control, etc.) | 0.2 |
| Specialist Coach (Special Teams) | 0.1 |

""")

    
    # Create sub-tabs for the different views
    subtab1, subtab2, subtab3 = st.tabs(["Influence Scores", "By Year", "Overall Summary"])
    
    with subtab1:
        st.subheader("PageRank Influence Scores")
        
        st.markdown("""
        ### About This Metric
        
        The **Combined Influence Score** is calculated using a weighted PageRank algorithm that measures how influential 
        each coach has been in developing future NFL coaching talent. 
        
        **How it works:**
        - Each connection between coaches (edges in the network) is weighted by the **total promotion value compared with current roles**
        - PageRank then flows this "influence" through the network, similar to how Google ranks web pages
        - Coaches who developed many successful future coaches receive higher scores
        - The scores also consider the quality of connections: being connected to other influential coaches increases your score
        
        **What this tells us:**
        A high influence score indicates that a coach has been particularly effective at:
        1. Developing assistant coaches who went on to successful careers
        2. Creating a "coaching tree" with multiple branches of successful coaches
        3. Being part of influential coaching networks that produced NFL leadership
        
        Think of this as the "coaching tree impact score" - it identifies coaches who have had the greatest 
        ripple effect on the development of NFL coaching talent.
        """)
        
        # Filter data
        filtered_influence = influence_scores_df.copy()
        filtered_influence = filtered_influence[['coach_id', 'coach', 'last_year'] + [col for col in filtered_influence.columns if col not in ['coach_id', 'coach', 'last_year']]]
        filtered_influence['last_year'] = filtered_influence['last_year'].astype(str)
        # Sort by influence score descending
        filtered_influence = filtered_influence.sort_values('Combined_influence_score', ascending=False)
        
        st.dataframe(filtered_influence, use_container_width=True)
        
        # Summary statistics
        st.subheader("Summary Statistics")
        col1, col2, col3= st.columns(3)
        with col1:
            st.metric("Total Coaches", f"{len(filtered_influence):,}")
        with col2:
            st.metric("Average Influence Score", f"{filtered_influence['Combined_influence_score'].mean():.4f}")
        with col3:
            st.metric("Median Influence Score", f"{filtered_influence['Combined_influence_score'].median():.4f}")
        
    
    with subtab2:
        st.subheader("Future Coaching Talent Accumulated by Year")
        st.write("This is a measure of each team's future coaching promotion value accumulated by year, based on the coaches they employed.")
        
        
        
        # Filter data
        filtered_by_year = avg_downstream_by_year_df.copy()
        filtered_by_year['year'] = filtered_by_year['year'].astype(str)
        filtered_by_year = filtered_by_year[['from_coach_id', 'community', 'coach', 'team', 'year', 'num_reports', 'median_oe_future_value', 'total_oe_future_value']]
        # Sort by total_oe_future_value descending
        filtered_by_year = filtered_by_year.sort_values('total_oe_future_value', ascending=False)
        # rename oe columns to promotion value
        filtered_by_year = filtered_by_year.rename(columns={'median_oe_future_value': 'Median Future Promotion Value', 'total_oe_future_value': 'Total Future Promotion Value'})

        st.dataframe(filtered_by_year, use_container_width=True)
        
        # Summary statistics
        st.subheader("Summary Statistics")
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Median Future Promotion Value", f"{filtered_by_year['Median Future Promotion Value'].mean():.3f}")
        with col2:
            st.metric("Total Reports", f"{filtered_by_year['num_reports'].sum():,}")
        with col3:
            st.metric("Unique Coaches", f"{filtered_by_year['from_coach_id'].nunique():,}")
    
    with subtab3:
        st.subheader("Future Coaching Talent Accumulated - Overall Career")
        st.write("Aggregated measure of each coach's total future coaching promotion value accumulated by their mentees over their entire career.")
        
        
        # Filter data
        filtered_overall = avg_downstream_overall_df.copy()
        filtered_overall = filtered_overall[['from_coach_id', 'community', 'coach', 'last_team', 'last_year', 'years_active', 'total_reports',
                                              'median_oe_future_value', 'total_oe_future_value', 'total_value_by_year']]
        filtered_overall['last_year'] = filtered_overall['last_year'].astype(str)
        # Sort by total_oe_future_value descending
        filtered_overall = filtered_overall.sort_values('total_oe_future_value', ascending=False)
        # rename oe columns to promotion value
        filtered_overall = filtered_overall.rename(columns={'median_oe_future_value': 'Median Future Promotion Value', 'total_oe_future_value': 'Total Future Promotion Value'})
        st.dataframe(filtered_overall, use_container_width=True)
        
        # Summary statistics
        st.subheader("Summary Statistics")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Median Future Promotion Value", f"{filtered_overall['Median Future Promotion Value'].mean():.3f}")
        with col2:
            st.metric("Total Reports (All Time)", f"{filtered_overall['total_reports'].sum():,}")
        with col3:
            st.metric("Unique Coaches", f"{filtered_overall['from_coach_id'].nunique():,}")
        with col4:
            st.metric("Avg Years Active", f"{filtered_overall['years_active'].mean():.1f}")
        
        

