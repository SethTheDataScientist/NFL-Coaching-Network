import pandas as pd

# Load new base file
df = pd.read_csv("nfl_staff_updated_OC.csv")

# Keep only allowed role categories
allowed_roles = ["Head Coach", "Coordinator", "Position Coach - Defense", "Position Coach - Offense", "Specialist Coach"]
df = df[df["role_category"].isin(allowed_roles)].copy()

# Assign hierarchy
hierarchy_map = {
    "Head Coach": 1,
    "Coordinator": 2,
    "Position Coach - Defense": 3,
    "Position Coach - Offense": 3,
    "Specialist Coach": 4
}
df["hierarchy_level"] = df["role_category"].map(hierarchy_map)

# Create nodes table
nodes = df[["Name"]].drop_duplicates().reset_index(drop=True)
nodes = nodes.rename(columns={"Name": "coach"})
nodes["coach_id"] = nodes.index + 1

# Merge coach_id back
df = df.merge(nodes[["coach_id", 'coach']], left_on = 'Name', right_on="coach", how="left")

# Hierarchical edges WITH side-of-ball constraint
edges = []
for (team, year), g in df.groupby(["Team", "Year"]):
    for _, src in g.iterrows():
        for _, tgt in g.iterrows():
            if src["hierarchy_level"] < tgt["hierarchy_level"]:
                if src["role_category"] == "Head Coach" or src["side_of_ball"] == tgt["side_of_ball"]:
                    edges.append({
                        "from_coach_id": src["coach_id"],
                        "to_coach_id": tgt["coach_id"],
                        "team": team,
                        "year": year,
                        "from_role": src["role_category"],
                        "to_role": tgt["role_category"],
                        "from_side": src["side_of_ball"],
                        "to_side": tgt["side_of_ball"],
                        "edge_weight": 1,
                        "edge_type": "hierarchical"
                    })

edges_df = pd.DataFrame(edges)

# Co-staff edges
costaff_edges = []
for (team, year), g in df.groupby(["Team", "Year"]):
    for _, src in g.iterrows():
        for _, tgt in g.iterrows():
                    costaff_edges.append({
                        "from_coach_id": src["coach_id"],
                        "to_coach_id": tgt["coach_id"],
                        "team": team,
                        "year": year,
                        "from_role": src["role_category"],
                        "from_role_subcategory": src["role_subcategory"],
                        "from_position_group": src["position_group"],
                        "to_role": tgt["role_category"],
                        "to_role_subcategory": tgt["role_subcategory"],
                        "to_position_group": tgt["position_group"],
                        "from_side": src["side_of_ball"],
                        "to_side": tgt["side_of_ball"]
                    })

costaff_df = pd.DataFrame(costaff_edges)

# Save outputs
nodes.to_csv("nodes_v3.csv", index=False)
edges_df.to_csv("edges_df_v3.csv", index=False)
costaff_df.to_csv("costaff_df_v3.csv", index=False)

(nodes.shape, edges_df.shape, costaff_df.shape)
