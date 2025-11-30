# Imports
import streamlit as st
import geopandas as gpd
import polars as pl
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import pandas as pd

# Set page config for wide layout
st.set_page_config(layout="wide")

# Load data
@st.cache_data # This allows caching of the loaded data, to speed up app performance
def load_data():
    # Load in data files
    opensky_df = pl.read_parquet("opensky_enriched.parquet")
    airspace_gdf = gpd.read_parquet("airspace_enriched.parquet")
    transitions_df = pl.read_parquet("transitions.parquet")

    # Perform static filtering (Exclude 'Outside National Airspace')
        # Doing this inside the function means we cache the CLEANED result.
    opensky_df = opensky_df.filter((pl.col("IDENT") != "Outside National Airspace") & pl.col("on_ground") == False)
    airspace_gdf = airspace_gdf[airspace_gdf["IDENT"] != "Outside National Airspace"]
    transitions_df = transitions_df.filter(
        (pl.col("IDENT_prev") != "Outside National Airspace") & 
        (pl.col("IDENT_new") != "Outside National Airspace")
    )
    
    return opensky_df, airspace_gdf, transitions_df

# Call the function to get the cached data
opensky_df, airspace_gdf, transitions_df = load_data()

# Sidebar button selection for airspace
idents = sorted(airspace_gdf["IDENT"].unique().tolist())
st.sidebar.subheader("Select Airspace Region")
selected_ident = None
for ident in idents:
    if st.sidebar.button(ident, use_container_width=True):
        st.session_state.selected_ident = ident
        selected_ident = ident

if selected_ident is None and "selected_ident" in st.session_state:
    selected_ident = st.session_state.selected_ident
elif selected_ident is None:
    selected_ident = idents[0]
    st.session_state.selected_ident = selected_ident

# Filter transitions for this airspace
transitions_in_region = transitions_df.filter(
    (pl.col("IDENT_prev") == selected_ident) | (pl.col("IDENT_new") == selected_ident)
).sort("datetime")

# Get all related regions
related_idents = set(transitions_in_region.select(["IDENT_prev", "IDENT_new"]).to_numpy().flatten())
related_idents.add(selected_ident)

# Filter airspaces involved
airspaces_to_plot = airspace_gdf[airspace_gdf["IDENT"].isin(related_idents)]

# Filter opensky planes for these regions
opensky_df_in_region = opensky_df.filter(
    opensky_df["IDENT"].is_in(list(related_idents))
).with_columns(
    pl.from_epoch(pl.col("time_position"), time_unit="s").alias("datetime")
)

# Get most recent time and filter to last 5 minutes
most_recent_time = opensky_df_in_region.select(pl.col("time_position").max()).item()
cutoff_time = most_recent_time - 300  # 5 minutes prior

# Filter planes to last 5 minutes
recent_planes = opensky_df_in_region.filter(
    (pl.col("time_position") >= pl.lit(cutoff_time)) &
    (pl.col("time_position") <= pl.lit(most_recent_time))
)

# Filter transitions to last 5 minutes
transitions_cutoff = pl.from_epoch(pl.lit(cutoff_time), time_unit="s")
transitions_in_region = transitions_in_region.filter(
    pl.col("datetime") >= transitions_cutoff
)

# Filter entrances and exits for the selected airspace
entrances = transitions_in_region.filter(pl.col("IDENT_new") == selected_ident)
exits = transitions_in_region.filter(pl.col("IDENT_prev") == selected_ident)

# Get callsigns that entered/exited
entering_callsigns = set(entrances["callsign"].to_list())
exiting_callsigns = set(exits["callsign"].to_list())

# Only include regions with at least 1 transition
regions_with_transitions = set(transitions_in_region.select(["IDENT_prev", "IDENT_new"]).to_numpy().flatten())
regions_with_transitions.add(selected_ident)
airspaces_to_plot = airspaces_to_plot[airspaces_to_plot["IDENT"].isin(regions_with_transitions)]

# Filter planes to only those in the transition regions
recent_planes = recent_planes.filter(
    recent_planes["IDENT"].is_in(list(regions_with_transitions))
)

# Keep only most recent record per plane
recent_planes = recent_planes.sort("time_position", descending=True).group_by("icao24").head(1)
recent_planes_pd = recent_planes.to_pandas()

# Add transition status and assign colors
def get_transition_status(callsign):
    if callsign in entering_callsigns:
        return "Entered"
    elif callsign in exiting_callsigns:
        return "Exited"
    else:
        return "Neither"

recent_planes_pd["status"] = recent_planes_pd["callsign"].apply(get_transition_status)
status_to_color = {
    "Entered": "#00FF00",  # Bright green
    "Exited": "#FF4444",   # Bright red
    "Neither": "#CCCCCC"   # Light gray
}
recent_planes_pd["color"] = recent_planes_pd["status"].map(status_to_color)

# Create entrance dataframe
entrances_df = entrances.select(["callsign", "datetime", "IDENT_prev"]).to_pandas()
entrances_df.columns = ["Callsign", "Time of Entrance (UTC)", "Incoming Airspace"]
entrances_df = entrances_df.sort_values("Time of Entrance (UTC)", ascending=False).reset_index(drop=True)
entrances_df["Time of Entrance (UTC)"] = entrances_df["Time of Entrance (UTC)"].dt.strftime("%H:%M:%S")

# Create exit dataframe
exits_df = exits.select(["callsign", "datetime", "IDENT_new"]).to_pandas()
exits_df.columns = ["Callsign", "Time of Exit (UTC)", "Outgoing Airspace"]
exits_df = exits_df.sort_values("Time of Exit (UTC)", ascending=False).reset_index(drop=True)
exits_df["Time of Exit (UTC)"] = exits_df["Time of Exit (UTC)"].dt.strftime("%H:%M:%S")

# Title
st.title(f"Airspace Transitions: {selected_ident}")

# Top row: Tables side by side
col1, col2 = st.columns(2)

with col1:
    st.subheader("Entrances")
    st.dataframe(entrances_df, use_container_width=True, height=200, hide_index=True)

with col2:
    st.subheader("Exits")
    st.dataframe(exits_df, use_container_width=True, height=200, hide_index=True)

# Bottom row: Full-width map
st.subheader("Most Recent Aircraft Locations (Last 5 Minutes)")

# Display legend above the plot
col_legend = st.columns([0.2, 0.6, 0.2])
with col_legend[1]:
    legend_col1, legend_col2, legend_col3 = st.columns(3)
    with legend_col1:
        st.markdown("🟢 Entered")
    with legend_col2:
        st.markdown("🔴 Exited")
    with legend_col3:
        st.markdown("⚪ Neither")

# Set dark background style
plt.style.use('dark_background')

fig, ax = plt.subplots(
    figsize=(16, 10), 
    subplot_kw={"projection": ccrs.PlateCarree()},
    dpi=100,
    facecolor='#0e1117'
)
ax.set_facecolor('#0e1117')

# Plot all airspace polygons
for idx, row in airspaces_to_plot.iterrows():
    # Use darker blue for selected ident, darker gray for others
    if row['IDENT'] == selected_ident:
        facecolor = "#1E3A5F"  # Dark blue
        alpha = 0.3
    else:
        facecolor = "#2D2D2D"  # Dark gray
        alpha = 0.3
    
    ax.add_geometries(
        [row['geometry']], 
        crs=ccrs.PlateCarree(),
        facecolor=facecolor, 
        edgecolor="#FFFFFF", 
        linewidth=2,
        alpha=alpha
    )
    
    # Add separate geometry with full opacity to get white outline
    ax.add_geometries(
            [row['geometry']], 
            crs=ccrs.PlateCarree(),
            facecolor="none", 
            edgecolor="#FFFFFF", 
            linewidth=2
    
    )
    # Add airsapace labels at centroids
    centroid = row['geometry'].centroid
    ax.text(
        centroid.x, 
        centroid.y, 
        row['IDENT'], 
        ha='center', 
        va='center',
        fontsize=14, 
        fontweight='bold', 
        color='#FFFFFF', 
        transform=ccrs.PlateCarree()
    )

# Plot plane locations colored by transition status with directional arrows
arrow_length = 0.1 # Length of arrow in degrees

# Loop through planes and plot
for _, plane in recent_planes_pd.iterrows():
    lon, lat = plane['longitude'], plane['latitude']
    color = plane['color']
    true_track = plane['true_track']
    
    # Plot the point (lower alpha for Neither planes)
    if color == "#CCCCCC":
        alpha = 0.25
    else:
        alpha = 1.0

    ax.scatter(
        lon,
        lat,
        color=color,
        s=25,
        alpha=alpha,
        transform=ccrs.PlateCarree(),
        edgecolors='#FFFFFF',
        linewidth=0.5,
        zorder=5
    )
    
    # Add directional arrow if true_track is available
    if pd.notna(true_track):
        # Convert true track (degrees clockwise from north) to radians
        angle_rad = np.radians(90 - true_track)
        
        # Calculate arrow endpoint
        dx = arrow_length * np.cos(angle_rad)
        dy = arrow_length * np.sin(angle_rad)
        
        ax.arrow(
            lon, lat, dx, dy,
            head_width=0.125,
            head_length=0.1,
            fc=color,
            ec=color,
            alpha=alpha,
            transform=ccrs.PlateCarree(),
            zorder=6,
            linewidth=1
        )

# Add coastlines and state borders
ax.coastlines(linewidth=0.025, color="#FFFFFF")
ax.add_feature(cfeature.STATES, linewidth=0.05, edgecolor="#FFFFFF")

# Remove axis frame
ax.set_frame_on(False)

# Hide grid labels
ax.set_xticks([], crs=ccrs.PlateCarree())
ax.set_yticks([], crs=ccrs.PlateCarree())

# Add plot to streamlit
st.pyplot(fig, use_container_width=True)