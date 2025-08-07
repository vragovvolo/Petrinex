"""
Petrinex Well Production Forecasting App

A Streamlit application for visualizing ARPS decline curve forecasts for all fluids
"""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.subplots as sp
from pathlib import Path
from datetime import datetime, date

# Import conversion constants for imperial units
from src.petrinex.ngl_calcs import M3_TO_BBL, M3_TO_MCF


def convert_to_imperial_units(df: pd.DataFrame) -> pd.DataFrame:
    """Convert production data from metric (m³) to imperial units (Mcf, bbl)."""
    df_imperial = df.copy()

    # Convert gas production from m³ to Mcf
    if "GasProduction" in df_imperial.columns:
        df_imperial["GasProduction"] = df_imperial["GasProduction"] * M3_TO_MCF

    # Convert oil and condensate production from m³ to bbl
    for col in ["OilProduction", "CondensateProduction"]:
        if col in df_imperial.columns:
            df_imperial[col] = df_imperial[col] * M3_TO_BBL

    # Convert cumulative columns if they exist
    for col in [
        "GasProduction_Cumulative",
        "OilProduction_Cumulative",
        "CondensateProduction_Cumulative",
    ]:
        if col in df_imperial.columns:
            if "Gas" in col:
                df_imperial[col] = df_imperial[col] * M3_TO_MCF
            else:
                df_imperial[col] = df_imperial[col] * M3_TO_BBL

    return df_imperial


# Page configuration
st.set_page_config(page_title="Petrinex Forecasting", layout="wide")

# Color configuration for fluids
FLUID_COLORS = {
    "gas": "#FF0000",  # Red
    "oil": "#00AA00",  # Green
    "condensate": "#FF8000",  # Orange
}


@st.cache_data
def load_forecast_data():
    """Load forecast data from fixtures folder"""
    fixtures_path = Path("fixtures")

    data = {}
    # Load standard forecast files
    for prod_type in ["gas", "oil", "condensate"]:
        summary_file = fixtures_path / f"{prod_type}_forecast_summary.parquet"
        production_file = fixtures_path / f"{prod_type}_forecasted_production.parquet"

        if summary_file.exists() and production_file.exists():
            data[prod_type] = {
                "summary": pd.read_parquet(summary_file),
                "production": pd.read_parquet(production_file),
            }

    # Load historical data
    ngl_file = fixtures_path / "ngl_vol_bronze_cvx.parquet"
    if ngl_file.exists():
        data["historical"] = pd.read_parquet(ngl_file)

    return data


def extend_forecast_to_2030(well_forecast, prod_col, last_historical_date):
    """Extend forecast data to 2030 if it doesn't already go that far"""
    if len(well_forecast) == 0:
        return well_forecast

    target_date = pd.Timestamp("2030-12-01")
    max_forecast_date = well_forecast["ProductionMonth"].max()

    if max_forecast_date >= target_date:
        return well_forecast  # Already goes to 2030 or beyond

    # Get last known production rate and decline parameters
    last_rate = well_forecast[prod_col].iloc[-1]

    # Simple exponential decline extrapolation (conservative approach)
    # Assume 10% annual decline rate for extension
    monthly_decline = 0.1 / 12  # Convert annual to monthly

    # Create additional forecast points
    current_date = max_forecast_date
    extended_data = []
    current_rate = last_rate

    while current_date < target_date:
        current_date += pd.DateOffset(months=1)
        current_rate *= 1 - monthly_decline  # Apply decline
        extended_data.append(
            {
                "ProductionMonth": current_date,
                prod_col: max(current_rate, 0.01),  # Minimum floor to prevent negative
            }
        )

    if extended_data:
        extended_df = pd.DataFrame(extended_data)
        well_forecast = pd.concat([well_forecast, extended_df], ignore_index=True)

    return well_forecast


def create_separate_fluid_plots(well_id, data):
    """Create separate plots for gas vs oil/condensate with log-time rate plots"""

    # Get historical data for this well
    historical = data["historical"]
    well_historical = historical[historical["WellID"] == well_id].copy()
    well_historical["ProductionMonth"] = pd.to_datetime(
        well_historical["ProductionMonth"]
    )
    well_historical = well_historical.sort_values("ProductionMonth")

    # Convert to imperial units for display
    well_historical = convert_to_imperial_units(well_historical)

    # Production column mapping
    prod_columns = {
        "gas": "GasProduction",
        "oil": "OilProduction",
        "condensate": "CondensateProduction",
    }

    # Units for each fluid type
    units = {"gas": "Mcf", "oil": "bbl", "condensate": "bbl"}

    # Determine which liquids are available (oil or condensate, not both)
    available_liquids = []
    liquid_type = None
    for fluid_type in ["oil", "condensate"]:
        if fluid_type in data:
            summary_data = data[fluid_type]["summary"]
            if well_id in summary_data["WellID"].values:
                available_liquids.append(fluid_type)
                liquid_type = fluid_type
                break  # Take the first one found

    # Create subplots: 2 rows, 2 cols with individual chart titles
    fig = sp.make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Gas Production Rate (Log Scale)",
            "Gas Cumulative Production",
            f"{liquid_type.title() if liquid_type else 'Liquid'} Production Rate (Log Scale)",
            f"{liquid_type.title() if liquid_type else 'Liquid'} Cumulative Production",
        ),
        specs=[
            [{"secondary_y": False}, {"secondary_y": False}],
            [{"secondary_y": False}, {"secondary_y": False}],
        ],
        vertical_spacing=0.15,  # Increased vertical spacing between plots
    )

    # Get last historical date for forecast extension
    last_historical_date = (
        well_historical["ProductionMonth"].max()
        if len(well_historical) > 0
        else pd.Timestamp("2024-01-01")
    )

    # Process Gas (Row 1)
    fluid_type = "gas"
    if fluid_type in data:
        prod_col = prod_columns[fluid_type]
        color = FLUID_COLORS[fluid_type]

        # Ensure historical production data is numeric
        if len(well_historical) > 0 and prod_col in well_historical.columns:
            well_historical[prod_col] = pd.to_numeric(
                well_historical[prod_col], errors="coerce"
            ).fillna(0)

            # Only plot if there's meaningful production data
            if well_historical[prod_col].max() > 0:
                # Historical rate plot (log scale)
                fig.add_trace(
                    go.Scatter(
                        x=well_historical["ProductionMonth"],
                        y=well_historical[prod_col],
                        mode="lines+markers",
                        name=f"Gas Historical",
                        line=dict(color=color, width=2),
                        marker=dict(size=4),
                    ),
                    row=1,
                    col=1,
                )

                # Historical cumulative
                cumulative_col = f"{prod_col}_Cumulative"
                well_historical[cumulative_col] = well_historical[prod_col].cumsum()

                fig.add_trace(
                    go.Scatter(
                        x=well_historical["ProductionMonth"],
                        y=well_historical[cumulative_col],
                        mode="lines+markers",
                        name=f"Gas Historical Cum",
                        line=dict(color=color, width=2),
                        marker=dict(size=4),
                        showlegend=False,
                    ),
                    row=1,
                    col=2,
                )

        # Get gas forecast data
        forecast_data = data[fluid_type]["production"]
        well_forecast = forecast_data[forecast_data["WellID"] == well_id].copy()

        if len(well_forecast) > 0:
            well_forecast["ProductionMonth"] = pd.to_datetime(
                well_forecast["ProductionMonth"]
            )
            well_forecast = well_forecast.sort_values("ProductionMonth")

            # Ensure forecast production data is numeric
            well_forecast[prod_col] = pd.to_numeric(
                well_forecast[prod_col], errors="coerce"
            ).fillna(0)

            # Convert to imperial units for display
            well_forecast = convert_to_imperial_units(well_forecast)

            # Extend forecast to 2030
            well_forecast = extend_forecast_to_2030(
                well_forecast, prod_col, last_historical_date
            )

            # Forecast rate plot (log scale)
            fig.add_trace(
                go.Scatter(
                    x=well_forecast["ProductionMonth"],
                    y=well_forecast[prod_col],
                    mode="lines",
                    name=f"Gas Forecast",
                    line=dict(color=color, dash="dash", width=2),
                ),
                row=1,
                col=1,
            )

            # Forecast cumulative
            last_cum = 0
            if (
                len(well_historical) > 0
                and f"{prod_col}_Cumulative" in well_historical.columns
            ):
                last_cum = well_historical[f"{prod_col}_Cumulative"].iloc[-1]

            forecast_cumulative = last_cum + well_forecast[prod_col].cumsum()

            fig.add_trace(
                go.Scatter(
                    x=well_forecast["ProductionMonth"],
                    y=forecast_cumulative,
                    mode="lines",
                    name=f"Gas Forecast Cum",
                    line=dict(color=color, dash="dash", width=2),
                    showlegend=False,
                ),
                row=1,
                col=2,
            )

    # Process Oil/Condensate (Row 2)
    if liquid_type and liquid_type in data:
        prod_col = prod_columns[liquid_type]
        color = FLUID_COLORS[liquid_type]

        # Ensure historical production data is numeric
        if len(well_historical) > 0 and prod_col in well_historical.columns:
            well_historical[prod_col] = pd.to_numeric(
                well_historical[prod_col], errors="coerce"
            ).fillna(0)

            # Only plot if there's meaningful production data
            if well_historical[prod_col].max() > 0:
                # Historical rate plot (log scale)
                fig.add_trace(
                    go.Scatter(
                        x=well_historical["ProductionMonth"],
                        y=well_historical[prod_col],
                        mode="lines+markers",
                        name=f"{liquid_type.title()} Historical",
                        line=dict(color=color, width=2),
                        marker=dict(size=4),
                    ),
                    row=2,
                    col=1,
                )

                # Historical cumulative
                cumulative_col = f"{prod_col}_Cumulative"
                well_historical[cumulative_col] = well_historical[prod_col].cumsum()

                fig.add_trace(
                    go.Scatter(
                        x=well_historical["ProductionMonth"],
                        y=well_historical[cumulative_col],
                        mode="lines+markers",
                        name=f"{liquid_type.title()} Historical Cum",
                        line=dict(color=color, width=2),
                        marker=dict(size=4),
                        showlegend=False,
                    ),
                    row=2,
                    col=2,
                )

        # Get liquid forecast data
        forecast_data = data[liquid_type]["production"]
        well_forecast = forecast_data[forecast_data["WellID"] == well_id].copy()

        if len(well_forecast) > 0:
            well_forecast["ProductionMonth"] = pd.to_datetime(
                well_forecast["ProductionMonth"]
            )
            well_forecast = well_forecast.sort_values("ProductionMonth")

            # Ensure forecast production data is numeric
            well_forecast[prod_col] = pd.to_numeric(
                well_forecast[prod_col], errors="coerce"
            ).fillna(0)

            # Convert to imperial units for display
            well_forecast = convert_to_imperial_units(well_forecast)

            # Extend forecast to 2030
            well_forecast = extend_forecast_to_2030(
                well_forecast, prod_col, last_historical_date
            )

            # Forecast rate plot (log scale)
            fig.add_trace(
                go.Scatter(
                    x=well_forecast["ProductionMonth"],
                    y=well_forecast[prod_col],
                    mode="lines",
                    name=f"{liquid_type.title()} Forecast",
                    line=dict(color=color, dash="dash", width=2),
                ),
                row=2,
                col=1,
            )

            # Forecast cumulative
            last_cum = 0
            if (
                len(well_historical) > 0
                and f"{prod_col}_Cumulative" in well_historical.columns
            ):
                last_cum = well_historical[f"{prod_col}_Cumulative"].iloc[-1]

            forecast_cumulative = last_cum + well_forecast[prod_col].cumsum()

            fig.add_trace(
                go.Scatter(
                    x=well_forecast["ProductionMonth"],
                    y=forecast_cumulative,
                    mode="lines",
                    name=f"{liquid_type.title()} Forecast Cum",
                    line=dict(color=color, dash="dash", width=2),
                    showlegend=False,
                ),
                row=2,
                col=2,
            )

    # Update layout - remove main title since we have individual chart titles
    fig.update_layout(
        height=900,  # Increased height for better spacing
        showlegend=False,  # Legend will be moved to sidebar
    )

    # Update axes labels and set log scale for rate plots
    # Gas plots (Row 1)
    fig.update_xaxes(title_text="Date", row=1, col=1)
    fig.update_xaxes(title_text="Date", row=1, col=2)
    fig.update_yaxes(title_text="Gas Rate (Mcf/month)", type="log", row=1, col=1)
    fig.update_yaxes(title_text="Gas Cumulative (Mcf)", row=1, col=2)

    # Liquid plots (Row 2)
    if liquid_type:
        fig.update_xaxes(title_text="Date", row=2, col=1)
        fig.update_xaxes(title_text="Date", row=2, col=2)
        fig.update_yaxes(
            title_text=f"{liquid_type.title()} Rate ({units[liquid_type]}/month)",
            type="log",
            row=2,
            col=1,
        )
        fig.update_yaxes(
            title_text=f"{liquid_type.title()} Cumulative ({units[liquid_type]})",
            row=2,
            col=2,
        )

    return fig


def get_all_wells_from_data(data):
    """Get all unique wells from all available data sources"""
    all_wells = set()

    # Get wells from historical data
    if "historical" in data:
        historical_wells = set(data["historical"]["WellID"].unique())
        all_wells.update(historical_wells)

    # Get wells from forecast data
    for fluid_type in ["gas", "oil", "condensate"]:
        if fluid_type in data:
            forecast_wells = set(data[fluid_type]["summary"]["WellID"].unique())
            all_wells.update(forecast_wells)

    return sorted(list(all_wells))


def get_well_summary_info(well_id, data):
    """Get summary information for a well across all fluids"""
    well_info = {}

    for fluid_type in ["gas", "oil", "condensate"]:
        if fluid_type in data:
            summary_data = data[fluid_type]["summary"]
            well_data = summary_data[summary_data["WellID"] == well_id]
            if len(well_data) > 0:
                well_summary = well_data.iloc[0]
                well_info[fluid_type] = {
                    "r_squared": well_summary.get("RSquared", 0),
                    "curve_type": well_summary.get("CurveType", "Unknown"),
                    "qi": well_summary.get("qi", well_summary.get("Qi", None)),
                    "di": well_summary.get("di", well_summary.get("Di", None)),
                    "b": well_summary.get("b", well_summary.get("B", None)),
                }

    return well_info


def main():
    st.title("Petrinex Multi-Fluid Production Forecasting")
    st.markdown(
        "*ARPS decline curve analysis and production forecasting for Gas, Oil, and Condensate*"
    )

    # Load data
    with st.spinner("Loading forecast data..."):
        data = load_forecast_data()

    if not data:
        st.error(
            "No forecast data found in fixtures folder. Please run the forecasting pipeline first."
        )
        return

    # Get all available wells
    all_wells = get_all_wells_from_data(data)

    if not all_wells:
        st.error("No wells found in the data.")
        return

    # Create layout with filters in right column
    col1, col2 = st.columns([3, 1])

    # Initialize selected_well in right column first
    with col2:
        st.subheader("🔧 Filters & Well Information")

        # Filters section
        selected_well = st.selectbox(
            "Select Well ID", all_wells, help="Choose a well to analyze"
        )

        # Show available fluids for selected well
        available_fluids = []
        for fluid_type in ["gas", "oil", "condensate"]:
            if fluid_type in data:
                summary_data = data[fluid_type]["summary"]
                if selected_well in summary_data["WellID"].values:
                    available_fluids.append(fluid_type.title())

        if available_fluids:
            st.info(f"Available fluids: {', '.join(available_fluids)}")
        else:
            st.warning("No forecast data available for this well")

        st.divider()

        # Legend section (moved from plot info)
        st.subheader("📈 Chart Legend")
        st.markdown("🔴 **Gas** (Solid: Historical, Dashed: Forecast)")
        st.markdown("🟢 **Oil** (Solid: Historical, Dashed: Forecast)")
        st.markdown("🟠 **Condensate** (Solid: Historical, Dashed: Forecast)")

        st.divider()

        # Well Information section
        st.subheader("📊 Well Details")

        # Get well summary info for all fluids
        well_info = get_well_summary_info(selected_well, data)

        if well_info:
            for fluid_type, info in well_info.items():
                st.write(f"**{fluid_type.title()} Production:**")
                st.metric(f"R-Squared ({fluid_type})", f"{info['r_squared']:.3f}")
                st.text(f"Curve Type: {info['curve_type'].title()}")

                # ARPS Parameters
                if info["qi"] is not None:
                    st.text(f"qi: {info['qi']:.1f}")
                if info["di"] is not None:
                    st.text(f"di: {info['di']:.3f}")
                if info["b"] is not None and pd.notna(info["b"]):
                    st.text(f"b: {info['b']:.3f}")
                st.write("---")
        else:
            st.info("No forecast data available for this well")

    # Main plot in left column
    with col1:
        # Create and display plot
        fig = create_separate_fluid_plots(selected_well, data)
        st.plotly_chart(fig, use_container_width=True)


if __name__ == "__main__":
    main()
