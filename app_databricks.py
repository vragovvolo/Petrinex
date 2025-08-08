import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config


# Configuration for Databricks connection
cfg = Config()  # Set the DATABRICKS_HOST environment variable when running locally


@st.cache_resource  # connection is cached
def get_connection(http_path):
    """Create a cached connection to Databricks SQL warehouse."""
    return sql.connect(
        server_hostname=cfg.host,
        http_path=http_path,
        credentials_provider=lambda: cfg.authenticate,
    )


def read_table(table_name, conn):
    """Read data from a Unity Catalog table."""
    with conn.cursor() as cursor:
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        return cursor.fetchall_arrow().to_pandas()


def get_table_count(table_name, conn):
    """Get row count for a table."""
    try:
        with conn.cursor() as cursor:
            query = f"SELECT COUNT(*) as row_count FROM {table_name}"
            cursor.execute(query)
            result = cursor.fetchone()
            return result[0] if result else 0
    except Exception as e:
        st.error(f"Error getting count for {table_name}: {e}")
        return 0


def main():
    st.set_page_config(page_title="Petrinex Forecast Dashboard", layout="wide")

    st.title("🛢️ Petrinex Production Forecast Dashboard")
    st.markdown("---")

    # Configuration section
    st.sidebar.header("⚙️ Configuration")

    # HTTP path input with default
    http_path_input = st.sidebar.text_input(
        "Databricks SQL Warehouse HTTP Path:",
        placeholder="/sql/1.0/warehouses/xxxxxx",
        help="Enter the HTTP path for your Databricks SQL warehouse",
    )

    # Catalog and schema configuration
    catalog = st.sidebar.text_input("Catalog", value="shm", help="Unity Catalog name")
    schema = st.sidebar.text_input("Schema", value="petrinex", help="Schema name")

    if not http_path_input:
        st.warning(
            "Please provide the SQL warehouse HTTP path in the sidebar to continue."
        )
        return

    try:
        # Get connection
        conn = get_connection(http_path_input)
        st.sidebar.success("✅ Connected to Databricks")

        # Define available tables
        table_options = {
            "📊 Forecast Summary Tables": {
                "Oil Production Summary": f"{catalog}.{schema}.oilproduction_forecast_summary",
                "Gas Production Summary": f"{catalog}.{schema}.gasproduction_forecast_summary",
                "Condensate Production Summary": f"{catalog}.{schema}.condensateproduction_forecast_summary",
            },
            "📈 Forecasted Production Tables": {
                "Oil Production Forecast": f"{catalog}.{schema}.oilproduction_forecasted_production",
                "Gas Production Forecast": f"{catalog}.{schema}.gasproduction_forecasted_production",
                "Condensate Production Forecast": f"{catalog}.{schema}.condensateproduction_forecasted_production",
            },
            "📋 Combined Historical + Forecast Tables": {
                "Oil Historical + Forecast": f"{catalog}.{schema}.oilproduction_historical_forecast_combined",
                "Gas Historical + Forecast": f"{catalog}.{schema}.gasproduction_historical_forecast_combined",
                "Condensate Historical + Forecast": f"{catalog}.{schema}.condensateproduction_historical_forecast_combined",
            },
            "🗃️ Source Data Tables": {
                "NGL Bronze": f"{catalog}.{schema}.ngl_bronze",
                "NGL Silver (base)": f"{catalog}.{schema}.ngl_silver",
                "NGL Calendar (monthly + cumulative)": f"{catalog}.{schema}.ngl_calendar",
                "NGL Normalized (730-hour periods)": f"{catalog}.{schema}.ngl_normalized",
            },
        }

        # Main content area
        st.header("📊 Available Tables")

        # Create tabs for different table categories
        tab_names = list(table_options.keys())
        tabs = st.tabs(tab_names)

        for tab, (category, tables) in zip(tabs, table_options.items()):
            with tab:
                st.subheader(category)

                # Create columns for better layout
                cols = st.columns(2)

                for i, (display_name, table_name) in enumerate(tables.items()):
                    col = cols[i % 2]

                    with col:
                        with st.expander(f"📁 {display_name}", expanded=False):
                            # Show table info
                            row_count = get_table_count(table_name, conn)
                            st.metric("Rows", f"{row_count:,}")
                            st.code(table_name, language="sql")

                            # Button to load and display table
                            if st.button(
                                f"Load {display_name}", key=f"load_{table_name}"
                            ):
                                with st.spinner(f"Loading {display_name}..."):
                                    try:
                                        df = read_table(table_name, conn)

                                        st.success(f"✅ Loaded {len(df):,} rows")

                                        # Show data info
                                        st.write("**Table Info:**")
                                        info_cols = st.columns(3)
                                        with info_cols[0]:
                                            st.metric("Rows", len(df))
                                        with info_cols[1]:
                                            st.metric("Columns", len(df.columns))
                                        with info_cols[2]:
                                            if "WellID" in df.columns:
                                                unique_wells = df["WellID"].nunique()
                                                st.metric("Unique Wells", unique_wells)

                                        # Display column info
                                        st.write("**Columns:**")
                                        st.write(", ".join(df.columns))

                                        # Display the dataframe
                                        st.dataframe(
                                            df, use_container_width=True, height=400
                                        )

                                        # Show basic statistics for numeric columns
                                        numeric_cols = df.select_dtypes(
                                            include=["number"]
                                        ).columns
                                        if len(numeric_cols) > 0:
                                            st.write("**Summary Statistics:**")
                                            st.dataframe(df[numeric_cols].describe())

                                    except Exception as e:
                                        st.error(f"Error loading table: {e}")

        # Custom table query section
        st.header("🔍 Custom Query")
        with st.expander("Run Custom SQL Query", expanded=False):
            custom_query = st.text_area(
                "Enter SQL Query:",
                placeholder=f"SELECT * FROM {catalog}.{schema}.gasproduction_forecast_summary LIMIT 100",
                height=100,
            )

            if st.button("Execute Query") and custom_query.strip():
                with st.spinner("Executing query..."):
                    try:
                        with conn.cursor() as cursor:
                            cursor.execute(custom_query)
                            result_df = cursor.fetchall_arrow().to_pandas()

                        st.success(
                            f"✅ Query executed successfully - {len(result_df):,} rows returned"
                        )
                        st.dataframe(result_df, use_container_width=True)

                    except Exception as e:
                        st.error(f"Query error: {e}")

    except Exception as e:
        st.error(f"Connection error: {e}")
        st.info(
            "Make sure your Databricks credentials are properly configured and the HTTP path is correct."
        )


if __name__ == "__main__":
    main()
