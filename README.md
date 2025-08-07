# Petrinex Production Forecasting

ARPS decline curve analysis and production forecasting for oil and gas wells with comprehensive data validation.

## Features

- **ARPS Decline Curves**: Automatic fitting of exponential, hyperbolic, and harmonic decline curves
- **Smart Data Processing**: Peak detection, outlier removal, and data quality filters
- **High-Quality Forecasts**: Average R² of 0.69 with 70% of wells achieving good fits (R² ≥ 0.6)
- **Data Validation Framework**: Bronze-to-silver data quality validation with change tracking
- **Interactive Visualization**: Streamlit app for exploring forecasts
- **Audit & Compliance**: JSON audit reports for data lineage and quality monitoring

## Quick Start

### 1. Install Dependencies
```bash
uv sync
```

### 2. Validate Data Quality
```bash
uv run python run_validation.py
```

### 3. Run Forecasting
```python
from petrinex.forecast import prepare_well_data, forecast_multiple_wells

# Load and prepare data
df = pd.read_parquet('fixtures/ngl_vol_bronze_cvx.parquet')
processed = prepare_well_data(df, min_months=6)

# Generate forecasts
forecasts = forecast_multiple_wells(processed, forecast_months=24, production_column='GasProduction')
```

## User Interface
We have created a Streamlit app to allow users to explore the forecasts.
```bash
uv run streamlit run streamlit_app.py
```

## Databricks Deployment
For deploying to Databricks:
```bash
databricks bundle validate
databricks bundle deploy --target dev
databricks bundle run
```

## Data Validation Framework

The project includes a comprehensive data quality framework that:

- **Bronze-to-Silver Validation**: Automatically validates incoming data against silver tables
- **Change Detection**: Efficiently tracks data changes using row hashes
- **Data Quality Checks**: Validates data types, ranges, and handles NA values
- **Audit Reports**: Generates JSON audit trails for compliance and monitoring

### Data Quality Pipeline

1. **Bronze Tables**: Raw Petrinex production data (`ngl_vol_bronze_cvx.parquet`, `conv_vol_bronze_cvx.parquet`)
2. **Validation**: Data type checking, range validation, NA handling, schema compliance
3. **Silver Tables**: Validated bronze data with metadata (hash, timestamps) for change tracking
4. **Audit Reports**: JSON files tracking data quality metrics and changes over time

All files are stored in `fixtures/` directory for easy access and future Spark integration.

### Validation Notebook

Run the complete validation pipeline using the Jupyter notebook:
```bash
uv run jupyter lab src/03_validate.ipynb
```

Or use the standalone script:
```bash
uv run python run_validation.py
```

## Testing

Run tests with:
```bash
uv run pytest
```

## Notes

- imperial unit conversions properly handled in the presentation layer (Streamlit app) rather than cluttering the core business logic.
- Logging should only happen within the codebase (src/petrinex) and not the notebooks.