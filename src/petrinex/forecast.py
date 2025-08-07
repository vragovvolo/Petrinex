"""
Forecasting module for oil and gas well production using ARPS decline curves.

This module provides functionality to:
1. Process well production data for forecasting
2. Fit ARPS (Arps) decline curves to historical production data
3. Generate production forecasts
"""

import logging
import warnings
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from scipy.optimize import curve_fit, OptimizeWarning

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit, when
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
)

logger = logging.getLogger(__name__)

warnings.filterwarnings("ignore", category=OptimizeWarning)


def filter_wells_by_min_months(df, min_months: int, well_id_column: str = "WellID"):
    """
    Filter wells to keep only those with minimum number of months of data.

    Args:
        df: DataFrame (pandas or Spark) with well production data
        min_months: Minimum number of months of data required for a well
        well_id_column: Name of the well ID column

    Returns:
        DataFrame with only wells that have sufficient data
    """
    well_counts = df.groupBy(well_id_column).agg(count("*").alias("month_count"))
    valid_wells = well_counts.filter(col("month_count") >= min_months).select(
        well_id_column
    )
    return df.join(valid_wells, on=well_id_column, how="inner")


def exponential_decline(t: np.ndarray, qi: float, di: float) -> np.ndarray:
    """
    Exponential decline curve (ARPS b=0).

    Args:
        t: Time array (days)
        qi: Initial production rate
        di: Nominal decline rate

    Returns:
        Production rate at time t
    """
    return qi * np.exp(-di * t / 365.25)


def hyperbolic_decline(t: np.ndarray, qi: float, di: float, b: float) -> np.ndarray:
    """
    Hyperbolic decline curve (ARPS 0 < b < 1).

    Args:
        t: Time array (days)
        qi: Initial production rate
        di: Initial decline rate
        b: Decline exponent (0 < b < 1)

    Returns:
        Production rate at time t
    """
    return qi * (1 + b * di * t / 365.25) ** (-1 / b)


def harmonic_decline(t: np.ndarray, qi: float, di: float) -> np.ndarray:
    """
    Harmonic decline curve (ARPS b=1).

    Args:
        t: Time array (days)
        qi: Initial production rate
        di: Initial decline rate

    Returns:
        Production rate at time t
    """
    return qi / (1 + di * t / 365.25)


def fit_arps_curve(
    well_data: pd.DataFrame,
    production_column: str = "OilProduction",
    curve_type: str = "auto",
) -> Dict:
    """
    Fit ARPS decline curve to well production data.

    Args:
        well_data: DataFrame with production data for a single well
        production_column: Column name for production values
        curve_type: Type of curve to fit ('exponential', 'hyperbolic', 'harmonic', 'auto')

    Returns:
        Dictionary with fit parameters, statistics, and forecast function
    """
    # Prepare data
    data = well_data.copy().sort_values("DaysFromFirst")

    # Filter out zero/negative production values
    data = data[data[production_column] > 0]

    if len(data) < 5:  # Increased minimum points for better fits
        logger.warning(f"Insufficient data points for curve fitting: {len(data)}")
        return {
            "success": False,
            "error": "Insufficient data points",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    # Identify peak production and fit from peak forward
    q_values = data[production_column].values
    peak_idx = np.argmax(q_values)

    # Only use data from peak production onward for decline curve fitting
    # This handles wells with initial ramp-up periods
    if peak_idx < len(data) - 3:  # Need at least 3 points after peak
        decline_data = data.iloc[peak_idx:].copy()
    else:
        # If peak is too late, use all data but check for decline pattern
        decline_data = data.copy()

    # Check if data shows actual decline pattern
    q_decline = decline_data[production_column].values
    t_decline = decline_data["DaysFromFirst"].values

    if len(q_decline) < 3:
        logger.warning(f"Insufficient decline data points: {len(q_decline)}")
        return {
            "success": False,
            "error": "Insufficient decline data points",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    # Check for minimum decline - if final production is > 80% of peak, likely no decline
    decline_ratio = q_decline[-1] / q_decline[0] if q_decline[0] > 0 else 1.0
    if decline_ratio > 0.8:
        logger.warning(
            f"Insufficient decline observed (final/peak = {decline_ratio:.3f})"
        )
        return {
            "success": False,
            "error": "No significant decline pattern observed",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    # Remove outliers using IQR method
    Q1 = np.percentile(q_decline, 25)
    Q3 = np.percentile(q_decline, 75)
    IQR = Q3 - Q1
    outlier_mask = (q_decline >= Q1 - 1.5 * IQR) & (q_decline <= Q3 + 1.5 * IQR)

    if np.sum(outlier_mask) < 3:
        # If too many outliers removed, use original data
        t = t_decline
        q = q_decline
    else:
        t = t_decline[outlier_mask]
        q = q_decline[outlier_mask]

    # Adjust time to start from 0 for fitting
    t_fit = t - t[0]

    # Improved initial parameter estimates based on data characteristics
    qi_est = q[0]  # Initial production from decline period

    # Estimate decline rate from data
    if len(q) >= 2:
        # Use log-linear regression for initial di estimate (exponential assumption)
        valid_q = q[q > 0]
        valid_t = t_fit[: len(valid_q)]

        if len(valid_q) >= 2:
            log_q = np.log(valid_q)
            # Linear regression: log(q) = log(qi) - di*t/365.25
            A = np.vstack([valid_t / 365.25, np.ones(len(valid_t))]).T
            try:
                slope, intercept = np.linalg.lstsq(A, log_q, rcond=None)[0]
                di_est = max(-slope, 0.01)  # Ensure positive decline
                qi_est = max(
                    np.exp(intercept), qi_est
                )  # Use regression estimate if better
            except:
                di_est = 0.1
        else:
            di_est = 0.1
    else:
        di_est = 0.1

    # Constrain estimates to reasonable ranges
    di_est = min(max(di_est, 0.01), 2.0)  # 1% to 200% annual decline
    qi_est = max(qi_est, q.min())  # At least minimum observed production
    b_est = 0.5

    results = {}

    if curve_type in ["exponential", "auto"]:
        # Try exponential decline with both nonlinear and log-linear methods
        best_exp_result = {"r_squared": -np.inf, "aic": np.inf}

        # Method 1: Log-linear regression (often more robust for exponential)
        try:
            if np.all(q > 0):  # Can only take log of positive values
                log_q = np.log(q)
                t_years = t_fit / 365.25

                # Linear regression: log(q) = log(qi) - di*t
                A = np.vstack([t_years, np.ones(len(t_years))]).T
                slope, intercept = np.linalg.lstsq(A, log_q, rcond=None)[0]

                qi_loglin = np.exp(intercept)
                di_loglin = -slope

                if di_loglin > 0 and qi_loglin > 0:  # Valid parameters
                    q_pred_loglin = exponential_decline(t_fit, qi_loglin, di_loglin)
                    r2_loglin = calculate_r_squared(q, q_pred_loglin)

                    if r2_loglin > best_exp_result["r_squared"]:
                        best_exp_result = {
                            "parameters": {"qi": qi_loglin, "di": di_loglin},
                            "r_squared": r2_loglin,
                            "aic": calculate_aic(q, q_pred_loglin, 2),
                            "t_offset": t[0],
                            "method": "log-linear",
                        }
        except Exception as e:
            logger.debug(f"Log-linear exponential fit failed: {e}")

        # Method 2: Nonlinear least squares with multiple attempts
        exp_attempts = [
            {"bounds": ([qi_est * 0.1, 0.001], [qi_est * 5, 3.0])},
            {"bounds": ([qi_est * 0.01, 0.0001], [qi_est * 20, 5.0])},
            {"bounds": ([0.001, 0.00001], [qi_est * 100, 10.0])},
        ]

        for attempt in exp_attempts:
            try:
                popt_exp, pcov_exp = curve_fit(
                    exponential_decline,
                    t_fit,
                    q,
                    p0=[qi_est, di_est],
                    bounds=attempt["bounds"],
                    maxfev=5000,
                    method="trf",
                )

                param_errors = np.sqrt(np.diag(pcov_exp))
                if not np.any(np.isnan(param_errors)) and not np.any(
                    np.isinf(param_errors)
                ):
                    q_pred_exp = exponential_decline(t_fit, *popt_exp)
                    r2_exp = calculate_r_squared(q, q_pred_exp)

                    if r2_exp > best_exp_result["r_squared"]:
                        best_exp_result = {
                            "parameters": {"qi": popt_exp[0], "di": popt_exp[1]},
                            "r_squared": r2_exp,
                            "aic": calculate_aic(q, q_pred_exp, len(popt_exp)),
                            "t_offset": t[0],
                            "method": "nonlinear",
                        }

                        if r2_exp > 0.8:  # Excellent fit found
                            break
            except Exception as e:
                logger.debug(f"Exponential nonlinear fit attempt failed: {e}")
                continue

        results["exponential"] = best_exp_result

    if curve_type in ["hyperbolic", "auto"]:
        # Try hyperbolic decline with multiple b values and bounds
        b_values = [0.1, 0.3, 0.5, 0.7, 0.9]
        hyp_attempts = []

        for b_init in b_values:
            hyp_attempts.extend(
                [
                    {
                        "p0": [qi_est, di_est, b_init],
                        "bounds": (
                            [qi_est * 0.1, 0.001, 0.001],
                            [qi_est * 5, 3.0, 0.999],
                        ),
                    },
                    {
                        "p0": [qi_est, di_est, b_init],
                        "bounds": (
                            [qi_est * 0.01, 0.0001, 0.001],
                            [qi_est * 20, 5.0, 0.999],
                        ),
                    },
                ]
            )

        best_hyp_result = {"r_squared": -np.inf, "aic": np.inf}

        for attempt in hyp_attempts:
            try:
                popt_hyp, pcov_hyp = curve_fit(
                    hyperbolic_decline,
                    t_fit,
                    q,
                    p0=attempt["p0"],
                    bounds=attempt["bounds"],
                    maxfev=10000,
                    method="trf",
                )

                param_errors = np.sqrt(np.diag(pcov_hyp))
                if not np.any(np.isnan(param_errors)) and not np.any(
                    np.isinf(param_errors)
                ):
                    q_pred_hyp = hyperbolic_decline(t_fit, *popt_hyp)
                    r2_hyp = calculate_r_squared(q, q_pred_hyp)

                    # Additional check: b should be reasonable (0 < b < 1)
                    if (
                        0.001 <= popt_hyp[2] <= 0.999
                        and r2_hyp > best_hyp_result["r_squared"]
                    ):
                        best_hyp_result = {
                            "parameters": {
                                "qi": popt_hyp[0],
                                "di": popt_hyp[1],
                                "b": popt_hyp[2],
                            },
                            "r_squared": r2_hyp,
                            "aic": calculate_aic(q, q_pred_hyp, len(popt_hyp)),
                            "t_offset": t[0],
                        }

                        if r2_hyp > 0.8:  # Very good fit found
                            break
            except Exception as e:
                logger.debug(f"Hyperbolic fit attempt failed: {e}")
                continue

        results["hyperbolic"] = best_hyp_result

    if curve_type in ["harmonic", "auto"]:
        # Try harmonic decline with improved bounds
        har_attempts = [
            {"bounds": ([qi_est * 0.1, 0.001], [qi_est * 5, 3.0])},
            {"bounds": ([qi_est * 0.01, 0.0001], [qi_est * 20, 5.0])},
            {"bounds": ([0.001, 0.00001], [qi_est * 100, 10.0])},
        ]

        best_har_result = {"r_squared": -np.inf, "aic": np.inf}

        for attempt in har_attempts:
            try:
                popt_har, pcov_har = curve_fit(
                    harmonic_decline,
                    t_fit,
                    q,
                    p0=[qi_est, di_est],
                    bounds=attempt["bounds"],
                    maxfev=5000,
                    method="trf",
                )

                param_errors = np.sqrt(np.diag(pcov_har))
                if not np.any(np.isnan(param_errors)) and not np.any(
                    np.isinf(param_errors)
                ):
                    q_pred_har = harmonic_decline(t_fit, *popt_har)
                    r2_har = calculate_r_squared(q, q_pred_har)

                    if r2_har > best_har_result["r_squared"]:
                        best_har_result = {
                            "parameters": {"qi": popt_har[0], "di": popt_har[1]},
                            "r_squared": r2_har,
                            "aic": calculate_aic(q, q_pred_har, len(popt_har)),
                            "t_offset": t[0],
                        }

                        if r2_har > 0.7:  # Good fit found
                            break
            except Exception as e:
                logger.debug(f"Harmonic fit attempt failed: {e}")
                continue

        results["harmonic"] = best_har_result

    # Select best curve if auto mode
    if curve_type == "auto":
        # Use AIC for model selection (lower is better)
        best_curve = min(results.keys(), key=lambda k: results[k].get("aic", np.inf))
    else:
        best_curve = curve_type

    if best_curve not in results or results[best_curve]["r_squared"] == -np.inf:
        return {
            "success": False,
            "error": "All curve fits failed",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    # Create forecast function with time offset handling
    params = results[best_curve]["parameters"]
    t_offset = results[best_curve].get("t_offset", 0)

    if best_curve == "exponential":
        forecast_func = lambda t_future: exponential_decline(
            t_future - t_offset, params["qi"], params["di"]
        )
    elif best_curve == "hyperbolic":
        forecast_func = lambda t_future: hyperbolic_decline(
            t_future - t_offset, params["qi"], params["di"], params["b"]
        )
    elif best_curve == "harmonic":
        forecast_func = lambda t_future: harmonic_decline(
            t_future - t_offset, params["qi"], params["di"]
        )

    return {
        "success": True,
        "curve_type": best_curve,
        "parameters": params,
        "r_squared": results[best_curve]["r_squared"],
        "forecast_func": forecast_func,
        "all_results": results,
    }


def calculate_r_squared(y_actual: np.ndarray, y_predicted: np.ndarray) -> float:
    """Calculate R-squared coefficient of determination."""
    ss_res = np.sum((y_actual - y_predicted) ** 2)
    ss_tot = np.sum((y_actual - np.mean(y_actual)) ** 2)
    return 1 - (ss_res / ss_tot) if ss_tot != 0 else 0


def calculate_aic(
    y_actual: np.ndarray, y_predicted: np.ndarray, n_params: int
) -> float:
    """Calculate Akaike Information Criterion."""
    n = len(y_actual)
    mse = np.mean((y_actual - y_predicted) ** 2)
    if mse <= 0:
        return np.inf
    return n * np.log(mse) + 2 * n_params


def forecast_well_production(
    well_data: pd.DataFrame,
    forecast_months: int = 24,
    production_column: str = "OilProduction",
    curve_type: str = "auto",
) -> Dict:
    """
    Forecast production for a single well using ARPS decline curves.

    Args:
        well_data: DataFrame with historical production data for a single well
        forecast_months: Number of months to forecast
        production_column: Column name for production values
        curve_type: Type of ARPS curve to fit

    Returns:
        Dictionary with historical data, fitted curve, and forecast
    """
    # Fit decline curve
    fit_result = fit_arps_curve(well_data, production_column, curve_type)

    if not fit_result["success"]:
        return {
            "success": False,
            "error": fit_result["error"],
            "historical_data": well_data,
            "forecast": None,
        }

    # Generate forecast time points
    last_day = well_data["DaysFromFirst"].max()
    forecast_days = np.arange(
        last_day + 30,  # Start 30 days after last data point
        last_day + (forecast_months * 30.44),  # Average days per month
        30.44,
    )

    # Generate forecast
    forecast_production = fit_result["forecast_func"](forecast_days)

    # Create forecast DataFrame
    first_date = well_data["ProductionMonth"].min()
    forecast_dates = [first_date + pd.Timedelta(days=int(d)) for d in forecast_days]

    forecast_df = pd.DataFrame(
        {
            "ProductionMonth": forecast_dates,
            "DaysFromFirst": forecast_days,
            f"{production_column}_Forecast": forecast_production,
        }
    )

    # Get AIC from the best fit result
    aic_value = None
    if (
        "all_results" in fit_result
        and fit_result["curve_type"] in fit_result["all_results"]
    ):
        aic_value = fit_result["all_results"][fit_result["curve_type"]].get("aic", None)

    return {
        "success": True,
        "well_id": well_data["WellID"].iloc[0],
        "curve_type": fit_result["curve_type"],
        "parameters": fit_result["parameters"],
        "r_squared": fit_result["r_squared"],
        "aic": aic_value,
        "historical_data": well_data,
        "forecast": forecast_df,
    }


def forecast_multiple_wells(
    df: pd.DataFrame,
    forecast_months: int = 24,
    production_column: str = "OilProduction",
    curve_type: str = "auto",
    min_r_squared: float = 0.5,
) -> Dict[str, Dict]:
    """
    Forecast production for multiple wells.

    Args:
        df: DataFrame with production data for multiple wells
        forecast_months: Number of months to forecast
        production_column: Column name for production values
        curve_type: Type of ARPS curve to fit
        min_r_squared: Minimum R-squared threshold for acceptable fits

    Returns:
        Dictionary mapping well IDs to forecast results
    """
    results = {}
    wells = df["WellID"].unique()

    logger.info(f"Forecasting {len(wells)} wells using {curve_type} decline curves")

    for well_id in wells:
        well_data = df[df["WellID"] == well_id].copy()

        try:
            result = forecast_well_production(
                well_data, forecast_months, production_column, curve_type
            )

            if result["success"] and result["r_squared"] >= min_r_squared:
                results[well_id] = result
                logger.debug(
                    f"Well {well_id}: {result['curve_type']} fit, R² = {result['r_squared']:.3f}"
                )
            else:
                logger.warning(
                    f"Well {well_id}: Poor fit (R² = {result.get('r_squared', 0):.3f})"
                )

        except Exception as e:
            logger.error(f"Error forecasting well {well_id}: {e}")

    logger.info(f"Successfully forecast {len(results)} out of {len(wells)} wells")

    return results


def export_forecast_summary_table(
    forecasts: Dict[str, Dict], production_type: str = "OilProduction"
) -> pd.DataFrame:
    """
    Export comprehensive forecast summary with ARPS parameters and date ranges.

    Args:
        forecasts: Dictionary of forecast results from forecast_multiple_wells
        production_type: Type of production being forecast

    Returns:
        DataFrame with complete forecast summary information including all ARPS parameters
    """
    import datetime as dt

    if not forecasts:
        return pd.DataFrame()

    summary_data = []
    forecast_date = dt.datetime.now().strftime("%Y-%m-%d")

    for well_id, forecast in forecasts.items():
        # Get ARPS parameters
        params = forecast["parameters"]

        # Calculate cumulative forecast production (first 12 months)
        forecast_df = forecast["forecast"]
        if len(forecast_df) >= 12:
            first_12_months = forecast_df.head(12)
            cum_forecast_12m = first_12_months[f"{production_type}_Forecast"].sum()
        else:
            cum_forecast_12m = forecast_df[f"{production_type}_Forecast"].sum()

        # Get historical data stats and date ranges
        hist_data = forecast["historical_data"]
        hist_prod = hist_data[hist_data[production_type] > 0]

        # Date range information
        if len(hist_prod) > 0:
            min_date = hist_prod["ProductionMonth"].min()
            max_date = hist_prod["ProductionMonth"].max()
            data_points = len(hist_prod)

            # Production statistics
            avg_production = hist_prod[production_type].mean()
            peak_production = hist_prod[production_type].max()
            last_production = hist_prod[production_type].iloc[-1]
        else:
            min_date = None
            max_date = None
            data_points = 0
            avg_production = 0
            peak_production = 0
            last_production = 0

        # Enhanced summary row with all ARPS parameters and metadata
        summary_row = {
            # Identifiers and metadata
            "WellID": well_id,
            "ProductionType": production_type,
            "ForecastDate": forecast_date,
            # ARPS fit information
            "CurveType": forecast["curve_type"],
            "RSquared": forecast["r_squared"],
            "FitSuccess": forecast["success"],
            # All ARPS parameters (qi, di, b)
            "InitialRate_qi": params.get("qi", None),
            "DeclineRate_di": params.get("di", None),
            "DeclineExponent_b": params.get("b", None),
            # Historical data date range
            "HistoricalDataMinDate": min_date,
            "HistoricalDataMaxDate": max_date,
            "DataPointsUsed": data_points,
            # Historical production statistics
            "HistoricalAvgProduction": avg_production,
            "HistoricalPeakProduction": peak_production,
            "HistoricalLastProduction": last_production,
            # Forecast information
            "ForecastStartDate": (
                forecast_df["ProductionMonth"].iloc[0] if len(forecast_df) > 0 else None
            ),
            "ForecastCumulative12Month": cum_forecast_12m,
            # Additional fit quality metrics
            "AIC": forecast.get("aic", None),
            "FitError": forecast.get("error", None),
        }

        summary_data.append(summary_row)

    return pd.DataFrame(summary_data)


def export_forecasted_production_table(
    forecasts: Dict[str, Dict],
    production_type: str = "OilProduction",
    original_data: pd.DataFrame = None,
) -> pd.DataFrame:
    """
    Export forecasted production data in the same schema as input data.

    Args:
        forecasts: Dictionary of forecast results from forecast_multiple_wells
        production_type: Type of production being forecast
        original_data: Original NGL data to match schema

    Returns:
        DataFrame with forecasted production in same schema as input
    """
    if not forecasts:
        return pd.DataFrame()

    forecast_data = []

    for well_id, forecast in forecasts.items():
        forecast_df = forecast["forecast"]

        # Get well metadata from historical data
        hist_data = forecast["historical_data"]
        well_info = hist_data.iloc[0]  # Get metadata from first row

        for _, row in forecast_df.iterrows():
            # Create forecast row matching input schema
            forecast_row = {
                # Core identifiers
                "WellID": well_id,
                "ProductionMonth": row["ProductionMonth"].strftime("%Y-%m"),
                # Production values - set forecast value for the specified type, zero for others
                "OilProduction": (
                    row[f"{production_type}_Forecast"]
                    if production_type == "OilProduction"
                    else 0.0
                ),
                "GasProduction": (
                    row[f"{production_type}_Forecast"]
                    if production_type == "GasProduction"
                    else 0.0
                ),
                "CondensateProduction": (
                    row[f"{production_type}_Forecast"]
                    if production_type == "CondensateProduction"
                    else 0.0
                ),
                "WaterProduction": 0.0,  # Not forecast
                # Additional volumes (set to zero for forecasts)
                "ResidueGasVolume": 0.0,
                "Energy": 0.0,
                "EthaneMixVolume": 0.0,
                "EthaneSpecVolume": 0.0,
                "PropaneMixVolume": 0.0,
                "PropaneSpecVolume": 0.0,
                "ButaneMixVolume": 0.0,
                "ButaneSpecVolume": 0.0,
                "PentaneMixVolume": 0.0,
                "PentaneSpecVolume": 0.0,
                "LiteMixVolume": 0.0,
                # Facility and operator info (from historical data)
                "ReportingFacilityID": well_info.get("ReportingFacilityID", ""),
                "ReportingFacilityName": well_info.get("ReportingFacilityName", ""),
                "OperatorBAID": well_info.get("OperatorBAID", ""),
                "OperatorName": well_info.get("OperatorName", ""),
                "WellLicenseNumber": well_info.get("WellLicenseNumber", ""),
                "Field": well_info.get("Field", ""),
                "Pool": well_info.get("Pool", ""),
                "Area": well_info.get("Area", ""),
                # Operational info
                "Hours": 720.0,  # Assume full month operation (30 days * 24 hours)
                # Forecast metadata (additional columns)
                "DataType": "Forecast",
                "ForecastMethod": f"ARPS_{forecast['curve_type']}",
                "ForecastRSquared": forecast["r_squared"],
            }

            forecast_data.append(forecast_row)

    forecast_table = pd.DataFrame(forecast_data)

    # Ensure datetime format matches original
    if len(forecast_table) > 0:
        forecast_table["ProductionMonth"] = pd.to_datetime(
            forecast_table["ProductionMonth"]
        )

    return forecast_table


def combine_historical_and_forecast(
    historical_data: pd.DataFrame,
    forecast_data: pd.DataFrame,
    production_type: str = "OilProduction",
) -> pd.DataFrame:
    """
    Combine historical and forecast data into a single table.

    Args:
        historical_data: Original historical production data
        forecast_data: Forecasted production data from export_forecasted_production_table
        production_type: Type of production being analyzed

    Returns:
        Combined DataFrame with both historical and forecast data
    """
    # Make copies to avoid modifying original data
    historical_data = historical_data.copy()
    forecast_data = forecast_data.copy()

    # Add DataType column to historical data if not present
    if "DataType" not in historical_data.columns:
        historical_data["DataType"] = "Historical"
        historical_data["ForecastMethod"] = None
        historical_data["ForecastRSquared"] = None

    # Ensure both DataFrames have the same columns
    hist_cols = set(historical_data.columns)
    forecast_cols = set(forecast_data.columns)

    # Add missing columns with default values
    for col in forecast_cols - hist_cols:
        if col not in ["DataType", "ForecastMethod", "ForecastRSquared"]:
            historical_data[col] = (
                0.0 if col.endswith(("Production", "Volume", "Energy", "Hours")) else ""
            )

    for col in hist_cols - forecast_cols:
        if col not in ["DataType", "ForecastMethod", "ForecastRSquared"]:
            forecast_data[col] = (
                0.0 if col.endswith(("Production", "Volume", "Energy", "Hours")) else ""
            )

    # Combine the data, handling empty DataFrames and dtype consistency
    if historical_data.empty and forecast_data.empty:
        # Both empty - return empty DataFrame with combined columns
        all_columns = list(hist_cols.union(forecast_cols))
        combined_data = pd.DataFrame(columns=all_columns)
    elif historical_data.empty:
        # Only historical is empty
        combined_data = forecast_data.copy()
    elif forecast_data.empty:
        # Only forecast is empty
        combined_data = historical_data.copy()
    else:
        # Both have data - ensure dtype consistency to avoid warnings
        # Filter out any columns that are all-NA in either DataFrame
        hist_data_clean = historical_data.dropna(axis=1, how="all")
        forecast_data_clean = forecast_data.dropna(axis=1, how="all")

        if hist_data_clean.empty or forecast_data_clean.empty:
            # One of them became empty after dropping all-NA columns
            combined_data = (
                pd.concat([hist_data_clean, forecast_data_clean], ignore_index=True)
                if not hist_data_clean.empty or not forecast_data_clean.empty
                else pd.DataFrame()
            )
        else:
            # Both have valid data
            combined_data = pd.concat(
                [hist_data_clean, forecast_data_clean], ignore_index=True
            )

    # Sort by well and date
    combined_data = combined_data.sort_values(["WellID", "ProductionMonth"])

    return combined_data


def forecast_spark_workflow(
    spark: SparkSession, config, input_table: str, **kwargs
) -> Dict[str, str]:
    """
    Spark-based forecasting workflow with memory-efficient batch processing.

    Args:
        spark: SparkSession
        config: Config object with forecast settings
        input_table: Full table name (catalog.schema.table) for input data


    Returns:
        Dictionary mapping production types to output table names
    """
    
    forecast_months = getattr(config.forecast, "forecast_months", 30)
    
    min_months = getattr(config.forecast, "forecast_months", 30)
    production_columns = getattr(
        config.ngl, 
        "production_columns",
        ['OilProduction','GasProduction', 'CondensateProduction']
        )
    curve_type = getattr(config.forecast, "curve_type", "auto")
    min_r_squared = getattr(config.forecast, "min_r_squared", 0.3)
    include_wells = getattr(config.forecast, "include_wells", [])
    exclude_wells = getattr(config.forecast, "exclude_wells", [])
    batch_size = getattr(config.forecast, "batch_size", 500)

    # Load and filter the Spark table
    input_df = spark.table(input_table)
    if include_wells:
        input_wells_df = input_df.filter(
            (input_df.WellID.isin(include_wells)) 
            & (~input_df.WellID.isin(exclude_wells))
        )
    else:
        input_wells_df = input_df.filter(~input_df.WellID.isin(exclude_wells))
    
    filtered_df = filter_wells_by_min_months(input_wells_df, min_months)

    # Get list of wells to process
    wells_list = [
        row.WellID for row in filtered_df.select("WellID").distinct().collect()
    ]
    total_wells = len(wells_list)

    output_tables = {}

    # Process each production type
    for production_type in production_columns:
        all_summaries = []
        all_forecasts = []
        all_combined = []

        # Process wells in batches
        for i in range(0, total_wells, batch_size):
            batch_wells = wells_list[i : i + batch_size]

            # Filter data for this batch of wells
            batch_spark_df = filtered_df.filter(col("WellID").isin(batch_wells))
            batch_pandas_df = batch_spark_df.toPandas()

            if batch_pandas_df.empty:
                continue

            # Run forecasting on this batch
            batch_forecasts = forecast_multiple_wells(
                batch_pandas_df,
                forecast_months=forecast_months,
                production_column=production_type,
                curve_type=curve_type,
                min_r_squared=min_r_squared,
            )

            if not batch_forecasts:
                continue

            # Generate tables for this batch
            batch_summary = export_forecast_summary_table(
                batch_forecasts, production_type
            )
            batch_forecast_production = export_forecasted_production_table(
                batch_forecasts, production_type, batch_pandas_df
            )
            batch_combined = combine_historical_and_forecast(
                batch_pandas_df, batch_forecast_production, production_type
            )

            # Collect batch results
            if len(batch_summary) > 0:
                all_summaries.append(batch_summary)
            if len(batch_forecast_production) > 0:
                all_forecasts.append(batch_forecast_production)
            if len(batch_combined) > 0:
                all_combined.append(batch_combined)

        # Combine all batch results and write to tables
        if all_summaries:
            final_summary = pd.concat(all_summaries, ignore_index=True)
            summary_table_name = f"{config.catalog}.{config.schema}.{production_type.lower()}_forecast_summary"
            _write_pandas_to_spark_table(spark, final_summary, summary_table_name)

        if all_forecasts:
            final_forecasts = pd.concat(all_forecasts, ignore_index=True)
            forecast_table_name = f"{config.catalog}.{config.schema}.{production_type.lower()}_forecasted_production"
            _write_pandas_to_spark_table(spark, final_forecasts, forecast_table_name)

        if all_combined:
            final_combined = pd.concat(all_combined, ignore_index=True)
            combined_table_name = f"{config.catalog}.{config.schema}.{production_type.lower()}_historical_forecast_combined"
            _write_pandas_to_spark_table(spark, final_combined, combined_table_name)

        output_tables[production_type] = {
            "summary": summary_table_name,
            "forecast": forecast_table_name,
            "combined": combined_table_name,
        }

    return output_tables


def _write_pandas_to_spark_table(
    spark: SparkSession,
    pandas_df: pd.DataFrame,
    table_name: str,
    mode: str = "overwrite",
) -> None:
    """
    Write a Pandas DataFrame to a Spark table with proper schema handling.

    Args:
        spark: SparkSession
        pandas_df: Pandas DataFrame to write
        table_name: Full table name (catalog.schema.table)
        mode: Write mode ('overwrite', 'append', etc.)
    """
    if pandas_df.empty:
        return

    # Convert Pandas DataFrame to Spark DataFrame
    spark_df = spark.createDataFrame(pandas_df)

    # Write to table
    spark_df.write.mode(mode).option("mergeSchema", "true").saveAsTable(table_name)
