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
from petrinex.utils import table_exists
from petrinex.process import build_ngl_calendar_and_normalized_tables
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
    min_r_squared: float = 0.3,
) -> Dict:
    """
    Fit ARPS decline curve to well production data.

    Args:
        well_data: DataFrame with production data for a single well
        production_column: Column name for production values
        curve_type: Type of curve to fit ('exponential', 'hyperbolic', 'harmonic', 'auto')
        min_r_squared: Minimum R-squared threshold for acceptable fits

    Returns:
        Dictionary with fit parameters, statistics, and forecast function
    """
    well_id = (
        well_data["WellID"].iloc[0] if "WellID" in well_data.columns else "Unknown"
    )

    # Prepare data
    data = well_data.copy().sort_values("DaysFromFirst")

    # Filter out zero/negative production values
    data = data[data[production_column] > 0]

    if len(data) < 8:  # Need more points for robust decline curve fitting
        logger.info(
            f"Well {well_id}: Insufficient data points for {production_column} curve fitting: {len(data)} points (need ≥8)"
        )
        return {
            "success": False,
            "error": "Insufficient data points",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    # Better peak detection using rolling average to handle volatility
    window_size = min(6, len(data) // 4)  # Use 6-month or 25% of data window
    if window_size >= 3:
        rolling_avg = (
            data[production_column].rolling(window=window_size, center=True).mean()
        )
        peak_idx = rolling_avg.idxmax()
        peak_idx = data.index.get_loc(peak_idx)
    else:
        peak_idx = np.argmax(data[production_column].values)

    # Use data from peak onward, but ensure we have enough decline period
    min_decline_points = max(6, len(data) // 3)  # Need substantial decline data
    if peak_idx < len(data) - min_decline_points:
        decline_data = data.iloc[peak_idx:].copy()
    else:
        # If peak is too late, use more of the data but skip early ramp-up
        start_idx = min(peak_idx // 2, len(data) - min_decline_points)
        decline_data = data.iloc[start_idx:].copy()

    q_decline = decline_data[production_column].values
    t_decline = decline_data["DaysFromFirst"].values

    if len(q_decline) < 6:
        logger.info(
            f"Well {well_id}: Insufficient decline data for {production_column}: {len(q_decline)} points (need ≥6)"
        )
        return {
            "success": False,
            "error": "Insufficient decline data points",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    # More sophisticated decline detection using trend analysis
    # Check both overall decline and sustained decline trend
    overall_decline = q_decline[-1] / q_decline[0] if q_decline[0] > 0 else 1.0

    # Also check if last third shows decline compared to first third
    third_size = len(q_decline) // 3
    if third_size >= 2:
        early_avg = np.mean(q_decline[:third_size])
        late_avg = np.mean(q_decline[-third_size:])
        trend_decline = late_avg / early_avg if early_avg > 0 else 1.0
    else:
        trend_decline = overall_decline

    # Use the more conservative (smaller) decline ratio
    decline_ratio = min(overall_decline, trend_decline)

    if decline_ratio > 0.7:  # More permissive threshold
        logger.info(
            f"Well {well_id}: No significant decline in {production_column} (overall={overall_decline:.3f}, trend={trend_decline:.3f})"
        )
        return {
            "success": False,
            "error": "No significant decline pattern observed",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    logger.debug(
        f"Well {well_id}: {production_column} decline analysis - overall={overall_decline:.3f}, trend={trend_decline:.3f}, using {len(q_decline)} points"
    )

    # With normalized data (no downtime), outlier removal can be more conservative
    # Use simple IQR-based outlier detection without smoothing
    Q1 = np.percentile(q_decline, 25)
    Q3 = np.percentile(q_decline, 75)
    IQR = Q3 - Q1

    # Conservative outlier bounds - only remove extreme outliers
    outlier_mask = (q_decline >= Q1 - 2.5 * IQR) & (q_decline <= Q3 + 2.5 * IQR)
    outliers_removed = len(q_decline) - np.sum(outlier_mask)

    if np.sum(outlier_mask) < len(q_decline) * 0.8:  # Keep at least 80% of data
        # If too many outliers would be removed, keep all data
        logger.debug(
            f"Well {well_id}: {outliers_removed} potential outliers detected but keeping all normalized data"
        )
        t = t_decline
        q = q_decline
    else:
        # Remove only extreme outliers
        t = t_decline[outlier_mask]
        q = q_decline[outlier_mask]
        if outliers_removed > 0:
            logger.debug(
                f"Well {well_id}: Removed {outliers_removed} extreme outliers from normalized {production_column} data"
            )

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

    # Select best curve with improved criteria
    valid_results = {
        k: v for k, v in results.items() if v.get("r_squared", -np.inf) > 0
    }

    if not valid_results:
        logger.info(
            f"Well {well_id}: All {production_column} curve fits failed - no valid results"
        )
        return {
            "success": False,
            "error": "All curve fits failed",
            "curve_type": None,
            "parameters": {},
            "r_squared": 0,
            "forecast_func": None,
        }

    if curve_type == "auto":
        # For auto mode, prefer log-linear exponential if good enough, otherwise use AIC
        if "exponential" in valid_results:
            exp_result = valid_results["exponential"]
            if (
                exp_result.get("method") == "log-linear"
                and exp_result["r_squared"] >= 0.4
            ):
                best_curve = "exponential"
                logger.debug(
                    f"Well {well_id}: Selected log-linear exponential fit (R²={exp_result['r_squared']:.3f})"
                )
            else:
                # Use AIC for model selection (lower is better)
                best_curve = min(
                    valid_results.keys(),
                    key=lambda k: valid_results[k].get("aic", np.inf),
                )
                logger.debug(f"Well {well_id}: Selected {best_curve} based on AIC")
        else:
            best_curve = min(
                valid_results.keys(), key=lambda k: valid_results[k].get("aic", np.inf)
            )
            logger.debug(f"Well {well_id}: Selected {best_curve} based on AIC")
    else:
        best_curve = curve_type
        if best_curve not in valid_results:
            logger.info(
                f"Well {well_id}: Requested {curve_type} fit failed for {production_column}"
            )
            return {
                "success": False,
                "error": f"{curve_type} curve fit failed",
                "curve_type": None,
                "parameters": {},
                "r_squared": 0,
                "forecast_func": None,
            }

    # Check if the best result meets minimum quality threshold
    best_result = valid_results[best_curve]
    if best_result["r_squared"] < min_r_squared:
        logger.info(
            f"Well {well_id}: Best {production_column} fit ({best_curve}) R²={best_result['r_squared']:.3f} below threshold {min_r_squared}"
        )
        return {
            "success": False,
            "error": f"Best fit R² ({best_result['r_squared']:.3f}) below threshold ({min_r_squared})",
            "curve_type": best_curve,
            "parameters": best_result["parameters"],
            "r_squared": best_result["r_squared"],
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

    # Log successful fit
    final_r2 = results[best_curve]["r_squared"]
    method_info = results[best_curve].get("method", "")
    method_str = f" ({method_info})" if method_info else ""
    logger.info(
        f"Well {well_id}: Successful {production_column} forecast - {best_curve}{method_str} curve, R²={final_r2:.3f}"
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
    min_r_squared: float = 0.3,
) -> Dict:
    """
    Forecast production for a single well using ARPS decline curves.

    Args:
        well_data: DataFrame with historical production data for a single well
        forecast_months: Number of months to forecast
        production_column: Column name for production values
        curve_type: Type of ARPS curve to fit
        min_r_squared: Minimum R-squared threshold for acceptable fits

    Returns:
        Dictionary with historical data, fitted curve, and forecast
    """
    # Fit decline curve
    fit_result = fit_arps_curve(well_data, production_column, curve_type, min_r_squared)

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
    ).astype(
        int
    )  # Convert to integers for schema compatibility

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
                well_data, forecast_months, production_column, curve_type, min_r_squared
            )

            if result["success"]:
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
            "InitialRate_qi": params.get("qi", 0.0),
            "DeclineRate_di": params.get("di", 0.0),
            "DeclineExponent_b": params.get(
                "b", 0.0
            ),  # Use 0.0 for exponential/harmonic curves
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
            "AIC": forecast.get("aic", 0.0),
            "FitError": forecast.get("error", ""),
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
            if col == "DaysFromFirst":
                historical_data[col] = 0  # Integer default for DaysFromFirst
            elif col.endswith(("Production", "Volume", "Energy", "Hours")):
                historical_data[col] = 0.0
            else:
                historical_data[col] = ""

    for col in hist_cols - forecast_cols:
        if col not in ["DataType", "ForecastMethod", "ForecastRSquared"]:
            if col == "DaysFromFirst":
                forecast_data[col] = 0  # Integer default for DaysFromFirst
            elif col.endswith(("Production", "Volume", "Energy", "Hours")):
                forecast_data[col] = 0.0
            else:
                forecast_data[col] = ""

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

    # Ensure DaysFromFirst is integer type for schema compatibility
    if "DaysFromFirst" in combined_data.columns:
        combined_data["DaysFromFirst"] = combined_data["DaysFromFirst"].astype(int)

    return combined_data


def forecast_spark_workflow(
    spark: SparkSession,
    config,
    input_table: str = None,
    use_normalized: bool = True,
    **kwargs,
) -> Dict[str, str]:
    """
    Spark-based forecasting workflow with memory-efficient batch processing.

    Args:
        spark: SparkSession
        config: Config object with forecast settings
        input_table: Full table name (catalog.schema.table) for input data.
                    If None and use_normalized=True, uses normalized table
        use_normalized: Whether to use normalized production data for better decline analysis


    Returns:
        Dictionary mapping production types to output table names
    """

    forecast_months = getattr(config.forecast, "horizon_months", 30)

    min_months = getattr(config.forecast, "min_months", 6)
    production_columns = getattr(
        config.ngl,
        "production_columns",
        ["OilProduction", "GasProduction", "CondensateProduction"],
    )
    curve_type = getattr(config.forecast, "curve_type", "auto")
    min_r_squared = getattr(config.forecast, "min_r_squared", 0.3)
    include_wells = getattr(config.forecast, "include_wells", [])
    exclude_wells = getattr(config.forecast, "exclude_wells", [])
    batch_size = getattr(config.forecast, "batch_size", 500)

    # Determine input table to use and ensure prerequisites exist
    if input_table is None:
        if use_normalized:
            normalized_table = f"{config.catalog}.{config.schema}.ngl_normalized"
            calendar_table = f"{config.catalog}.{config.schema}.ngl_calendar"

            # Build normalized/calendar if missing
            if not table_exists(spark, normalized_table) or not table_exists(
                spark, calendar_table
            ):
                logger.info(
                    "Building calendar and normalized tables prior to forecasting"
                )
                build_ngl_calendar_and_normalized_tables(spark, config)

            if table_exists(spark, normalized_table):
                input_table = normalized_table
                logger.info(
                    "Using normalized production data for improved decline curve analysis"
                )
            else:
                # Fallback to base silver
                input_table = f"{config.catalog}.{config.schema}.ngl_silver"
                logger.warning(
                    "Normalized table not available; falling back to NGL silver"
                )
        else:
            input_table = f"{config.catalog}.{config.schema}.ngl_silver"
            logger.info("Using standard NGL silver table")
    else:
        logger.info(f"Using specified input table: {input_table}")

    # Load and filter the Spark table
    input_df = spark.table(input_table)

    # When using normalized data, use the daily rate columns for forecasting
    if use_normalized and "normalized" in input_table.lower():
        # Map production columns to their daily rate equivalents
        production_column_mapping = {}
        for prod_col in production_columns:
            if prod_col + "DailyRate" in [f.name for f in input_df.schema.fields]:
                production_column_mapping[prod_col] = prod_col + "DailyRate"
                logger.info(f"Using {prod_col}DailyRate for {prod_col} forecasting")
            else:
                production_column_mapping[prod_col] = prod_col
                logger.warning(
                    f"Daily rate column not found for {prod_col}, using original"
                )

        # Use DaysFromFirstProducing instead of DaysFromFirst for normalized data
        time_column = (
            "DaysFromFirstProducing"
            if "DaysFromFirstProducing" in [f.name for f in input_df.schema.fields]
            else "DaysFromFirst"
        )
        logger.info(f"Using {time_column} for time-based analysis")
    else:
        production_column_mapping = {col: col for col in production_columns}
        time_column = "DaysFromFirst"
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
        # Get the actual column name to use for forecasting
        actual_production_column = production_column_mapping[production_type]
        logger.info(
            f"Processing {production_type} using column {actual_production_column}"
        )

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

            # Ensure the time column is named correctly for the forecast functions
            if (
                time_column != "DaysFromFirst"
                and time_column in batch_pandas_df.columns
            ):
                # Drop any existing DaysFromFirst column to avoid conflicts
                if "DaysFromFirst" in batch_pandas_df.columns:
                    batch_pandas_df = batch_pandas_df.drop(columns=["DaysFromFirst"])
                batch_pandas_df = batch_pandas_df.rename(
                    columns={time_column: "DaysFromFirst"}
                )

            # Run forecasting on this batch using the mapped production column
            batch_forecasts = forecast_multiple_wells(
                batch_pandas_df,
                forecast_months=forecast_months,
                production_column=actual_production_column,
                curve_type=curve_type,
                min_r_squared=min_r_squared,
            )

            if not batch_forecasts:
                continue

            # Generate tables for this batch
            # Pass the actual column name used for forecasting
            batch_summary = export_forecast_summary_table(
                batch_forecasts, actual_production_column
            )
            batch_forecast_production = export_forecasted_production_table(
                batch_forecasts, actual_production_column, batch_pandas_df
            )
            batch_combined = combine_historical_and_forecast(
                batch_pandas_df, batch_forecast_production, actual_production_column
            )

            # Collect batch results
            if len(batch_summary) > 0:
                all_summaries.append(batch_summary)
            if len(batch_forecast_production) > 0:
                all_forecasts.append(batch_forecast_production)
            if len(batch_combined) > 0:
                all_combined.append(batch_combined)

        # Initialize table names
        summary_table_name = f"{config.catalog}.{config.schema}.{production_type.lower()}_forecast_summary"
        forecast_table_name = f"{config.catalog}.{config.schema}.{production_type.lower()}_forecasted_production"
        combined_table_name = f"{config.catalog}.{config.schema}.{production_type.lower()}_historical_forecast_combined"

        # Combine all batch results and write to tables
        # Always create tables, even if empty
        if all_summaries:
            final_summary = pd.concat(all_summaries, ignore_index=True)
        else:
            # Create empty summary table with proper schema
            final_summary = _create_empty_summary_table(production_type)
        _write_pandas_to_spark_table(spark, final_summary, summary_table_name)

        if all_forecasts:
            final_forecasts = pd.concat(all_forecasts, ignore_index=True)
        else:
            # Create empty forecast table with proper schema
            final_forecasts = _create_empty_forecast_table(production_type)
        _write_pandas_to_spark_table(spark, final_forecasts, forecast_table_name)

        if all_combined:
            final_combined = pd.concat(all_combined, ignore_index=True)
        else:
            # Create empty combined table with proper schema
            final_combined = _create_empty_combined_table(production_type)
        _write_pandas_to_spark_table(spark, final_combined, combined_table_name)

        output_tables[production_type] = {
            "summary": summary_table_name,
            "forecast": forecast_table_name,
            "combined": combined_table_name,
        }

    return output_tables


def _create_empty_summary_table(production_type: str) -> pd.DataFrame:
    """Create an empty summary table with proper schema."""
    import datetime as dt

    return pd.DataFrame(
        {
            "WellID": pd.Series([], dtype="string"),
            "ProductionType": pd.Series([], dtype="string"),
            "ForecastDate": pd.Series([], dtype="string"),
            "CurveType": pd.Series([], dtype="string"),
            "RSquared": pd.Series([], dtype="float64"),
            "FitSuccess": pd.Series([], dtype="bool"),
            "InitialRate_qi": pd.Series([], dtype="float64"),
            "DeclineRate_di": pd.Series([], dtype="float64"),
            "DeclineExponent_b": pd.Series([], dtype="float64"),
            "HistoricalDataMinDate": pd.Series([], dtype="datetime64[ns]"),
            "HistoricalDataMaxDate": pd.Series([], dtype="datetime64[ns]"),
            "DataPointsUsed": pd.Series([], dtype="int32"),
            "HistoricalAvgProduction": pd.Series([], dtype="float64"),
            "HistoricalPeakProduction": pd.Series([], dtype="float64"),
            "HistoricalLastProduction": pd.Series([], dtype="float64"),
            "ForecastStartDate": pd.Series([], dtype="datetime64[ns]"),
            "ForecastCumulative12Month": pd.Series([], dtype="float64"),
            "AIC": pd.Series([], dtype="float64"),
            "FitError": pd.Series([], dtype="string"),
        }
    )


def _create_empty_forecast_table(production_type: str) -> pd.DataFrame:
    """Create an empty forecast table with proper schema matching NGL silver."""
    return pd.DataFrame(
        {
            "WellID": pd.Series([], dtype="string"),
            "ProductionMonth": pd.Series([], dtype="datetime64[ns]"),
            "OilProduction": pd.Series([], dtype="float64"),
            "GasProduction": pd.Series([], dtype="float64"),
            "CondensateProduction": pd.Series([], dtype="float64"),
            "WaterProduction": pd.Series([], dtype="float64"),
            "ResidueGasVolume": pd.Series([], dtype="float64"),
            "Energy": pd.Series([], dtype="float64"),
            "EthaneMixVolume": pd.Series([], dtype="float64"),
            "EthaneSpecVolume": pd.Series([], dtype="float64"),
            "PropaneMixVolume": pd.Series([], dtype="float64"),
            "PropaneSpecVolume": pd.Series([], dtype="float64"),
            "ButaneMixVolume": pd.Series([], dtype="float64"),
            "ButaneSpecVolume": pd.Series([], dtype="float64"),
            "PentaneMixVolume": pd.Series([], dtype="float64"),
            "PentaneSpecVolume": pd.Series([], dtype="float64"),
            "LiteMixVolume": pd.Series([], dtype="float64"),
            "ReportingFacilityID": pd.Series([], dtype="string"),
            "ReportingFacilityName": pd.Series([], dtype="string"),
            "OperatorBAID": pd.Series([], dtype="string"),
            "OperatorName": pd.Series([], dtype="string"),
            "WellLicenseNumber": pd.Series([], dtype="string"),
            "Field": pd.Series([], dtype="string"),
            "Pool": pd.Series([], dtype="string"),
            "Area": pd.Series([], dtype="string"),
            "Hours": pd.Series([], dtype="float64"),
            "DataType": pd.Series([], dtype="string"),
            "ForecastMethod": pd.Series([], dtype="string"),
            "ForecastRSquared": pd.Series([], dtype="float64"),
        }
    )


def _create_empty_combined_table(production_type: str) -> pd.DataFrame:
    """Create an empty combined table with proper schema."""
    return pd.DataFrame(
        {
            "WellID": pd.Series([], dtype="string"),
            "ProductionMonth": pd.Series([], dtype="datetime64[ns]"),
            "OilProduction": pd.Series([], dtype="float64"),
            "GasProduction": pd.Series([], dtype="float64"),
            "CondensateProduction": pd.Series([], dtype="float64"),
            "WaterProduction": pd.Series([], dtype="float64"),
            "ResidueGasVolume": pd.Series([], dtype="float64"),
            "Energy": pd.Series([], dtype="float64"),
            "EthaneMixVolume": pd.Series([], dtype="float64"),
            "EthaneSpecVolume": pd.Series([], dtype="float64"),
            "PropaneMixVolume": pd.Series([], dtype="float64"),
            "PropaneSpecVolume": pd.Series([], dtype="float64"),
            "ButaneMixVolume": pd.Series([], dtype="float64"),
            "ButaneSpecVolume": pd.Series([], dtype="float64"),
            "PentaneMixVolume": pd.Series([], dtype="float64"),
            "PentaneSpecVolume": pd.Series([], dtype="float64"),
            "LiteMixVolume": pd.Series([], dtype="float64"),
            "ReportingFacilityID": pd.Series([], dtype="string"),
            "ReportingFacilityName": pd.Series([], dtype="string"),
            "OperatorBAID": pd.Series([], dtype="string"),
            "OperatorName": pd.Series([], dtype="string"),
            "WellLicenseNumber": pd.Series([], dtype="string"),
            "Field": pd.Series([], dtype="string"),
            "Pool": pd.Series([], dtype="string"),
            "Area": pd.Series([], dtype="string"),
            "Hours": pd.Series([], dtype="float64"),
            "DaysFromFirst": pd.Series([], dtype="int32"),
            "DataType": pd.Series([], dtype="string"),
            "ForecastMethod": pd.Series([], dtype="string"),
            "ForecastRSquared": pd.Series([], dtype="float64"),
        }
    )


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
        # For empty DataFrames, we need to provide schema explicitly
        # Create a Spark DataFrame with the same schema as the pandas DataFrame
        from pyspark.sql.types import (
            StructType,
            StructField,
            StringType,
            DoubleType,
            TimestampType,
            IntegerType,
            BooleanType,
        )

        spark_fields = []
        for col_name in pandas_df.columns:
            dtype = pandas_df[col_name].dtype
            if dtype == "object" or dtype == "string":
                spark_type = StringType()
            elif dtype == "float64":
                spark_type = DoubleType()
            elif dtype == "int64":
                spark_type = IntegerType()
            elif dtype == "bool":
                spark_type = BooleanType()
            elif dtype == "datetime64[ns]":
                spark_type = TimestampType()
            else:
                spark_type = StringType()  # Default fallback

            spark_fields.append(StructField(col_name, spark_type, True))

        schema = StructType(spark_fields)
        spark_df = spark.createDataFrame([], schema)
    else:
        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(pandas_df)

    # Write to table with overwrite mode to handle existing tables
    spark_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        table_name
    )
