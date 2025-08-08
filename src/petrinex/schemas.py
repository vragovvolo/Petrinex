from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    IntegerType,
)

CONV_BRONZE_SCHEMA = StructType(
    [
        StructField("ProductionMonth", StringType(), nullable=False),
        StructField("OperatorBAID", StringType(), nullable=False),
        StructField("OperatorName", StringType(), nullable=True),
        StructField("ReportingFacilityID", StringType(), nullable=False),
        StructField("ReportingFacilityProvinceState", StringType(), nullable=True),
        StructField("ReportingFacilityType", StringType(), nullable=True),
        StructField("ReportingFacilityIdentifier", StringType(), nullable=True),
        StructField("ReportingFacilityName", StringType(), nullable=True),
        StructField("ReportingFacilitySubType", StringType(), nullable=True),
        StructField("ReportingFacilitySubTypeDesc", StringType(), nullable=True),
        StructField("ReportingFacilityLocation", StringType(), nullable=True),
        StructField("FacilityLegalSubdivision", StringType(), nullable=True),
        StructField("FacilitySection", StringType(), nullable=True),
        StructField("FacilityTownship", StringType(), nullable=True),
        StructField("FacilityRange", StringType(), nullable=True),
        StructField("FacilityMeridian", StringType(), nullable=True),
        StructField("SubmissionDate", StringType(), nullable=True),
        StructField("ActivityID", StringType(), nullable=True),
        StructField("ProductID", StringType(), nullable=True),
        StructField("FromToID", StringType(), nullable=True),
        StructField("FromToIDProvinceState", StringType(), nullable=True),
        StructField("FromToIDType", StringType(), nullable=True),
        StructField("FromToIDIdentifier", StringType(), nullable=True),
        StructField("Volume", DoubleType(), nullable=True),
        StructField("Energy", DoubleType(), nullable=True),
        StructField("Hours", DoubleType(), nullable=True),
        StructField("CCICode", StringType(), nullable=True),
        StructField("ProrationProduct", StringType(), nullable=True),
        StructField("ProrationFactor", DoubleType(), nullable=True),
        StructField("Heat", DoubleType(), nullable=True),
    ]
)

NGL_BRONZE_SCHEMA = StructType(
    [
        StructField("ReportingFacilityID", StringType(), nullable=False),
        StructField("ReportingFacilityName", StringType(), nullable=True),
        StructField("OperatorBAID", StringType(), nullable=False),
        StructField("OperatorName", StringType(), nullable=True),
        StructField("ProductionMonth", StringType(), nullable=False),
        StructField("WellID", StringType(), nullable=False),
        StructField("WellLicenseNumber", StringType(), nullable=True),
        StructField("Field", StringType(), nullable=True),
        StructField("Pool", StringType(), nullable=True),
        StructField("Area", StringType(), nullable=True),
        StructField("Hours", DoubleType(), nullable=True),
        StructField("GasProduction", DoubleType(), nullable=True),
        StructField("OilProduction", DoubleType(), nullable=True),
        StructField("CondensateProduction", DoubleType(), nullable=True),
        StructField("WaterProduction", DoubleType(), nullable=True),
        StructField("ResidueGasVolume", DoubleType(), nullable=True),
        StructField("Energy", DoubleType(), nullable=True),
        StructField("EthaneMixVolume", DoubleType(), nullable=True),
        StructField("EthaneSpecVolume", DoubleType(), nullable=True),
        StructField("PropaneMixVolume", DoubleType(), nullable=True),
        StructField("PropaneSpecVolume", DoubleType(), nullable=True),
        StructField("ButaneMixVolume", DoubleType(), nullable=True),
        StructField("ButaneSpecVolume", DoubleType(), nullable=True),
        StructField("PentaneMixVolume", DoubleType(), nullable=True),
        StructField("PentaneSpecVolume", DoubleType(), nullable=True),
        StructField("LiteMixVolume", DoubleType(), nullable=True),
    ]
)

CONV_SILVER_SCHEMA = StructType(
    [
        StructField("ProductionMonth", TimestampType(), nullable=False),
        StructField("OperatorBAID", StringType(), nullable=False),
        StructField("OperatorName", StringType(), nullable=True),
        StructField("ReportingFacilityID", StringType(), nullable=False),
        StructField("ReportingFacilityProvinceState", StringType(), nullable=True),
        StructField("ReportingFacilityType", StringType(), nullable=True),
        StructField("ReportingFacilityIdentifier", StringType(), nullable=True),
        StructField("ReportingFacilityName", StringType(), nullable=True),
        StructField("ReportingFacilitySubType", StringType(), nullable=True),
        StructField("ReportingFacilitySubTypeDesc", StringType(), nullable=True),
        StructField("ReportingFacilityLocation", StringType(), nullable=True),
        StructField("FacilityLegalSubdivision", StringType(), nullable=True),
        StructField("FacilitySection", StringType(), nullable=True),
        StructField("FacilityTownship", StringType(), nullable=True),
        StructField("FacilityRange", StringType(), nullable=True),
        StructField("FacilityMeridian", StringType(), nullable=True),
        StructField("SubmissionDate", TimestampType(), nullable=True),
        StructField("ActivityID", StringType(), nullable=True),
        StructField("ProductID", StringType(), nullable=True),
        StructField("FromToID", StringType(), nullable=True),
        StructField("FromToIDProvinceState", StringType(), nullable=True),
        StructField("FromToIDType", StringType(), nullable=True),
        StructField("FromToIDIdentifier", StringType(), nullable=True),
        StructField("Volume", DoubleType(), nullable=True),
        StructField("Energy", DoubleType(), nullable=True),
        StructField("Hours", DoubleType(), nullable=True),
        StructField("CCICode", StringType(), nullable=True),
        StructField("ProrationProduct", StringType(), nullable=True),
        StructField("ProrationFactor", DoubleType(), nullable=True),
        StructField("Heat", DoubleType(), nullable=True),
    ]
)

# Calendar Silver: Monthly calendar data with cumulative production
NGL_CALENDAR_SCHEMA = StructType(
    [
        StructField("ReportingFacilityID", StringType(), nullable=False),
        StructField("ReportingFacilityName", StringType(), nullable=True),
        StructField("OperatorBAID", StringType(), nullable=False),
        StructField("OperatorName", StringType(), nullable=True),
        StructField("ProductionMonth", TimestampType(), nullable=False),
        StructField("WellID", StringType(), nullable=False),
        StructField("WellLicenseNumber", StringType(), nullable=True),
        StructField("Field", StringType(), nullable=True),
        StructField("Pool", StringType(), nullable=True),
        StructField("Area", StringType(), nullable=True),
        StructField("Hours", DoubleType(), nullable=True),
        # Monthly calendar volumes
        StructField("GasProduction", DoubleType(), nullable=True),
        StructField("OilProduction", DoubleType(), nullable=True),
        StructField("CondensateProduction", DoubleType(), nullable=True),
        StructField("WaterProduction", DoubleType(), nullable=True),
        StructField("ResidueGasVolume", DoubleType(), nullable=True),
        StructField("Energy", DoubleType(), nullable=True),
        StructField("EthaneMixVolume", DoubleType(), nullable=True),
        StructField("EthaneSpecVolume", DoubleType(), nullable=True),
        StructField("PropaneMixVolume", DoubleType(), nullable=True),
        StructField("PropaneSpecVolume", DoubleType(), nullable=True),
        StructField("ButaneMixVolume", DoubleType(), nullable=True),
        StructField("ButaneSpecVolume", DoubleType(), nullable=True),
        StructField("PentaneMixVolume", DoubleType(), nullable=True),
        StructField("PentaneSpecVolume", DoubleType(), nullable=True),
        StructField("LiteMixVolume", DoubleType(), nullable=True),
        # Cumulative production from start of well
        StructField("GasProductionCumulative", DoubleType(), nullable=True),
        StructField("OilProductionCumulative", DoubleType(), nullable=True),
        StructField("CondensateProductionCumulative", DoubleType(), nullable=True),
        StructField("WaterProductionCumulative", DoubleType(), nullable=True),
        StructField("HoursCumulative", DoubleType(), nullable=True),
        StructField("DaysFromFirst", IntegerType(), nullable=True),
    ]
)

# Normalized Silver: 730-hour normalized periods with rates and estimated dates
NGL_NORMALIZED_SCHEMA = StructType(
    [
        StructField("ReportingFacilityID", StringType(), nullable=False),
        StructField("ReportingFacilityName", StringType(), nullable=True),
        StructField("OperatorBAID", StringType(), nullable=False),
        StructField("OperatorName", StringType(), nullable=True),
        StructField("WellID", StringType(), nullable=False),
        StructField("WellLicenseNumber", StringType(), nullable=True),
        StructField("Field", StringType(), nullable=True),
        StructField("Pool", StringType(), nullable=True),
        StructField("Area", StringType(), nullable=True),
        # Normalized period info
        StructField(
            "NormalizedPeriod", IntegerType(), nullable=False
        ),  # 1, 2, 3... representing 730-hour periods
        StructField(
            "PeriodStartDate", TimestampType(), nullable=True
        ),  # Estimated start date of this 730-hour period
        StructField(
            "PeriodEndDate", TimestampType(), nullable=True
        ),  # Estimated end date of this 730-hour period
        StructField(
            "HoursInPeriod", DoubleType(), nullable=False
        ),  # Should be 730 for complete periods
        # Production for this 730-hour period (differential of cumulative)
        StructField("GasProduction", DoubleType(), nullable=True),
        StructField("OilProduction", DoubleType(), nullable=True),
        StructField("CondensateProduction", DoubleType(), nullable=True),
        StructField("WaterProduction", DoubleType(), nullable=True),
        # Daily rates for this period
        StructField("GasProductionDailyRate", DoubleType(), nullable=True),
        StructField("OilProductionDailyRate", DoubleType(), nullable=True),
        StructField("CondensateProductionDailyRate", DoubleType(), nullable=True),
        StructField("WaterProductionDailyRate", DoubleType(), nullable=True),
        # Cumulative from start of well
        StructField("GasProductionCumulative", DoubleType(), nullable=True),
        StructField("OilProductionCumulative", DoubleType(), nullable=True),
        StructField("CondensateProductionCumulative", DoubleType(), nullable=True),
        StructField("WaterProductionCumulative", DoubleType(), nullable=True),
        StructField("HoursCumulative", DoubleType(), nullable=True),
        StructField(
            "DaysFromFirstProducing", IntegerType(), nullable=True
        ),  # Days from first production
    ]
)

# Legacy silver schema (kept for compatibility)
NGL_SILVER_SCHEMA = NGL_CALENDAR_SCHEMA


def get_schema(table_code: str, layer: str = "bronze") -> StructType:
    """
    Get the appropriate schema for a table type and layer.

    Args:
        table_code: Type of table ('vol', 'ngl', 'ngl_calendar', 'ngl_normalized')
        layer: Data layer ('bronze' or 'silver')

    Returns:
        Spark StructType schema
    """
    schema_map = {
        "vol": {
            "bronze": CONV_BRONZE_SCHEMA,
            "silver": CONV_SILVER_SCHEMA,
        },
        "ngl": {
            "bronze": NGL_BRONZE_SCHEMA,
            "silver": NGL_SILVER_SCHEMA,  # This is now the calendar schema
        },
        "ngl_calendar": {
            "silver": NGL_CALENDAR_SCHEMA,
        },
        "ngl_normalized": {
            "silver": NGL_NORMALIZED_SCHEMA,
        },
    }

    if table_code not in schema_map:
        raise ValueError(f"Unknown table type: {table_code}")
    if layer not in schema_map[table_code]:
        raise ValueError(f"Unknown layer: {layer}")

    return schema_map[table_code][layer]
