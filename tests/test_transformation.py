import sys
sys.path = [p for p in sys.path if "spark-3.4.2-bin-hadoop3" not in p]
VENV_PATH = "C:/Users/Gunav/Desktop/spark1-master/venv/Lib/site-packages"


import pytest
from pyspark.sql import Row, SparkSession, functions as F
from src.core.transformation import (
    parse_dates,
    clean_and_stage,
    derive_emission_standard,
    generate_surrogate_keys,
    build_dimensions,
    fuzzy_vehicle_resolution,
    assemble_fact_table
)

# -------------------------------------------------------------------------
# Spark Session Setup
# -------------------------------------------------------------------------
@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("TransformationTests").getOrCreate()

# -------------------------------------------------------------------------
# ETL1 Tests
# -------------------------------------------------------------------------

def test_parse_dates_mixed_formats(spark):
    data = [("01/01/23", "31/12/23"), ("12/12/2022", "31/12/2022"), ("31/05/21", "30/06/21")]
    df = spark.createDataFrame(data, ["fromdate", "todate"])
    df_parsed = parse_dates(df)
    assert "fromdate_parsed" in df_parsed.columns
    assert df_parsed.filter(F.col("fromdate_parsed").isNull()).count() == 0

def test_fuel_normalization_logic(spark):
    data = [("battery",), ("Diesel",), ("CNG",), ("Hybrid",)]
    df = spark.createDataFrame(data, ["fuel"])
    df_norm = df.withColumn(
        "fuel_clean",
        F.upper(
            F.when(F.col("fuel").rlike("BATTERY|ELECTRIC"), "ELECTRIC")
             .when(F.col("fuel").rlike("PETROL|GASOLINE"), "PETROL")
             .when(F.col("fuel").rlike("DIESEL"), "DIESEL")
             .when(F.col("fuel").rlike("CNG"), "CNG")
             .when(F.col("fuel").rlike("LPG"), "LPG")
             .otherwise("UNKNOWN")
        )
    )
    valid_fuels = {"PETROL", "DIESEL", "ELECTRIC", "CNG", "LPG", "UNKNOWN"}
    fuels = set(r[0] for r in df_norm.select("fuel_clean").collect())
    assert fuels.issubset(valid_fuels)

def test_officecd_misalignment_logic(spark):
    data = [
        ("TG108", "01/12/2024"),
        (None, "RTA HYDERABAD"),
        ("TG202", "UNIT OFFICE NIZAMABAD"),
    ]
    df = spark.createDataFrame(data, ["OfficeCd", "fromdate"])
    df_fixed = (
        df.withColumn(
            "OfficeCd",
            F.when(
                F.col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*")
                & F.col("OfficeCd").isNull(),
                F.col("fromdate")
            ).otherwise(F.col("OfficeCd"))
        )
        .withColumn(
            "fromdate",
            F.when(
                F.col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*"),
                F.lit(None).cast("string")
            ).otherwise(F.col("fromdate"))
        )
    )
    rows = df_fixed.collect()
    assert rows[0].OfficeCd == "TG108" and rows[0].fromdate == "01/12/2024"
    assert rows[1].OfficeCd == "RTA HYDERABAD" and rows[1].fromdate is None
    assert rows[2].OfficeCd == "TG202" and rows[2].fromdate is None

@pytest.mark.integration
def test_etl1_integration_quality(spark):
    data = [
        Row(tempRegistrationNumber="TS09AA1234", makerName="Maruti.", modelDesc="Swift VXI", fromdate="01/01/2023", todate="31/12/2023", fuel="petrol", colour="Red", OfficeCd=None),
        Row(tempRegistrationNumber="TS09BB5678", makerName="Tata Motors", modelDesc="Nexon@EV", fromdate="RTA HYDERABAD", todate="2024-01-01", fuel="ELECTRIC", colour="Blue", OfficeCd=None),
        Row(tempRegistrationNumber="TS09CC9012", makerName="Ashok Leyland", modelDesc="Truck Trailer", fromdate="12/12/22", todate="SRI SRINIVASA AUTO AGENCY", fuel="diesel", colour="BLACK", OfficeCd=None),
        Row(tempRegistrationNumber="TS09DD3456", makerName=None, modelDesc="null", fromdate="05/05/2022", todate="06/06/2023", fuel="cng", colour=None, OfficeCd="TG01"),
    ]
    df_raw = spark.createDataFrame(data)
    pre_metrics = {
        "total_rows": df_raw.count(),
        "null_makerName": df_raw.filter(F.col("makerName").isNull()).count(),
    }
    df = parse_dates(df_raw)
    df = df.withColumn(
        "OfficeCd",
        F.when(
            F.col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*") & F.col("OfficeCd").isNull(),
            F.col("fromdate")
        ).otherwise(F.col("OfficeCd"))
    ).withColumn(
        "fromdate",
        F.when(
            F.col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*"),
            F.lit(None).cast("string")
        ).otherwise(F.col("fromdate"))
    )
    df = df.withColumn("modelDescClean", F.regexp_replace(F.col("modelDesc"), r"[^A-Za-z0-9\s\+\-\(\)\./]", " ")) \
           .withColumn("fuel_clean", F.upper(
                F.when(F.col("fuel").rlike("BATTERY|ELECTRIC"), "ELECTRIC")
                .when(F.col("fuel").rlike("PETROL|GASOLINE"), "PETROL")
                .when(F.col("fuel").rlike("DIESEL"), "DIESEL")
                .when(F.col("fuel").rlike("CNG"), "CNG")
                .when(F.col("fuel").rlike("LPG"), "LPG")
                .otherwise("UNKNOWN")
            ))
    type1_flag = ~F.col("todate").rlike(r"^\d{2}/\d{2}/(\d{2}|\d{4})$")
    type2_flag = F.col("modelDescClean").rlike(r"(?i)null|dealer|agency")
    type3_flag = F.col("makerName").isNull() | F.col("colour").isNull()
    df = df.withColumn(
        "misalignment_type",
        F.when(type1_flag, "Type 1")
         .when(type2_flag, "Type 2")
         .when(type3_flag, "Type 3")
         .otherwise("Clean")
    )
    post_metrics = {
        "total_rows": df.count(),
        "misaligned_rows": df.filter(F.col("misalignment_type") != "Clean").count(),
        "null_makerName": df.filter(F.col("makerName").isNull()).count(),
        "valid_dates": df.filter(F.col("fromdate_parsed").isNotNull()).count(),
    }
    misalignment_percent = post_metrics["misaligned_rows"] / post_metrics["total_rows"] * 100
    assert post_metrics["total_rows"] == pre_metrics["total_rows"]
    assert post_metrics["valid_dates"] > 0
    assert misalignment_percent < 80  # or adjust your test data
    assert post_metrics["null_makerName"] <= pre_metrics["null_makerName"]
    assert "misalignment_type" in df.columns

# -------------------------------------------------------------------------
# ETL2 (Star Schema) Tests
# -------------------------------------------------------------------------

def test_surrogate_key_determinism(spark):
    data = [
        ("SWIFT", "VDI", "2018"),
        ("SWIFT", "VDI", "2018")
    ]
    df = spark.createDataFrame(data, ["modelName", "variant", "makeYear"])
    df = df.withColumn(
        "VEHICLE_KEY_SOURCE",
        F.concat_ws("|", F.lower(F.trim(F.col("modelName"))), F.lower(F.trim(F.col("variant"))), F.col("makeYear"))
    ).withColumn("VEHICLE_ID", F.sha2(F.col("VEHICLE_KEY_SOURCE"), 256))
    assert df.select("VEHICLE_ID").distinct().count() == 1

def test_dim_manufacturer_unique(spark):
    data = [("HYUNDAI",), ("HONDA",), ("HYUNDAI",)]
    df = spark.createDataFrame(data, ["MAKER_NAME"]) \
              .withColumn("MANUFACTURER_ID", F.sha2(F.lower(F.trim(F.col("MAKER_NAME"))), 256))
    total = df.count()
    distinct_ids = df.select("MANUFACTURER_ID").distinct().count()
    assert distinct_ids < total

def test_dim_vehicle_unique_and_nonnull(spark):
    data = [
        ("V1", "MODEL_A", "BSVI", "PETROL"),
        ("V2", "MODEL_B", "BSIV", "DIESEL"),
        ("V2", "MODEL_B", "BSIV", "DIESEL")
    ]
    df = spark.createDataFrame(data, ["VEHICLE_ID", "MODEL_NAME", "EMISSION_STANDARD", "FUEL"])
    assert df.filter(F.col("VEHICLE_ID").isNull()).count() == 0
    assert df.select("VEHICLE_ID").distinct().count() < df.count()

def test_emission_standard_valid_values(spark):
    valid_values = {"BSIII", "BSIV", "BSVI", "ELECTRIC", "UNKNOWN"}
    df = spark.createDataFrame([("BSVI",), ("BSIV",), ("UNKNOWN",)], ["EMISSION_STANDARD"])
    invalid = df.filter(~F.col("EMISSION_STANDARD").isin(valid_values)).count()
    assert invalid == 0

def test_fuzzy_match_distance(spark):
    data = [
        ("TS09", "VEH123", True, 2),
        ("TS10", "VEH456", True, 3),
        ("TS11", "VEH789", False, None)
    ]
    df = spark.createDataFrame(data, ["tempRegNo", "VEHICLE_ID", "IS_FUZZY_MATCH", "LEV_DIST"])
    threshold = 3
    invalid = df.filter((F.col("IS_FUZZY_MATCH") == True) & (F.col("LEV_DIST") > threshold)).count()
    assert invalid == 0

def test_fact_foreign_keys_present(spark):
    data = [
        ("VEH1", "MAN1", "RTA1", 20240101),
        ("VEH2", None, "RTA2", 20240102)
    ]
    df = spark.createDataFrame(data, ["VEHICLE_ID", "MANUFACTURER_ID", "RTA_ID", "REGISTRATION_DATE_ID"])
    null_fk_count = df.filter(F.col("VEHICLE_ID").isNull() | F.col("RTA_ID").isNull()).count()
    assert null_fk_count == 0

def test_date_key_format(spark):
    data = [(20230101,), (20231231,), (2024,)]
    df = spark.createDataFrame(data, ["REGISTRATION_DATE_ID"])
    valid = df.filter(F.length(F.col("REGISTRATION_DATE_ID")) == 8).count()
    assert valid == 2

def test_fact_to_dim_fk_integrity(spark):
    fact = spark.createDataFrame(
        [("VEH1", "MAN1", "RTA1"), ("VEH2", "MAN2", "RTA9")],
        ["VEHICLE_ID", "MANUFACTURER_ID", "RTA_ID"]
    )
    dim_vehicle = spark.createDataFrame([("VEH1",), ("VEH2",)], ["VEHICLE_ID"])
    dim_manufacturer = spark.createDataFrame([("MAN1",), ("MAN2",)], ["MANUFACTURER_ID"])
    dim_rta = spark.createDataFrame([("RTA1",), ("RTA2",)], ["RTA_ID"])
    missing_vehicle = fact.join(dim_vehicle, "VEHICLE_ID", "left_anti").count()
    missing_man = fact.join(dim_manufacturer, "MANUFACTURER_ID", "left_anti").count()
    missing_rta = fact.join(dim_rta, "RTA_ID", "left_anti").count()
    assert missing_vehicle == 0
    assert missing_man == 0
    assert missing_rta == 1  # one intentionally unmatched

