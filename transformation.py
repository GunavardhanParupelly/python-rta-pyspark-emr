import logging.config
import os
import sys
import math
import json
import tempfile
import shutil
import logging
from urllib.parse import urlparse

from pyspark.sql import Window
from pyspark.sql.functions import (
      col, lit, upper, lower, trim, regexp_replace, regexp_extract,
      concat_ws, when, split, slice, to_date, year, month, size,
      sha2, concat, coalesce, length, expr, row_number, broadcast, levenshtein
)
from pyspark.sql.types import BooleanType

logging.config.fileConfig('Proporties/configeration/logging.config')

loggers = logging.getLogger('transformation')


# ---------- Helpers: IO (local + s3 awareness) ----------
def is_s3_path(path: str) -> bool:
      return isinstance(path, str) and path.startswith("s3://")

def list_local_parquet_size_bytes(path: str) -> int:
      total = 0
      for root, _, files in os.walk(path):
            for f in files:
                  fp = os.path.join(root, f)
                  try:
                        total += os.path.getsize(fp)
                  except Exception:
                        pass
      return total

def s3_total_size_bytes(s3_path: str, boto3_client) -> int:
      parsed = urlparse(s3_path)
      bucket = parsed.netloc
      prefix = parsed.path.lstrip('/')
      paginator = boto3_client.get_paginator('list_objects_v2')
      total = 0
      for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            if 'Contents' in page:
                  for obj in page['Contents']:
                        key = obj['Key']
                        if key.endswith('.parquet') or '/part-' in key or key.startswith(prefix):
                              total += obj['Size']
      return total

def s3_put_string(s3_path: str, content: str):
      parsed = urlparse(s3_path)
      bucket = parsed.netloc
      key = parsed.path.lstrip('/')
      try:
            import boto3
            s3 = boto3.client('s3')
            s3.put_object(Bucket=bucket, Key=key, Body=content.encode('utf-8'))
            return True
      except Exception as e:
            loggers.warning(f"Could not upload to S3 {s3_path}: {e}")
            return False


# ---------- ETL primitives (adapted) ----------
def parse_dates(df):
      df = df.withColumn("fromdate_clean", F.trim(F.regexp_replace(col("fromdate").cast("string"), r"[^\d/.\-]", ""))) \
               .withColumn("todate_clean", F.trim(F.regexp_replace(col("todate").cast("string"), r"[^\d/.\-]", ""))) \
               .withColumn("fromdate_clean", F.regexp_replace("fromdate_clean", r"[\.-]", "/")) \
               .withColumn("todate_clean", F.regexp_replace("todate_clean", r"[\.-]", "/")) \
               .withColumn("fromdate_parsed",
                     F.when(col("fromdate_clean").rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(col("fromdate_clean"), "dd/MM/yyyy"))
                     .when(col("fromdate_clean").rlike(r"^\d{2}/\d{2}/\d{2}$"), F.to_date(col("fromdate_clean"), "dd/MM/yy"))
                     .when(col("fromdate_clean").rlike(r"^\d{4}/\d{2}/\d{2}$"), F.to_date(col("fromdate_clean"), "yyyy/MM/dd"))
                     .otherwise(lit(None).cast("date"))
               ).withColumn("todate_parsed",
                     F.when(col("todate_clean").rlike(r"^\d{2}/\d{2}/\d{4}$"), F.to_date(col("todate_clean"), "dd/MM/yyyy"))
                     .when(col("todate_clean").rlike(r"^\d{2}/\d{2}/\d{2}$"), F.to_date(col("todate_clean"), "dd/MM/yy"))
                     .when(col("todate_clean").rlike(r"^\d{4}/\d{2}/\d{2}$"), F.to_date(col("todate_clean"), "yyyy/MM/dd"))
                     .otherwise(lit(None).cast("date"))
               ).drop("fromdate_clean", "todate_clean")
      return df


def clean_and_stage(df_raw, metrics, bad_root, output_format='parquet'):
      loggers.info("Starting clean_and_stage()")

      metrics.setdefault('steps', {})
      step = metrics['steps'].setdefault('clean_and_stage', {})

      before_cnt = df_raw.count()
      step['before'] = before_cnt

      # missing key
      df_missing_key = df_raw.filter(col("tempRegistrationNumber").isNull() | (trim(col("tempRegistrationNumber")) == ""))
      missing_key_cnt = df_missing_key.count()
      step['missing_tempRegistrationNumber'] = missing_key_cnt
      if missing_key_cnt > 0:
            out = os.path.join(bad_root, "missing_key")
            try:
                  if output_format == 'csv':
                        os.makedirs(out, exist_ok=True)
                        df_missing_key.coalesce(1).write.mode("overwrite").option("header", True).csv(out)
                  else:
                        df_missing_key.write.mode("overwrite").parquet(out)
            except Exception:
                  loggers.exception(f"Failed writing missing_key bad records to {out}")

      df = df_raw.filter(~(col("tempRegistrationNumber").isNull() | (trim(col("tempRegistrationNumber")) == "")))

      df_before_dedupe = df
      window_spec = Window.partitionBy("tempRegistrationNumber").orderBy(col("fromdate").desc_nulls_last())
      df = df.withColumn("row_num", F.row_number().over(window_spec)).filter(col("row_num") == 1).drop("row_num")
      after_dedupe_cnt = df.count()
      deduped = df_before_dedupe.count() - after_dedupe_cnt
      step['after_dedupe'] = after_dedupe_cnt
      step['deduplicated_rows'] = deduped
      if deduped > 0:
            dup_removed = df_before_dedupe.alias("a").join(df.alias("b"), on="tempRegistrationNumber", how="left_anti")
            dup_out = os.path.join(bad_root, "duplicates_removed")
            try:
                  if output_format == 'csv':
                        os.makedirs(dup_out, exist_ok=True)
                        dup_removed.coalesce(1).write.mode("overwrite").option("header", True).csv(dup_out)
                  else:
                        dup_removed.write.mode("overwrite").parquet(dup_out)
            except Exception:
                  loggers.exception(f"Failed writing duplicates_removed bad records to {dup_out}")

      df = df.withColumn(
            "OfficeCd",
            F.when(
                  col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*") & col("OfficeCd").isNull(),
                  col("fromdate")
            ).otherwise(col("OfficeCd"))
      ).withColumn(
            "fromdate",
            F.when(col("fromdate").cast("string").rlike("(?i)^(RTA|UNIT OFFICE|MVI|DTO|ZONAL|TRANSPORT).*"), lit(None).cast("string"))
            .otherwise(col("fromdate"))
      )
      df = df.withColumn("OfficeCd", F.when(col("OfficeCd").rlike("(?i)^(TS|TG)$"), lit(None)).otherwise(col("OfficeCd")))
      df = df.withColumn("OfficeCd", F.when(col("OfficeCd").isNull() & col("fromdate").isNotNull(), col("fromdate")).otherwise(col("OfficeCd")))

      df = df.withColumn("modelDescClean", F.trim(F.regexp_replace(col("modelDesc"), r"[^A-Za-z0-9\s\+\-\(\)\./]", " ")))
      df = df.withColumn("isTrailer", F.lower(col("modelDescClean")).rlike("trailer|trailor|tipper|tractor|tanker"))
      df = df.withColumn("isElectric", F.lower(col("modelDescClean")).rlike(r"\b(ev|bov|electric|hybrid)\b"))
      df = df.withColumn("modelWords", F.split(col("modelDescClean"), r"\s+")) \
               .withColumn("modelName", F.upper(F.when(col("isTrailer"), col("modelDescClean")).otherwise(col("modelWords")[0]))) \
               .withColumn("variant_words", F.slice(col("modelWords"), 2, F.size(col("modelWords")) - 1)) \
               .withColumn("variant", F.upper(F.when(col("isTrailer"), lit("TRAILER/TIPPER/TRACTOR/TANKER"))
                        .otherwise(F.trim(F.concat_ws(" ", col("variant_words")))))) \
               .withColumn("variant", F.when((col("variant") == "") | col("variant").isNull(), lit("UNKNOWN")).otherwise(col("variant")))

      df = parse_dates(df)

      invalid_dates = df.filter(col("fromdate_parsed").isNull())
      invalid_dates_cnt = invalid_dates.count()
      step['invalid_dates'] = invalid_dates_cnt
      if invalid_dates_cnt > 0:
            out = os.path.join(bad_root, "invalid_dates")
            try:
                  if output_format == 'csv':
                        os.makedirs(out, exist_ok=True)
                        invalid_dates.coalesce(1).write.mode("overwrite").option("header", True).csv(out)
                  else:
                        invalid_dates.write.mode("overwrite").parquet(out)
            except Exception:
                  loggers.exception(f"Failed writing invalid_dates bad records to {out}")

      df_stage = df.filter(col("fromdate_parsed").isNotNull())
      staged_cnt = df_stage.count()
      step['staged_rows'] = staged_cnt

      df_stage = df_stage.withColumn("fuel_clean", F.upper(
            F.when(col("fuel").rlike("BATTERY|ELECTRIC"), "ELECTRIC")
            .when(col("fuel").rlike("PETROL|GASOLINE"), "PETROL")
            .when(col("fuel").rlike("DIESEL"), "DIESEL")
            .when(col("fuel").rlike("CNG"), "CNG")
            .when(col("fuel").rlike("LPG"), "LPG")
            .otherwise("UNKNOWN")
      ))

      df_stage = df_stage.withColumn("makeYear_inferred", F.regexp_extract(col("modelDescClean"), r'(19\d{2}|20[0-2]\d)', 0)) \
               .withColumn("makeYear", F.when((col("makeYear").isNull()) | (col("makeYear") == "") | (col("makeYear") == "UNKNOWN"),
                                                            F.when(col("makeYear_inferred") != "", col("makeYear_inferred")).otherwise(lit("UNKNOWN"))
                                                         ).otherwise(col("makeYear"))).drop("makeYear_inferred")

      df_stage = df_stage.withColumn("year", F.year(col("fromdate_parsed"))).withColumn("month", F.month(col("fromdate_parsed")))

      loggers.info("Completed clean_and_stage()")
      return df_stage


def derive_emission_standard(df, metrics):
      loggers.info("Deriving emissionStandard")
      m = metrics.setdefault('steps', {}).setdefault('derive_emission_standard', {})
      before = df.count()
      m['before'] = before
      df = df.withColumn("emissionStandard", F.when(col("isElectric"), lit("ELECTRIC"))
                                 .otherwise(F.regexp_extract(col("modelDescClean"), r"(BS\s?III[AB]?|BS\s?IV|BS\s?V|BS\s?VI)", 0)))
      df = df.withColumn("emissionStandard", F.when(col("emissionStandard").isNull() | (col("emissionStandard") == ""), lit("UNKNOWN")).otherwise(col("emissionStandard")))
      after = df.count()
      m['after'] = after
      return df


def generate_surrogate_keys(df, metrics):
      loggers.info("Generating surrogate keys")
      m = metrics.setdefault('steps', {}).setdefault('generate_surrogate_keys', {})
      before = df.count()
      m['before'] = before
      df = df.withColumn("MAKE_YEAR_KEY", F.when(col("makeYear").isNull(), lit("UNKNOWN")).otherwise(col("makeYear")))
      df = df.withColumn("VEHICLE_KEY_SOURCE", F.concat_ws("|", F.lower(F.trim(col("modelName"))), F.lower(F.trim(col("variant"))), col("MAKE_YEAR_KEY")))
      df = df.withColumn("VEHICLE_ID", F.sha2(col("VEHICLE_KEY_SOURCE"), 256))
      df = df.withColumn("MANUFACTURER_KEY_SOURCE", F.lower(F.trim(col("makerName")))).withColumn("MANUFACTURER_ID", F.sha2(col("MANUFACTURER_KEY_SOURCE"), 256))
      df = df.withColumn("RTA_KEY_SOURCE", F.lower(F.trim(col("OfficeCd")))).withColumn("RTA_ID", F.sha2(col("RTA_KEY_SOURCE"), 256))
      after = df.count()
      m['after'] = after
      return df


def build_dimensions(df, metrics):
      loggers.info("Building dimensions")
      m = metrics.setdefault('steps', {}).setdefault('build_dimensions', {})
      dim_vehicle = df.select(
            col("VEHICLE_ID"),
            col("modelName").alias("MODEL_NAME"),
            col("variant").alias("VARIANT"),
            col("emissionStandard").alias("EMISSION_STANDARD"),
            col("fuel_clean").alias("FUEL"),
            coalesce(col("colour"), lit("UNKNOWN")).alias("COLOUR"),
            coalesce(col("vehicleClass"), lit("UNKNOWN")).alias("VEHICLE_CLASS"),
            col("makeYear").alias("MAKE_YEAR"),
            col("seatCapacity").cast("int").alias("SEAT_CAPACITY"),
            col("isElectric").alias("IS_ELECTRIC")
      ).dropDuplicates(["VEHICLE_ID"])

      dim_manufacturer = df.select(col("MANUFACTURER_ID"), col("makerName").alias("MAKER_NAME")).dropDuplicates(["MANUFACTURER_ID"])
      dim_rta = df.select(col("RTA_ID"), col("OfficeCd").alias("RTA_OFFICE_CODE")).dropDuplicates(["RTA_ID"]) \
                        .withColumn("RTA_REGION", lit(None).cast("string")) \
                        .withColumn("RTA_STATE", lit(None).cast("string")) \
                        .withColumn("RTA_CITY", lit(None).cast("string"))

      m['dim_vehicle_count'] = dim_vehicle.count()
      m['dim_manufacturer_count'] = dim_manufacturer.count()
      m['dim_rta_count'] = dim_rta.count()
      return dim_vehicle, dim_manufacturer, dim_rta


def fuzzy_vehicle_resolution(df, dim_vehicle, metrics, fuzzy_threshold=3):
      loggers.info("Running exact + fuzzy vehicle resolution")
      m = metrics.setdefault('steps', {}).setdefault('fuzzy_vehicle_resolution', {})
      df_for_match = df.select("tempRegistrationNumber", "VEHICLE_ID", "modelName", "variant", "MAKE_YEAR_KEY").dropDuplicates(["tempRegistrationNumber"])
      dv_lookup = dim_vehicle.select(
            col("VEHICLE_ID"),
            F.lower(F.trim(col("MODEL_NAME"))).alias("dv_model"),
            F.lower(F.trim(col("VARIANT"))).alias("dv_variant"),
            col("MAKE_YEAR").alias("dv_make_year")
      )

      joined_exact = df_for_match.alias("s").join(
            broadcast(dv_lookup).alias("dv"),
            (F.lower(F.trim(col("s.modelName"))) == col("dv.dv_model")) &
            (F.lower(F.trim(col("s.variant"))) == col("dv.dv_variant")) &
            (col("s.MAKE_YEAR_KEY") == col("dv.dv_make_year")),
            how="left"
      ).select(col("s.tempRegistrationNumber"), col("dv.VEHICLE_ID").alias("VEHICLE_ID_exact"))

      resolved_exact = joined_exact.filter(col("VEHICLE_ID_exact").isNotNull())
      exact_count = resolved_exact.count()
      m['exact_matches'] = exact_count

      all_reg = df_for_match.select("tempRegistrationNumber").distinct()
      resolved_keys = resolved_exact.select("tempRegistrationNumber").distinct()
      unresolved_keys = all_reg.join(resolved_keys, on="tempRegistrationNumber", how="left_anti")

      unresolved = unresolved_keys.join(df_for_match, on="tempRegistrationNumber", how="inner") \
            .withColumnRenamed("VEHICLE_ID", "ORIGINAL_VEHICLE_ID") \
            .withColumn("FUZZY_KEY", F.lower(F.trim(F.concat_ws(" ", col("modelName"), col("variant"))))) \
            .withColumn("BLOCK_KEY", col("FUZZY_KEY").substr(1, 2))

      # Rename the lookup VEHICLE_ID to avoid ambiguous column names when joining
      # (df_for_match also has VEHICLE_ID). Use DV_VEHICLE_ID to be explicit.
      dv_fuzzy = dv_lookup.withColumn("DV_FUZZY_KEY", F.lower(F.trim(F.concat_ws(" ", col("dv_model"), col("dv_variant"))))) \
                                    .withColumn("BLOCK_KEY", col("dv_model").substr(1, 2)) \
                                    .withColumnRenamed("VEHICLE_ID", "DV_VEHICLE_ID")

      fuzzy_candidates = unresolved.join(broadcast(dv_fuzzy), on="BLOCK_KEY", how="inner") \
                                                 .filter(col("MAKE_YEAR_KEY") == col("dv_make_year")) \
                                                 .withColumn("LEV_DIST", levenshtein(col("FUZZY_KEY"), col("DV_FUZZY_KEY"))) \
                                                 .filter(col("LEV_DIST") <= fuzzy_threshold)

      # Order by LEV_DIST then the lookup vehicle id (renamed to DV_VEHICLE_ID) to break ties
      fuzzy_window = Window.partitionBy("tempRegistrationNumber").orderBy(col("LEV_DIST").asc(), col("DV_VEHICLE_ID").asc())
      best_fuzzy = fuzzy_candidates.withColumn("rn", F.row_number().over(fuzzy_window)).filter(col("rn") == 1) \
            .select("tempRegistrationNumber", col("DV_VEHICLE_ID").alias("VEHICLE_ID_fuzzy"), col("LEV_DIST"))

      fuzzy_count = best_fuzzy.count()
      m['fuzzy_matches'] = fuzzy_count

      exact_sel = resolved_exact.select("tempRegistrationNumber", col("VEHICLE_ID_exact").alias("VEHICLE_ID_resolved"))
      fuzzy_sel = best_fuzzy.select("tempRegistrationNumber", col("VEHICLE_ID_fuzzy").alias("VEHICLE_ID_resolved"), col("LEV_DIST"))
      all_resolved = exact_sel.unionByName(fuzzy_sel, allowMissingColumns=True)
      all_resolved = all_resolved.withColumn("IS_FUZZY_MATCH", F.when(col("LEV_DIST").isNotNull(), lit(True)).otherwise(lit(False)))
      total_resolved = all_resolved.count()
      m['total_resolved'] = total_resolved
      return all_resolved


def assemble_fact_table(df, resolved_map, metrics):
      loggers.info("Assembling fact table")
      m = metrics.setdefault('steps', {}).setdefault('assemble_fact_table', {})
      before = df.count()
      m['before'] = before
      man_res = df.select("tempRegistrationNumber", "MANUFACTURER_ID").dropDuplicates(["tempRegistrationNumber"])
      fact_df = df.alias("s").join(resolved_map.alias("r"), on="tempRegistrationNumber", how="left") \
            .join(man_res.alias("m"), on="tempRegistrationNumber", how="left") \
            .select(
                  F.coalesce(col("r.VEHICLE_ID_resolved"), col("s.VEHICLE_ID")).alias("VEHICLE_ID"),
                  col("m.MANUFACTURER_ID").alias("MANUFACTURER_ID"),
                  col("s.RTA_ID").alias("RTA_ID"),
                  F.expr("CAST(date_format(s.fromdate_parsed, 'yyyyMMdd') AS INT)").alias("REGISTRATION_ISSUE_DATE_ID"),
                  F.expr("CAST(date_format(s.todate_parsed, 'yyyyMMdd') AS INT)").alias("REGISTRATION_EXPIRY_DATE_ID"),
                  F.year(col("s.fromdate_parsed")).alias("REGISTRATION_YEAR"),
                  col("s.vehicleClass").alias("TRANSPORT_TYPE"),
                  col("s.tempRegistrationNumber").alias("TEMP_REGISTRATION_NUMBER"),
                  col("s.slno").alias("SLNO"),
                  F.coalesce(col("r.IS_FUZZY_MATCH"), lit(False)).alias("IS_FUZZY_MATCH"),
                  col("s.colour").alias("COLOUR"),
                  col("s.fuel_clean").alias("FUEL_TYPE"),
                  col("s.modelName").alias("MODEL_NAME"),
                  col("s.emissionStandard").alias("EMISSION_STANDARD")
            ).filter(col("REGISTRATION_ISSUE_DATE_ID").isNotNull())
      after = fact_df.count()
      m['after'] = after
      return fact_df


# ---------- DYNAMIC COALESCE ----------
def dynamic_coalesce_write(spark, df, final_output_path, partition_by=None, temp_dir=None, target_mb=128.0, write_mode='overwrite', output_format='parquet'):
      loggers.info(f"Starting dynamic coalesce write to {final_output_path} (format={output_format})")

      # If CSV output is requested, avoid the parquet temp-stage which triggers
      # native Hadoop calls on Windows. Write CSV directly (coalesced to 1 file).
      if output_format == 'csv':
            try:
                  df.coalesce(1).write.mode(write_mode).option("header", True).csv(final_output_path)
                  return 1, 0
            except Exception as e:
                  loggers.exception(f"Failed to write CSV final output: {e}")
                  raise

      # default parquet flow (write temp parquet then coalesce)
      local_temp = False
      if temp_dir:
            temp_path = os.path.join(temp_dir, "_tmp_coalesce")
      else:
            tmp = tempfile.mkdtemp(prefix="etl_coalesce_")
            temp_path = tmp
            local_temp = True

      if partition_by:
            if isinstance(partition_by, (list, tuple)):
                  df.write.mode(write_mode).partitionBy(*partition_by).parquet(temp_path)
            else:
                  df.write.mode(write_mode).partitionBy(partition_by).parquet(temp_path)
      else:
            df.write.mode(write_mode).parquet(temp_path)

      total_bytes = 0
      if is_s3_path(temp_path):
            try:
                  import boto3
                  total_bytes = s3_total_size_bytes(temp_path, boto3.client('s3'))
            except Exception as e:
                  loggers.warning(f"  Could not compute S3 temp size: {e}")
                  total_bytes = 0
      else:
            total_bytes = list_local_parquet_size_bytes(temp_path)

      target_bytes = target_mb * 1024 * 1024
      if total_bytes <= 0:
            optimal_parts = 1
      else:
            optimal_parts = max(1, math.ceil(total_bytes / target_bytes))
            optimal_parts = min(optimal_parts, 200)

      df_staged = spark.read.parquet(temp_path)
      # write final parquet
      if partition_by:
            df_staged.coalesce(optimal_parts).write.mode(write_mode).partitionBy(partition_by).parquet(final_output_path)
      else:
            df_staged.coalesce(optimal_parts).write.mode(write_mode).parquet(final_output_path)

      if local_temp:
            try:
                  shutil.rmtree(temp_path)
            except Exception:
                  pass
      return optimal_parts, total_bytes


def write_metrics_summary(metrics: dict, output_root: str):
      metrics_path = os.path.join(output_root, "metrics_summary.json")
      content = json.dumps(metrics, indent=2, default=str)
      if is_s3_path(metrics_path):
            ok = s3_put_string(metrics_path, content)
            if not ok:
                  tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
                  tmp.write(content.encode("utf-8"))
                  tmp.close()
                  loggers.warning(f"Failed to write metrics to S3 directly; metrics saved locally to {tmp.name}")
      else:
            os.makedirs(output_root, exist_ok=True)
            with open(metrics_path, "w", encoding="utf-8") as f:
                  f.write(content)


def run_rta_etl_on_df(df_raw, spark, output_root, mode='overwrite', target_mb=128.0, fuzzy_threshold=3, temp_dir=None, run_id=None, output_format='parquet'):
      metrics = {"run_id": run_id, "config": {"output_root": output_root, "mode": mode, "target_mb": target_mb}}
      bad_root = os.path.join(output_root, "bad_records")
      try:
            df_stage = clean_and_stage(df_raw, metrics, bad_root, output_format=output_format)
            df_stage = derive_emission_standard(df_stage, metrics)
            df_stage = generate_surrogate_keys(df_stage, metrics)
            dim_vehicle, dim_manufacturer, dim_rta = build_dimensions(df_stage, metrics)
            resolved_map = fuzzy_vehicle_resolution(df_stage, dim_vehicle, metrics, fuzzy_threshold=fuzzy_threshold)
            fact_df = assemble_fact_table(df_stage, resolved_map, metrics)

            # write dims (respect output_format)
            os.makedirs(output_root, exist_ok=True)
            if output_format == 'csv':
                  dim_vehicle.write.mode(mode).option("header", True).csv(os.path.join(output_root, "gold_dim_vehicle"))
                  dim_manufacturer.write.mode(mode).option("header", True).csv(os.path.join(output_root, "gold_dim_manufacturer"))
                  dim_rta.write.mode(mode).option("header", True).csv(os.path.join(output_root, "gold_dim_rta"))
            else:
                  dim_vehicle.write.mode(mode).parquet(os.path.join(output_root, "gold_dim_vehicle"))
                  dim_manufacturer.write.mode(mode).parquet(os.path.join(output_root, "gold_dim_manufacturer"))
                  dim_rta.write.mode(mode).parquet(os.path.join(output_root, "gold_dim_rta"))

            parts, total_bytes = dynamic_coalesce_write(spark, fact_df, os.path.join(output_root, "gold_fact_registrations"), partition_by="REGISTRATION_YEAR", temp_dir=temp_dir, target_mb=target_mb, write_mode=mode, output_format=output_format)
            metrics['writes'] = {'fact': {'path': os.path.join(output_root, "gold_fact_registrations"), 'coalesced_parts': parts, 'bytes': total_bytes}}

            metrics['final_counts'] = {
                  'dim_vehicle': dim_vehicle.count(),
                  'dim_manufacturer': dim_manufacturer.count(),
                  'dim_rta': dim_rta.count(),
                  'fact_rows': fact_df.count()
            }
            write_metrics_summary(metrics, output_root)
            loggers.info("RTA ETL on dataframe completed successfully.")
            return metrics
      except Exception as e:
            loggers.exception(f"ETL failed: {e}")
            metrics['error'] = str(e)
            try:
                  write_metrics_summary(metrics, output_root)
            except Exception:
                  pass
            raise
