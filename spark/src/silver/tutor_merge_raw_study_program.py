import logging
import argparse
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("silver.study_program")

# Bronze source
BRONZE_DATABASE     = "bronze"
BRONZE_TABLE        = "tutor_study_program"
S3_BRONZE_PATH      = "s3a://lakehouse/warehouse/bronze.db/tutor/study_program"

# Silver target
SILVER_DATABASE     = "silver"
SILVER_TABLE        = "tutor_study_program"
S3_SILVER_PATH      = "s3a://lakehouse/warehouse/silver.db/tutor/study_program"

# Merge key
PRIMARY_KEY         = "id"

# Columns to exclude from silver (bronze metadata cols)
BRONZE_META_COLS    = {
    "_op", "_ts_ms", "_kafka_topic", "_kafka_partition",
    "_kafka_offset", "_kafka_timestamp", "_ingested_at", "_event_date"
}


def parse_args():
    parser = argparse.ArgumentParser(description="Silver UPSERT – study_program")
    parser.add_argument(
        "--start-date",
        type=str,
        default='2026-04-02',
        help="Ngày bắt đầu đọc Bronze (YYYY-MM-DD). Mặc định: hôm qua",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default='2026-04-03',
        help="Ngày kết thúc đọc Bronze (YYYY-MM-DD, inclusive). Mặc định: hôm nay",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("silver_study_program")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created – app: %s", spark.sparkContext.appName)
    return spark


def ensure_silver_table(spark: SparkSession) -> None:
    """Tạo Silver database + table nếu chưa tồn tại."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {SILVER_DATABASE}.{SILVER_TABLE} (
            id                      STRING         NOT NULL,
            name                    STRING,
            max_student_in_class    INT,
            subject_id              STRING,
            created_by              STRING,
            created_at              BIGINT,
            updated_by              STRING,
            updated_at              BIGINT,
            code                    STRING,
            type                    STRING,
            product_type_code       STRING,
            group_type              STRING,
            sessions_per_week       INT,
            learning_model          STRING,
            schedule_type           STRING,
            _silver_updated_at      TIMESTAMP
        )
        USING DELTA
        LOCATION '{S3_SILVER_PATH}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'description' = 'Silver raw (UPSERT) for study_program'
        )
    """)
    logger.info("Silver table '%s.%s' ensured.", SILVER_DATABASE, SILVER_TABLE)


def read_bronze(spark: SparkSession, start_date: str, end_date: str):
    """
    Đọc Bronze theo partition _event_date trong khoảng [start_date, end_date].
    Với mỗi primary key, chỉ giữ lại bản ghi có _ts_ms lớn nhất (latest state).
    Loại bỏ các bản ghi op='d' (delete) — silver raw không lưu deletes.
    """
    logger.info("Reading Bronze: _event_date BETWEEN '%s' AND '%s'", start_date, end_date)

    bronze_df = (
        spark.read
        .format("delta")
        .load(S3_BRONZE_PATH)
        .filter(
            (F.col("_event_date") >= start_date) &
            (F.col("_event_date") <= end_date)
        )
        .filter(F.col("_op") != "d")          # bỏ delete events
        .filter(F.col(PRIMARY_KEY).isNotNull())
    )

    # Lấy bản ghi mới nhất theo primary key trong window đọc
    from pyspark.sql.window import Window
    window = Window.partitionBy(PRIMARY_KEY).orderBy(F.col("_ts_ms").desc())

    latest_df = (
        bronze_df
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    # Chỉ giữ các cột business (bỏ bronze metadata)
    business_cols = [c for c in latest_df.columns if c not in BRONZE_META_COLS]
    latest_df = latest_df.select(*business_cols)

    count = latest_df.count()
    logger.info("Bronze records to upsert: %d", count)
    return latest_df


def upsert_to_silver(spark: SparkSession, source_df):
    """
    MERGE (UPSERT) source_df vào Silver Delta table theo PRIMARY_KEY.
    Thêm _silver_updated_at = current_timestamp() cho mỗi dòng được insert/update.
    """
    source_df = source_df.withColumn("_silver_updated_at", F.current_timestamp())

    silver_table = DeltaTable.forPath(spark, S3_SILVER_PATH)

    merge_condition = f"silver.{PRIMARY_KEY} = source.{PRIMARY_KEY}"

    # Build update map (tất cả cột trừ primary key)
    update_cols = [c for c in source_df.columns if c != PRIMARY_KEY]
    update_map  = {col: f"source.{col}" for col in update_cols}

    # Insert map (tất cả cột)
    insert_map  = {col: f"source.{col}" for col in source_df.columns}

    (
        silver_table.alias("silver")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
    logger.info("MERGE into '%s.%s' completed.", SILVER_DATABASE, SILVER_TABLE)


def main():
    args = parse_args()

    today     = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    start_date = args.start_date or str(yesterday)
    end_date   = args.end_date   or str(today)

    logger.info("=== Silver pipeline study_program | %s → %s ===", start_date, end_date)

    spark = build_spark()
    ensure_silver_table(spark)

    source_df = read_bronze(spark, start_date, end_date)

    if source_df.rdd.isEmpty():
        logger.info("No new data in Bronze for the given window. Exiting.")
        spark.stop()
        return

    upsert_to_silver(spark, source_df)

    logger.info("=== Pipeline finished successfully ===")
    spark.stop()


if __name__ == "__main__":
    main()