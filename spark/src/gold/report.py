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
logger = logging.getLogger("gold.student_session_report")

# Silver sources
S3_SILVER_SESSION_STUDENT = "s3a://lakehouse/warehouse/silver.db/tutor/class_regular_session_student"
S3_SILVER_SESSION         = "s3a://lakehouse/warehouse/silver.db/tutor/class_regular_session"
S3_SILVER_STUDY_PROGRAM   = "s3a://lakehouse/warehouse/silver.db/tutor/study_program"
# S3_SILVER_GRADE           = "s3a://lakehouse/warehouse/silver.db/tutor/grade"

# Gold target
GOLD_DATABASE = "gold"
GOLD_TABLE    = "tutor_student_session_report"
S3_GOLD_PATH  = "s3a://lakehouse/warehouse/gold.db/tutor/student_session_report"

# Merge key (composite: student + session)
MERGE_KEYS = ["student_id", "class_regular_session_id"]


def parse_args():
    parser = argparse.ArgumentParser(description="Gold flat – student_session_report")
    parser.add_argument(
        "--start-date",
        type=str,
        default='2026-04-02',
        help="Lọc theo learn_date >= start-date (YYYY-MM-DD). Mặc định: hôm qua",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        default='2026-04-03',
        help="Lọc theo learn_date <= end-date (YYYY-MM-DD, inclusive). Mặc định: hôm nay",
    )
    return parser.parse_args()


def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("gold_student_session_report")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created – app: %s", spark.sparkContext.appName)
    return spark


def ensure_gold_table(spark: SparkSession) -> None:
    """Tạo Gold database + table nếu chưa tồn tại."""
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_DATABASE}.{GOLD_TABLE} (
            learn_date                  DATE,
            student_id                  BIGINT,
            class_regular_session_id    STRING,
            study_program               STRING,
            _gold_updated_at            TIMESTAMP
        )
        USING DELTA
        PARTITIONED BY (learn_date)
        LOCATION '{S3_GOLD_PATH}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'description' = 'Gold flat report: student attendance per session with program and grade info'
        )
    """)
    logger.info("Gold table '%s.%s' ensured.", GOLD_DATABASE, GOLD_TABLE)


def build_report(spark: SparkSession, start_date: str, end_date: str):
    """
    Đọc từ Silver và thực hiện join theo logic:
        class_regular_session_student  A
        LEFT JOIN class_regular_session B ON B.id = A.class_regular_session_id
        LEFT JOIN study_program         C ON C.id = B.study_program_id
        WHERE A.status IN ('SUCCESS', 'FAILED_STUDENT_NO_LEARNED')
        AND learn_date BETWEEN start_date AND end_date
    """
    logger.info("Building Gold report: learn_date BETWEEN '%s' AND '%s'", start_date, end_date)

    # --- Load Silver tables ---
    sess_student = (
        spark.read.format("delta").load(S3_SILVER_SESSION_STUDENT)
        .filter(F.col("status").isin("SUCCESS", "FAILED_STUDENT_NO_LEARNED"))
        .select("id", "student_id", "class_regular_session_id", "status")
    )

    session = (
        spark.read.format("delta").load(S3_SILVER_SESSION)
        .select("id", "learn_date", "study_program_id", "grade_id")
        # Chuyển learn_date từ int YYYYMMDD → DATE
        .withColumn(
            "learn_date",
            F.to_date(F.col("learn_date").cast("string"), "yyyyMMdd")
        )
        .filter(
            (F.col("learn_date") >= start_date) &
            (F.col("learn_date") <= end_date)
        )
    )

    study_program = (
        spark.read.format("delta").load(S3_SILVER_STUDY_PROGRAM)
        .select(
            F.col("id").alias("sp_id"),
            F.col("name").alias("study_program"),
        )
    )

    # grade = (
    #     spark.read.format("delta").load(S3_SILVER_GRADE)
    #     .select(
    #         F.col("id").alias("grade_id"),
    #         F.col("code").alias("grade_code"),
    #         F.col("value").alias("grade_value"),
    #     )
    # )

    # --- Join ---
    df = (
        sess_student.alias("a")

        # A LEFT JOIN B
        .join(
            session.alias("b"),
            F.col("a.class_regular_session_id") == F.col("b.id"),
            "left"
        )

        # LEFT JOIN C (study_program)
        .join(
            study_program.alias("c"),
            F.col("b.study_program_id") == F.col("c.sp_id"),
            "left"
        )

        # # LEFT JOIN D (grade)
        # .join(
        #     grade.alias("d"),
        #     F.col("b.grade_id") == F.col("d.grade_id"),
        #     "left"
        # )
    )

    # --- Project & transform ---
    report = (
        df.select(
            F.col("b.learn_date").alias("learn_date"),
            F.col("a.student_id").cast("long").alias("student_id"),
            F.col("a.class_regular_session_id"),
            F.col("c.study_program"),
            # F.col("d.grade_code"),
            # F.when(
            #     (F.col("d.grade_value") >= 1) & (F.col("d.grade_value") <= 5),
            #     F.lit("Tiểu học")
            # ).when(
            #     (F.col("d.grade_value") >= 6) & (F.col("d.grade_value") <= 9),
            #     F.lit("THCS")
            # ).otherwise(
            #     F.lit("Others")
            # ).alias("grade_program"),
            F.current_timestamp().alias("_gold_updated_at"),
        )
    )

    count = report.count()
    logger.info("Gold records to upsert: %d", count)
    return report


def upsert_to_gold(spark: SparkSession, source_df):
    """
    MERGE vào Gold Delta table theo composite key (student_id, class_regular_session_id).
    Vì Gold partitioned by learn_date, thêm learn_date vào merge condition
    để Spark có thể prune partition khi merge.
    """
    gold_table = DeltaTable.forPath(spark, S3_GOLD_PATH)

    merge_condition = " AND ".join([
        f"gold.{k} = source.{k}" for k in MERGE_KEYS
    ] + ["gold.learn_date = source.learn_date"])

    update_cols = [c for c in source_df.columns if c not in MERGE_KEYS]
    update_map  = {col: f"source.{col}" for col in update_cols}
    insert_map  = {col: f"source.{col}" for col in source_df.columns}

    (
        gold_table.alias("gold")
        .merge(source_df.alias("source"), merge_condition)
        .whenMatchedUpdate(set=update_map)
        .whenNotMatchedInsert(values=insert_map)
        .execute()
    )
    logger.info("MERGE into '%s.%s' completed.", GOLD_DATABASE, GOLD_TABLE)


def main():
    args = parse_args()

    today      = datetime.utcnow().date()
    yesterday  = today - timedelta(days=1)
    start_date = args.start_date or str(yesterday)
    end_date   = args.end_date   or str(today)

    logger.info(
        "=== Gold pipeline student_session_report | %s → %s ===",
        start_date, end_date
    )

    spark = build_spark()
    ensure_gold_table(spark)

    report_df = build_report(spark, start_date, end_date)

    if report_df.rdd.isEmpty():
        logger.info("No data in the given window. Exiting.")
        spark.stop()
        return

    upsert_to_gold(spark, report_df)

    logger.info("=== Pipeline finished successfully ===")
    spark.stop()


if __name__ == "__main__":
    main()