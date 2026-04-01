import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, DoubleType
)

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("bronze.report_monthly_learning_student_info")

KAFKA_BOOTSTRAP_SERVERS = "172.25.80.136:9092"
KAFKA_TOPIC             = "edupia_cdp.raw_tutor.report_monthly_learning_student_info"
KAFKA_CONSUMER_GROUP    = "spark-bronze-report_monthly_learning_student_info"
KAFKA_STARTING_OFFSETS  = "earliest"

S3_BRONZE_PATH          = "s3a://lakehouse/bronze/tutor/report_monthly_learning_student_info"
S3_CHECKPOINT_PATH      = "s3a://lakehouse/_checkpoints/bronze/tutor/report_monthly_learning_student_info"

HIVE_DATABASE           = "bronze"
HIVE_TABLE              = "tutor_report_monthly_learning_student_info"

TRIGGER_INTERVAL        = "60 seconds"
MAX_OFFSETS_PER_TRIGGER = 50_000

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("bronze_report_monthly_learning_student_info")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created – app: %s", spark.sparkContext.appName)
    return spark

# --- Schema Definition ---
AFTER_SCHEMA = StructType([
    StructField("id",                       StringType(),  True),
    StructField("student_id",               LongType(),    True),
    StructField("full_name",                StringType(),  True),
    StructField("phone",                    StringType(),  True),
    StructField("grade",                    StringType(),  True),
    StructField("level",                    StringType(),  True),
    StructField("sync_date",                IntegerType(), True),
    StructField("month_of_report",          IntegerType(), True),
    StructField("created_by",               StringType(),  True),
    StructField("created_at",               LongType(),    True),
    StructField("updated_by",               StringType(),  True),
    StructField("updated_at",               LongType(),    True),
    StructField("pss1_learning",            IntegerType(), True),
    StructField("pss2_attitude",            IntegerType(), True),
    StructField("pss3_english",             IntegerType(), True),
    StructField("number_sessions_per_week", IntegerType(), True),
    StructField("pss_total_score",          DoubleType(),  True),
    StructField("pss_note",                 StringType(),  True),
    StructField("eligible_calculate_ps",    StringType(),  True),
])

PAYLOAD_SCHEMA = StructType([
    StructField("before",      AFTER_SCHEMA,  True),
    StructField("after",       AFTER_SCHEMA,  True),
    StructField("op",          StringType(),  True),
    StructField("ts_ms",       LongType(),    True),
    StructField("ts_us",       LongType(),    True),
    StructField("ts_ns",       LongType(),    True),
    StructField("transaction", StringType(),  True),
])

MESSAGE_SCHEMA = StructType([
    StructField("payload", PAYLOAD_SCHEMA, True),
])

def read_kafka(spark: SparkSession):
    logger.info("Subscribing to Kafka topic: %s", KAFKA_TOPIC)
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",    KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",                   KAFKA_TOPIC)
        .option("groupIdPrefix",               KAFKA_CONSUMER_GROUP)
        .option("startingOffsets",             KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger",         MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss",              "false")
        .load()
    )

def transform(raw_df):
    parsed = (
        raw_df
        .select(
            F.col("topic").alias("_kafka_topic"),
            F.col("partition").alias("_kafka_partition"),
            F.col("offset").alias("_kafka_offset"),
            F.col("timestamp").alias("_kafka_timestamp"),
            F.from_json(
                F.col("value").cast("string"),
                MESSAGE_SCHEMA
            ).alias("msg")
        )
    )

    flattened = (
        parsed
        .select(
            F.col("msg.payload.op").alias("_op"),
            F.col("msg.payload.ts_ms").alias("_ts_ms"),
            F.col("_kafka_topic"),
            F.col("_kafka_partition"),
            F.col("_kafka_offset"),
            F.col("_kafka_timestamp"),
            F.col("msg.payload.after.*"), # Lấy toàn bộ các trường trong after
            F.current_timestamp().alias("_ingested_at"),
        )
        .withColumn(
            "_event_date",
            F.to_date(F.from_unixtime(F.col("_ts_ms") / 1000))
        )
    )
    return flattened

def ensure_hive_table(spark: SparkSession) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DATABASE}.{HIVE_TABLE} (
            _op                         STRING,
            _ts_ms                      BIGINT,
            _kafka_topic                STRING,
            _kafka_partition            INT,
            _kafka_offset               BIGINT,
            _kafka_timestamp            TIMESTAMP,
            id                          STRING,
            student_id                  BIGINT,
            full_name                   STRING,
            phone                       STRING,
            grade                       STRING,
            level                       STRING,
            sync_date                   INT,
            month_of_report             INT,
            created_by                  STRING,
            created_at                  BIGINT,
            updated_by                  STRING,
            updated_at                  BIGINT,
            pss1_learning               INT,
            pss2_attitude               INT,
            pss3_english                INT,
            number_sessions_per_week    INT,
            pss_total_score             DOUBLE,
            pss_note                    STRING,
            eligible_calculate_ps       STRING,
            _ingested_at                TIMESTAMP,
            _event_date                 DATE
        )
        USING DELTA
        PARTITIONED BY (_event_date)
        LOCATION '{S3_BRONZE_PATH}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'description' = 'Bronze CDC landing for report_monthly_learning_student_info'
        )
    """)
    logger.info("Hive table '%s.%s' ensured.", HIVE_DATABASE, HIVE_TABLE)

def write_stream(df, spark: SparkSession):
    def foreach_batch_fn(batch_df, batch_id: int):
        count = batch_df.count()
        if count == 0:
            return

        logger.info("Batch %d – writing %d rows to %s.", batch_id, count, HIVE_TABLE)

        (
            batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_event_date")
            .save(S3_BRONZE_PATH)
        )

    query = (
        df.writeStream
        .foreachBatch(foreach_batch_fn)
        .option("checkpointLocation", S3_CHECKPOINT_PATH)
        .trigger(processingTime=TRIGGER_INTERVAL)
        .queryName(f"bronze_{HIVE_TABLE}")
        .start()
    )
    return query

def main():
    spark = build_spark()
    ensure_hive_table(spark)

    raw_df    = read_kafka(spark)
    bronze_df = transform(raw_df)
    query     = write_stream(bronze_df, spark)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping streaming query...")
        query.stop()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()