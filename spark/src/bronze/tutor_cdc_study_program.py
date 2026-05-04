import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType
)

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("bronze.study_program")

KAFKA_BOOTSTRAP_SERVERS = "172.25.80.136:9092"
KAFKA_TOPIC             = "lakehouse.tutor.study_program"
KAFKA_CONSUMER_GROUP    = "spark-bronze-study_program"
KAFKA_STARTING_OFFSETS  = "earliest"

S3_BRONZE_PATH          = "s3a://lakehouse/warehouse/bronze.db/tutor/study_program"
S3_CHECKPOINT_PATH      = "s3a://lakehouse/warehouse/_checkpoints/bronze.db/tutor/study_program"

HIVE_DATABASE           = "bronze"
HIVE_TABLE              = "tutor_study_program"

TRIGGER_INTERVAL        = "60 seconds"
MAX_OFFSETS_PER_TRIGGER = 50_000

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("bronze_study_program")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created – app: %s", spark.sparkContext.appName)
    return spark

# --- Schema Definition ---
AFTER_SCHEMA = StructType([
    StructField("id",                   StringType(),  True),
    StructField("name",                 StringType(),  True),
    StructField("max_student_in_class", IntegerType(), True),
    StructField("subject_id",           StringType(),  True),
    StructField("created_by",           StringType(),  True),
    StructField("created_at",           LongType(),    True),
    StructField("updated_by",           StringType(),  True),
    StructField("updated_at",           LongType(),    True),
    StructField("code",                 StringType(),  True),
    StructField("type",                 StringType(),  True),
    StructField("product_type_code",    StringType(),  True),
    StructField("group_type",           StringType(),  True),
    StructField("sessions_per_week",    IntegerType(), True),
    StructField("learning_model",       StringType(),  True),
    StructField("schedule_type",        StringType(),  True),
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
        .option("kafka.bootstrap.servers",  KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe",                KAFKA_TOPIC)
        .option("groupIdPrefix",            KAFKA_CONSUMER_GROUP)
        .option("startingOffsets",          KAFKA_STARTING_OFFSETS)
        .option("maxOffsetsPerTrigger",     MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss",           "false")
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
            F.col("msg.payload.after.*"),  # Lấy toàn bộ các trường trong after
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
            _op                     STRING,
            _ts_ms                  BIGINT,
            _kafka_topic            STRING,
            _kafka_partition        INT,
            _kafka_offset           BIGINT,
            _kafka_timestamp        TIMESTAMP,
            id                      STRING,
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
            _ingested_at            TIMESTAMP,
            _event_date             DATE
        )
        USING DELTA
        PARTITIONED BY (_event_date)
        LOCATION '{S3_BRONZE_PATH}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'description' = 'Bronze CDC landing for study_program'
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