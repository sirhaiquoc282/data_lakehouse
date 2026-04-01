import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, LongType, IntegerType, BooleanType
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("bronze.class_regular_session")
KAFKA_BOOTSTRAP_SERVERS = "172.25.80.136:9092"
KAFKA_TOPIC             = "edupia_cdp.raw_tutor.class_regular_session"
KAFKA_CONSUMER_GROUP    = "spark-bronze-class_regular_session"
KAFKA_STARTING_OFFSETS  = "earliest"

S3_BRONZE_PATH          = "s3a://lakehouse/bronze/tutor/class_regular_session"
S3_CHECKPOINT_PATH      = "s3a://lakehouse/_checkpoints/bronze/tutor/class_regular_session"

HIVE_DATABASE           = "bronze"
HIVE_TABLE              = "tutor_class_regular_session"

TRIGGER_INTERVAL        = "60 seconds"      # set to None for continuous
MAX_OFFSETS_PER_TRIGGER = 50_000

def build_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("bronze_class_regular_session")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("SparkSession created – app: %s", spark.sparkContext.appName)
    return spark


AFTER_SCHEMA = StructType([
    StructField("id",                          StringType(),  True), \
    StructField("class_in_id",                 StringType(),  True), \
    StructField("class_in_session_id",         StringType(),  True), \
    StructField("learn_date",                  IntegerType(), True), \
    StructField("code",                        StringType(),  True), \
    StructField("status",                      StringType(),  True), \
    StructField("day_of_week",                 StringType(),  True), \
    StructField("number_of_students",          IntegerType(), True), \
    StructField("week_studying",               StringType(),  True), \
    StructField("notes",                       StringType(),  True), \
    StructField("class_regular_id",            StringType(),  True), \
    StructField("class_time_slot_id",          StringType(),  True), \
    StructField("grade_id",                    StringType(),  True), \
    StructField("level_id",                    StringType(),  True), \
    StructField("teacher_id",                  LongType(),    True), \
    StructField("created_by",                  StringType(),  True), \
    StructField("created_at",                  LongType(),    True), \
    StructField("updated_by",                  StringType(),  True), \
    StructField("updated_at",                  LongType(),    True), \
    StructField("study_program_id",            StringType(),  True), \
    StructField("session_type",                StringType(),  True), \
    StructField("init_monitor_id",             StringType(),  True), \
    StructField("folder_id",                   StringType(),  True), \
    StructField("chairman_teacher_id",         LongType(),    True), \
    StructField("ordinal_number_on_week",      IntegerType(), True), \
    StructField("exam_type",                   StringType(),  True), \
    StructField("backup_status",               StringType(),  True), \
    StructField("class_room_id",               StringType(),  True), \
    StructField("class_in_acct_search_status", StringType(),  True), \
    StructField("mgmt_class_in_acct_id",       StringType(),  True), \
])

PAYLOAD_SCHEMA = StructType([
    StructField("before",      AFTER_SCHEMA,  True), \
    StructField("after",       AFTER_SCHEMA,  True), \
    StructField("op",          StringType(),  True), \
    StructField("ts_ms",       LongType(),    True), \
    StructField("ts_us",       LongType(),    True), \
    StructField("ts_ns",       LongType(),    True), \
    StructField("transaction",  StringType(), True), \
])

MESSAGE_SCHEMA = StructType([
    StructField("payload", PAYLOAD_SCHEMA, True), \
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
        .option("maxOffsetsPerTrigger",        MAX_OFFSETS_PER_TRIGGER)
        .option("failOnDataLoss",              "false")
        .option("kafka.session.timeout.ms",    "45000")
        .option("kafka.request.timeout.ms",    "60000")
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
            F.col("msg.payload.ts_us").alias("_ts_us"),
            F.col("msg.payload.ts_ns").alias("_ts_ns"),
            F.col("_kafka_topic"),
            F.col("_kafka_partition"),
            F.col("_kafka_offset"),
            F.col("_kafka_timestamp"),
            F.col("msg.payload.after.id"),
            F.col("msg.payload.after.class_in_id"),
            F.col("msg.payload.after.class_in_session_id"),
            F.col("msg.payload.after.learn_date"),
            F.col("msg.payload.after.code"),
            F.col("msg.payload.after.status"),
            F.col("msg.payload.after.day_of_week"),
            F.col("msg.payload.after.number_of_students"),
            F.col("msg.payload.after.week_studying"),
            F.col("msg.payload.after.notes"),
            F.col("msg.payload.after.class_regular_id"),
            F.col("msg.payload.after.class_time_slot_id"),
            F.col("msg.payload.after.grade_id"),
            F.col("msg.payload.after.level_id"),
            F.col("msg.payload.after.teacher_id"),
            F.col("msg.payload.after.created_by"),
            F.col("msg.payload.after.created_at"),
            F.col("msg.payload.after.updated_by"),
            F.col("msg.payload.after.updated_at"),
            F.col("msg.payload.after.study_program_id"),
            F.col("msg.payload.after.session_type"),
            F.col("msg.payload.after.init_monitor_id"),
            F.col("msg.payload.after.folder_id"),
            F.col("msg.payload.after.chairman_teacher_id"),
            F.col("msg.payload.after.ordinal_number_on_week"),
            F.col("msg.payload.after.exam_type"),
            F.col("msg.payload.after.backup_status"),
            F.col("msg.payload.after.class_room_id"),
            F.col("msg.payload.after.class_in_acct_search_status"),
            F.col("msg.payload.after.mgmt_class_in_acct_id"),
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
    logger.info("Hive database '%s' ensured.", HIVE_DATABASE)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_DATABASE}.{HIVE_TABLE} (
            _op                          STRING,
            _ts_ms                       BIGINT,
            _ts_us                       BIGINT,
            _ts_ns                       BIGINT,
            _kafka_topic                 STRING,
            _kafka_partition             INT,
            _kafka_offset                BIGINT,
            _kafka_timestamp             TIMESTAMP,
            id                           STRING,
            class_in_id                  STRING,
            class_in_session_id          STRING,
            learn_date                   INT,
            code                         STRING,
            status                       STRING,
            day_of_week                  STRING,
            number_of_students           INT,
            week_studying                STRING,
            notes                        STRING,
            class_regular_id             STRING,
            class_time_slot_id           STRING,
            grade_id                     STRING,
            level_id                     STRING,
            teacher_id                   BIGINT,
            created_by                   STRING,
            created_at                   BIGINT,
            updated_by                   STRING,
            updated_at                   BIGINT,
            study_program_id             STRING,
            session_type                 STRING,
            init_monitor_id              STRING,
            folder_id                    STRING,
            chairman_teacher_id          BIGINT,
            ordinal_number_on_week       INT,
            exam_type                    STRING,
            backup_status                STRING,
            class_room_id                STRING,
            class_in_acct_search_status  STRING,
            mgmt_class_in_acct_id        STRING,
            _ingested_at                 TIMESTAMP,
            _event_date                  DATE
        )
        USING DELTA
        PARTITIONED BY (_event_date)
        LOCATION '{S3_BRONZE_PATH}'
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact'   = 'true',
            'description' = 'Bronze CDC landing for class_regular_session (Debezium/Kafka)',
            'source.topic' = '{KAFKA_TOPIC}',
            'layer'        = 'bronze'
        )
    """)
    logger.info(
        "Hive table '%s.%s' ensured at %s",
        HIVE_DATABASE, HIVE_TABLE, S3_BRONZE_PATH
    )


def write_stream(df, spark: SparkSession):
    def foreach_batch_fn(batch_df, batch_id: int):
        count = batch_df.count()
        if count == 0:
            logger.info("Batch %d – empty, skipping.", batch_id)
            return

        logger.info("Batch %d – writing %d rows to Delta bronze.", batch_id, count)

        (
            batch_df.write
            .format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .partitionBy("_event_date")
            .save(S3_BRONZE_PATH)
        )
        logger.info("Batch %d – committed and metastore repaired.", batch_id)

    trigger_opts = (
        {"processingTime": TRIGGER_INTERVAL}
        if TRIGGER_INTERVAL
        else {"continuous": "5 seconds"}
    )

    query = (
        df.writeStream
        .foreachBatch(foreach_batch_fn)
        .option("checkpointLocation", S3_CHECKPOINT_PATH)
        .trigger(**trigger_opts)
        .queryName("bronze_class_regular_session")
        .start()
    )
    logger.info("Streaming query started: %s", query.id)
    return query


def main():
    spark = build_spark()

    ensure_hive_table(spark)

    raw_df        = read_kafka(spark)
    bronze_df     = transform(raw_df)
    query         = write_stream(bronze_df, spark)

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Interrupted – stopping streaming query gracefully.")
        query.stop()
    finally:
        spark.stop()
        logger.info("SparkSession closed.")


if __name__ == "__main__":
    main()