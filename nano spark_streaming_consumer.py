from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import time
spark = SparkSession.builder \
    .appName("SparkStreamingConsumer") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
KAFKA_TOPIC = "traffic_violations" 
KAFKA_SERVERS = "localhost:9092"
schema = StructType([
    StructField("ViolationType", StringType(), True),
    StructField("Location", StringType(), True),
    StructField("Severity", StringType(), True),
    StructField("Timestamp", StringType(), True),
    StructField("FineAmount", DoubleType(), True)
])
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "latest") \
    .load()
streaming_df = kafka_df.selectExpr("CAST(value AS STRING)", "timestamp")
parsed_df = streaming_df.select(from_json(col("value"), schema).alias("data"), col("timestamp").alias("kafka_timestamp")) \
    .select(col("data.*"), col("kafka_timestamp"))
parsed_df = parsed_df.withColumn("EventTime", col("kafka_timestamp").cast("timestamp"))
violations_count = parsed_df.groupBy(
    window(col("EventTime"), "30 seconds", "10 seconds"),
    col("ViolationType")
).count().withColumnRenamed("count", "TotalViolations")
hotspot_analysis = parsed_df.groupBy(
    window(col("EventTime"), "1 minute", "30 seconds"),
    col("Location")
).count().withColumnRenamed("count", "HotspotCount")
query_violations = violations_count.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("ViolationsByTime") \
    .start()
query_hotspots = hotspot_analysis.writeStream \
    .outputMode("update") \
    .format("console") \
    .option("truncate", "false") \
    .queryName("LocationHotspots") \
    .start()
print("Spark Streaming iniciado. El análisis aparecerá cada 10-30 segundos...")
query_violations.awaitTermination()
query_hotspots.awaitTermination()
spark.stop()
