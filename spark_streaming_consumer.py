from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, avg
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType, LongType
)

spark = (
    SparkSession.builder
    .appName("MonitoreoSensores")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType())
])

datos_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "sensor_data")
    .option("startingOffsets", "latest")
    .load()
)

sensores = (
    datos_kafka
    .select(from_json(col("value").cast("string"), schema).alias("datos"))
    .select("datos.*")
    .withColumn("timestamp", col("timestamp").cast("timestamp"))
)

promedios = (
    sensores
    .groupBy(
        window(col("timestamp"), "1 minute"),
        col("sensor_id")
    )
    .agg(
        avg("temperature").alias("temperatura_promedio"),
        avg("humidity").alias("humedad_promedio")
    )
)

consulta = (
    promedios.writeStream
    .outputMode("complete")
    .format("console")
    .option("truncate", "false")
    .option("numRows", "50")
    .option(
        "checkpointLocation",
        "file:///home/hadoop/checkpoints/sensor_data"
    )
    .trigger(processingTime="10 seconds")
    .start()
)

consulta.awaitTermination()
