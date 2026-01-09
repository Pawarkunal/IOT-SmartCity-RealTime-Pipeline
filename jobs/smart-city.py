import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from pyspark.sql.avro.functions import from_avro

# Load variables from .env
load_dotenv()

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                "org.apache.hadoop:hadoop-aws:3.3.1,"
                "com.amazonaws:aws-java-sdk:1.12.150,"
                "org.apache.spark:spark-avro_2.12:3.5.1") \
        .getOrCreate()
    
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    
    hadoop_conf.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") 
    hadoop_conf.set("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) 
    hadoop_conf.set("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_SECRET_ACCESS_KEY')) 
    hadoop_conf.set('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') 

    
    # Adjust the log level to minimize console output 
    spark.sparkContext.setLogLevel('WARN')

    # Avro schema

    vehicleSchemaAvro = """
    {
    "type": "record",
    "name": "VehicleData",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "vehicle_id", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "speed", "type": "double"},
        {"name": "direction", "type": "string"},
        {"name": "make", "type": "string"},
        {"name": "model", "type": "string"},
        {"name": "year", "type": "int"},
        {"name": "fuelType", "type": "string"}
    ]
    }
    """

    gpsSchemaAvro = """
    {
    "type": "record",
    "name": "GpsData",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "vehicle_id", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "speed", "type": "double"},
        {"name": "direction", "type": "string"},
        {"name": "vehicleType", "type": "string"}
    ]
    }
    """

    trafficSchemaAvro = """
    {
    "type": "record",
    "name": "TrafficData",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "vehicle_id", "type": "string"},
        {"name": "camera_id", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "snapshot", "type": "string"}
    ]
    }
    """

    weatherSchemaAvro = """
    {
    "type": "record",
    "name": "WeatherData",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "vehicle_id", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "temperature", "type": "double"},
        {"name": "weatherCondition", "type": "string"},
        {"name": "precipitation", "type": "double"},
        {"name": "windSpeed", "type": "double"},
        {"name": "humidity", "type": "int"},
        {"name": "airQualityIndex", "type": "double"}
    ]
    }
    """

    emergencySchemaAvro = """
    {
    "type": "record",
    "name": "EmergencyData",
    "fields": [
        {"name": "id", "type": "string"},
        {"name": "vehicle_id", "type": "string"},
        {"name": "incidentId", "type": "string"},
        {"name": "type", "type": "string"},
        {"name": "timestamp", "type": "string"},
        {"name": "location", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "description", "type": "string"}
    ]
    }
    """

    def read_kafka_topic(topic, avro_schema):
        return (spark.readStream
                .format('kafka')
                .option('kafka.bootstrap.servers','broker:29092')
                .option('subscribe',topic)
                .option('startingOffsets','earliest')
                .load()
                # Magic byte remover
                .selectExpr("substring(value, 6, length(value)-5) as avro_value")
                # Avro deserializer
                .select(from_avro(col("avro_value") , avro_schema).alias("data"))
                .select("data.*")
                .withColumn('timestamp', col('timestamp').cast('timestamp'))
                .withWatermark('timestamp', '2 minutes')
                )

    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option('checkpointLocation', checkpointFolder)
                .option('path', output)
                .outputMode('append')
                .start()
                )

    vehicle_df = read_kafka_topic('vehicle_data', vehicleSchemaAvro).alias("vehicle")
    gps_df = read_kafka_topic('gps_data', gpsSchemaAvro).alias("gps")
    traffic_df = read_kafka_topic('traffic_data', trafficSchemaAvro).alias("traffic")
    weather_df = read_kafka_topic('weather_data', weatherSchemaAvro).alias("weather")
    emergency_df = read_kafka_topic('emergency_data', emergencySchemaAvro).alias("emergency")

    query1 = streamWriter(vehicle_df,'s3a://spark-smartcity-storage/checkpoints/vehicle_data', 's3a://spark-smartcity-storage/data/vehicle_data')
    query2 = streamWriter(gps_df,'s3a://spark-smartcity-storage/checkpoints/gps_data', 's3a://spark-smartcity-storage/data/gps_data')
    query3 = streamWriter(traffic_df,'s3a://spark-smartcity-storage/checkpoints/traffic_data', 's3a://spark-smartcity-storage/data/traffic_data')
    query4 = streamWriter(weather_df,'s3a://spark-smartcity-storage/checkpoints/weather_data', 's3a://spark-smartcity-storage/data/weather_data')
    query5 = streamWriter(emergency_df,'s3a://spark-smartcity-storage/checkpoints/emergency_data', 's3a://spark-smartcity-storage/data/emergency_data')

    query5.awaitTermination()

if __name__ == "__main__":
    main()