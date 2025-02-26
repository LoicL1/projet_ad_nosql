from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialisation de la session Spark
spark = SparkSession.builder.appName("test").getOrCreate()

# Lecture des logs depuis Kafka
kafka_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "kafka:9092").option("subscribe", "logs").load()

# Conversion des données en string
logs_df = kafka_df.selectExpr("CAST(value AS STRING)")

# **Extraction des champs en utilisant split()**
parsed_logs = logs_df.withColumn("log_parts", split(col("value"), " ")).select(
        col("log_parts")[0].alias("ip"),  # Adresse IP
        regexp_extract(col("value"), r'\[(.*?)\]', 1).alias("timestamp"),  # Extraire la date entre []
        regexp_extract(col("value"), r'"(\w+) ', 1).alias("method"),  # Extraire le verbe HTTP (GET, POST, etc.)
        regexp_extract(col("value"), r'"(?:\w+) (.*?) HTTP', 1).alias("url"),  # Extraire l'URL demandée
        regexp_extract(col("value"), r'HTTP/\d.\d"', 0).alias("protocol"),  # Extraire le protocole HTTP
        col("log_parts")[8].cast("int").alias("status"),  # Code HTTP
        col("log_parts")[9].cast("int").alias("size")  # Taille de la réponse
    )

# Agrégation des logs par methode et code HTTP

status_error = parsed_logs.groupBy("status","method","timestamp").agg(
    collect_list("url").alias("url"),
    collect_list("ip").alias("ip"),
    count("status").alias("count")
)

status_sort_by_ip = parsed_logs.groupBy("ip",).agg(
    collect_list("timestamp").alias("timestamp"),
    collect_list("method").alias("method"),
    collect_list("url").alias("urls"),
    collect_list("status").alias("status"),
    collect_list("protocol").alias("protocol")
)

status_url = parsed_logs.groupBy("url").agg(
    collect_list("method").alias("method"),
    collect_list("status").alias("status"),
    collect_list("size").alias("size"),
    collect_list("ip").alias("ip")
    )

# Fonction pour écrire dans MongoDB (sans écraser)
def write_to_mongo(df, epoch_id, uri):
    df.write.format("mongo")\
    .mode("append")\
    .option("uri", uri)\
    .option("replaceDocument", "false")\
    .save()

# Écriture des résultats dans MongoDB en streaming
error_query = status_error.writeStream.outputMode("update")\
.foreachBatch(lambda df, epoch_id: write_to_mongo(df, epoch_id,"mongodb://mongo:27017/logs.test_error"))\
.start()

url_query = status_url.writeStream.outputMode("update")\
.foreachBatch(lambda df, epoch_id: write_to_mongo(df, epoch_id,"mongodb://mongo:27017/logs.test_url"))\
.start()

id_query = status_sort_by_ip.writeStream.outputMode("update")\
.foreachBatch(lambda df, epoch_id: write_to_mongo(df, epoch_id,"mongodb://mongo:27017/logs.test_id"))\
.start()


spark.streams.awaitAnyTermination()
