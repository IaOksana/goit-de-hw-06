# 2. Агрегація даних:
# Зчитайте потік даних, що ви згенерували в першому пункті. За допомогою Sliding window, що має довжину 1 хвилину,
# sliding_interval — 30 секунд, та watermark duration — 10 секунд, знайдіть середню температуру та вологість.
# 3. Знайомство з параметрами алертів:
# Ваш начальник любить змінювати критерії алертів. Тому, щоб деплоїти код кожного разу, параметри алертів вказані в файлі:
# alerts_conditions.csv
# Файл містить максимальні та мінімальні значення для температури й вологості, повідомлення та код алерту. Значення -999,-999 вказують, що вони не використовується для цього алерту.
# Подивіться на дані в файлі. Вони мають бути інтуїтивно зрозумілі. Ви маєте зчитати дані з файлу та використати для налаштування алертів.
#
# 4. Побудова визначення алертів:
# Після того, як ви знайшли середні значення, необхідно встановити, чи підпадають вони під критерії у файлі (підказка: виконайте cross join та фільтрацію).
#
# 5. Запис даних у Kafka-топік:
# Отримані алерти запишіть у вихідний Kafka-топік.
# Приклад повідомлення в Kafka, що є результатом роботи цього коду:

from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from configs import kafka_config
import os

# Пакет, необхідний для читання Kafka зі Spark
os.environ[
    'PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1 pyspark-shell'

# Створення SparkSession

import pyspark
sc = pyspark.SparkContext()
sc.setLogLevel("ERROR")

spark = (SparkSession.builder
         .appName("KafkaStreaming")
         .master("local[*]")
         .config("spark.ui.port", "4041")
         .config("spark.sql.shuffle.partitions", "8")
         .getOrCreate())


# 2. Агрегація даних:
# Зчитайте потік даних, що ви згенерували в першому пункті. За допомогою Sliding window, що має довжину 1 хвилину,
# sliding_interval — 30 секунд, та watermark duration — 10 секунд, знайдіть середню температуру та вологість.
my_name = "oksana"
input_topic_name = f'{my_name}_building_sensors'
output_topic_name = f'{my_name}_alerts'

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0]) \
    .option("kafka.security.protocol", "SASL_PLAINTEXT") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" '
            f'password="{kafka_config["password"]}";') \
    .option("subscribe", input_topic_name) \
    .option("startingOffsets", "earliest") \
    .load()

# id, temperature , humidity, timestamp
schema = StructType([
    StructField("temperature", IntegerType(), True),  # якщо це число, краще DoubleType(),
    StructField("humidity", IntegerType(), True),  # якщо це число, краще DoubleType()
    StructField("event_time", StringType(), True)
])


parsed = ((df.select(
                regexp_replace(col("key").cast("string"), '^"|"$', '').cast("int").alias("id"),
                from_json(col("value").cast("string"), schema).alias("j"))
           .select("id", "j.*"))
          .withColumn("event_time", from_unixtime(col("event_time").cast("double")).cast("timestamp"))
          .withWatermark("event_time", "10 seconds"))

'''query = (parsed.writeStream
         .format("console")
         .outputMode("update")
         .option("truncate", "false")
         .option("checkpointLocation", "/tmp/oksana_chk/avgDF")
         .trigger(processingTime="5 seconds")
         .start())
'''

# знайдіть середню температуру та вологість
avgDF = (parsed
         .groupBy(window(col("event_time"), "1 minute", "30 seconds"))
         .agg(
             round(avg("temperature"), 2).alias("avg_temp"),
             round(avg("humidity"), 2).alias("avg_hum")))

'''
query = (avgDF.writeStream
         .format("console")
         .outputMode("complete")
         .option("truncate", "false")
         .option("checkpointLocation", "/tmp/oksana_chk/grouped")
         .trigger(processingTime="5 seconds")
         .start())


query.awaitTermination()

'''
#_____________________________________________________________________

# 3. Знайомство з параметрами алертів:
# Ваш начальник любить змінювати критерії алертів. Тому, щоб деплоїти код кожного разу, параметри алертів вказані в файлі:
# alerts_conditions.csv
# Файл містить максимальні та мінімальні значення для температури й вологості, повідомлення та код алерту.
# Значення -999,-999 вказують, що вони не використовується для цього алерту.
# Подивіться на дані в файлі. Вони мають бути інтуїтивно зрозумілі. Ви маєте зчитати дані з файлу та використати для налаштування алертів.
#id,humidity_min,humidity_max,temperature_min,temperature_max,code,message
alerts_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("humidity_min", IntegerType(), True),
    StructField("humidity_max", IntegerType(), True),
    StructField("temperature_min",   IntegerType(), True),
    StructField("temperature_max",   IntegerType(), True),
    StructField("code", IntegerType(), True),
    StructField("message", StringType(), True),
])
# змінюємо одразу Значення -999,-999 , що  не використовується для цього алерту на Нулл
alerts_static = (
    spark.read
    .option("header", True)
    .schema(alerts_schema)
    .csv("alerts_conditions.csv")
    .withColumn("temperature_min", when(col("temperature_min") == -999, None).otherwise(col("temperature_min")))
    .withColumn("temperature_max", when(col("temperature_max") == -999, None).otherwise(col("temperature_max")))
    .withColumn("humidity_min",  when(col("humidity_min")  == -999, None).otherwise(col("humidity_min")))
    .withColumn("humidity_max",  when(col("humidity_max")  == -999, None).otherwise(col("humidity_max")))
)

# 4. Побудова визначення алертів:
# Після того, як ви знайшли середні значення, необхідно встановити, чи підпадають вони під критерії у файлі
# (підказка: виконайте cross join та фільтрацію).
# 1) if only one output topic
joined = avgDF.crossJoin(alerts_static)

temp_alert = (col("temperature_max").isNotNull()) & (col("temperature_min").isNotNull()) & (col("avg_temp") >  col("temperature_min")) & (col("avg_temp") <  col("temperature_max"))
hum_alert  = (col("humidity_max").isNotNull())  & (col("humidity_min").isNotNull())  & (col("avg_hum")  >  col("humidity_min")) & (col("avg_hum")  < col("humidity_max"))

trigger = temp_alert | hum_alert

alerts_df = (
    joined
    .where(trigger)
    .select(
        col("window.start").alias("win_start"),
        col("window.end").alias("win_end"),
        "avg_temp", "avg_hum",
        "code", "message",
    )
)

#
# 5. Запис даних у Kafka-топік:
# Отримані алерти запишіть у вихідний Kafka-топік.

import uuid
chk_alerts = f"/tmp/oksana_chk/alerts_{uuid.uuid4()}"
chk_alerts1 = f"/tmp/oksana_chk/alerts_{uuid.uuid4()}"

displaying_df = alerts_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", chk_alerts) \
    .start()

out_json = alerts_df.select(
    to_json(struct(
        col("win_start"),
        col("win_end"),
        col("avg_temp"),
        col("avg_hum"),
        col("code"),
        col("message")
    )).alias("value")
)

query = (
    out_json.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism",    kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
    .option("topic", 'oksana_output_alerts')
    .option("checkpointLocation", chk_alerts1)
    .outputMode("append")  # агрегаты с watermark → можно append
    .start()
)

# 2) if 2 output topics: one for temperature and one for humidity

temp_alerts_df = (
    joined
    .where(temp_alert)
    .select(
        col("window.start").alias("win_start"),
        col("window.end").alias("win_end"),
        "avg_temp", "avg_hum",
        "code", "message",
    )
)

temp_chk_alerts = f"/tmp/oksana_chk/temp_alerts_{uuid.uuid4()}"
temp_chk_alerts1 = f"/tmp/oksana_chk/temp_alerts_{uuid.uuid4()}"

temp_displaying_df = temp_alerts_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", temp_chk_alerts) \
    .start()

temp_out_json = temp_alerts_df.select(
    to_json(struct(
        col("win_start"),
        col("win_end"),
        col("avg_temp"),
        col("avg_hum"),
        col("code"),
        col("message")
    )).alias("value")
)

temp_query = (
    temp_out_json.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism",    kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
    .option("topic", 'oksana_temperature_alerts')
    .option("checkpointLocation", temp_chk_alerts1)
    .outputMode("append")  # агрегаты с watermark → можно append
    .start()
)

# ____________________________________________________________--__

hum_alerts_df = (
    joined
    .where(hum_alert)
    .select(
        col("window.start").alias("win_start"),
        col("window.end").alias("win_end"),
        "avg_temp", "avg_hum",
        "code", "message",
    )
)

hum_chk_alerts = f"/tmp/oksana_chk/hum_alerts_{uuid.uuid4()}"
hum_chk_alerts1 = f"/tmp/oksana_chk/hum_alerts_{uuid.uuid4()}"

hum_displaying_df = hum_alerts_df.writeStream \
    .trigger(availableNow=True) \
    .outputMode("append") \
    .format("console") \
    .option("checkpointLocation", hum_chk_alerts) \
    .start()

hum_out_json = hum_alerts_df.select(
    to_json(struct(
        col("win_start"),
        col("win_end"),
        col("avg_temp"),
        col("avg_hum"),
        col("code"),
        col("message")
    )).alias("value")
)

hum_query = (
    hum_out_json.writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers'][0])
    .option("kafka.security.protocol", kafka_config['security_protocol'])
    .option("kafka.sasl.mechanism",    kafka_config['sasl_mechanism'])
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.plain.PlainLoginModule required '
            f'username="{kafka_config["username"]}" password="{kafka_config["password"]}";')
    .option("topic", 'oksana_humidity_alerts')
    .option("checkpointLocation", hum_chk_alerts1)
    .outputMode("append")  # агрегаты с watermark → можно append
    .start()
)

query.awaitTermination()

#spark.stop()