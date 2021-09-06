# importing required libraries and creating a new spark session
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('RETAIL_DATA_ANALYSIS').master("local").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
from pyspark.sql.types import StructType, ArrayType, StructField, IntegerType, StringType, BooleanType, DoubleType, LongType, TimestampType
from pyspark.sql.functions import *

# defining the schema of messages that are being streamed from kafka topic
schema = StructType([
  StructField("type", StringType()),
  StructField("country", StringType()),
  StructField("invoice_no", LongType()),
  StructField("timestamp", TimestampType()),
  StructField("items", ArrayType(
      StructType([
          StructField("SKU", StringType()),
          StructField("title", StringType()),
          StructField("unit_price", DoubleType()),
          StructField("quantity", IntegerType())
      ])
   ))
])

# consuming the messgaes and parsing the json based of the schema
parsed = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "18.211.252.152:9092") \
  .option("subscribe", "real-time-project") \
  .option("startingOffsets","earliest") \
  .load() \
  .select(from_json(col("value").cast("string"), schema).alias("parsed_value"))

# expanding the columns
parsed_1=parsed.select(col("parsed_value.*"))

# UDFs required to caluclate total cost and total items
@udf('float')
def getTotalCost(x,type):
    sum = 0
    for val in x:
     sum += val.quantity*val.unit_price
    if type == 'ORDER':
     return sum
    return -1*sum

@udf('int')
def getTotalItems(x):
    sum = 0
    for val in x:
     sum += val.quantity
    return sum

# creating the new columns total_cost and total_items using the UDFs defined above
df4 = parsed_1.withColumn('total_cost',getTotalCost(parsed_1['items'],parsed_1['type'])).withColumn('total_items',getTotalItems(parsed_1['items']))

# UDFs required to classify order or return
@udf("int")
def isOrder(type):
    if type == 'ORDER':
        isOrder = 1
    else:
        isOrder = 0
    return isOrder

@udf("int")
def isReturn(type):
    if type == 'RETURN':
        isReturn = 1
    else:
        isReturn = 0
    return isReturn

# input data frame with all the final summarized input values
df5 = df4.withColumn('is_order',isOrder(df4['type'])).withColumn('is_return',isReturn(df4['type'])).select('invoice_no','country','timestamp','total_cost','total_items','is_order','is_return')

# time based kpi aggregation to calculate OPM, rate_of_return, total_sale_volume, average_transaction_size
df6 = df5.withWatermark("timestamp", "1 minute").groupBy(window('timestamp','1 minute')).agg(count(df5['invoice_no']).alias('OPM'),(sum(df5['is_return'])/(sum(df5['is_return']+df5['is_order']))).alias('rate_of_return'),sum(df5['total_cost']).alias('total_sale_volume'),(sum(df5['total_cost'])/(sum(df5['is_return']+df5['is_order']))).alias('average_transaction_size'))

# time and country based kpi aggregation to calculate OPM, rate_of_return, total_sale_volume
df7 = df5.withWatermark("timestamp", "1 minute").groupBy(window('timestamp','1 minute'),'country').agg(count(df5['invoice_no']).alias('OPM'),(sum(df5['is_return'])/(sum(df5['is_return']+df5['is_order']))).alias('rate_of_return'),sum(df5['total_cost']).alias('total_sale_volume'))

# writing to console for summarized input in 1 minute batches
query_in = df5.writeStream \
  .outputMode("append") \
  .format("console") \
  .trigger(processingTime='1 minute') \
  .start()

# writing to json for time based kpi in 1 minute batches
query_timebased = df6.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("truncate", "false") \
  .option("path", "user/root/time_kpi") \
  .option("checkpointLocation", "user/root/checkpoint2") \
  .trigger(processingTime='1 minute') \
  .start()
        
# writing to json for time and country based kpi in 1 minute batches
query_timecountrybased = df7.writeStream \
  .outputMode("append") \
  .format("json") \
  .option("truncate", "false") \
  .option("path", "user/root/country_kpi") \
  .option("checkpointLocation", "user/root/checkpoint3") \
  .trigger(processingTime='1 minute') \
  .start()

# waiting for a 10 minute = 600 seconds interval for the process to finish up
query_in.awaitTermination(600)
query_timebased.awaitTermination(600)
query_timecountrybased.awaitTermination(600)
