# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
spark = SparkSession.builder.appName('AV').getOrCreate()
# Your code goes inside this function
# Function input - spark object, click data path, resolved data path
# Function output - final spark dataframe
def sample_function(spark, s3_clickstream_path, s3_login_path):

    windowSpec  = Window.partitionBy("current_date","browser_id", "user_id").orderBy("event_date_time")
    windowSpec1  = Window.partitionBy("browser_id").orderBy("row_number")
    windowSpec2  = Window.partitionBy("current_date","browser_id", "user_id")
	# Your code goes below
    df_clickstream = spark.read.format("json").load(s3_clickstream_path)
    user_mapping =  spark.read.format("csv").option("header",True).load(s3_login_path)
    df_clickstream = df_clickstream.join(broadcast(user_mapping), df_clickstream.session_id==user_mapping.session_id, "left")\
    .drop(user_mapping.session_id)\
    .withColumn('current_date',substring(col("event_date_time"),1,10))\
    .withColumn("row_number",row_number().over(windowSpec))\
    .withColumn("logged_in", when(col('user_id').isNull() ,0).otherwise(1))\
    .withColumn('first_url', first('client_side_data', True).over(windowSpec1))\
    .withColumn('click', when(col('event_type')=='click',1).otherwise(0))\
    .withColumn('pageload', when(col('event_type')=='pageload',1).otherwise(0))\
    .withColumn('number_of_clicks', sum(col('click')).over(windowSpec2))\
    .withColumn('number_of_pageloads', sum(col('pageload')).over(windowSpec2))\
    .select(
        col('browser_id'), \
        col('user_id'), \
        col('logged_in'), \
        col('row_number'), \
        col('client_side_data'), \
        col('first_url'), \
        col('event_type'), \
        col('number_of_clicks'), \
        col('number_of_pageloads')
        )
    df_clickstream.createOrReplaceTempView("df_union_tbl")
    df_result = spark.sql("select * from df_union_tbl limit 1000")	
	# Return your final spark df
    return df_result
sample_function(spark, s3_clickstream_path, s3_login_path)