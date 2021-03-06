from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql.types import ArrayType, DoubleType, BooleanType 
from pyspark.sql.functions import split, col, regexp_replace,size
from pyspark.sql.functions import udf
import gcsfs
import pandas as pd
from google.cloud import bigquery

APP_NAME = "ULTimetable"
#schema for dataFrame
schema = StructType()
project_name = '20097786-etl-spark-timetable'
bucket_name = 'gs://20097786-ultimetable/ultimetable.csv'

def calculate_lecs(week_array):
    total_lecs = 0
    for item in week_array:
        if len(item.split("-")) > 1:
            if len(item.split("-")[1].strip()) > 0 and len(item.split("-")[0].strip()) > 0:
                total_lecs= total_lecs + int(item.split("-")[1].strip()) - int(item.split("-")[0].strip()) + 1
        else:
            total_lecs = total_lecs + 1
    return   total_lecs

def getTotalLectureSessions(df,spark):
    df.createOrReplaceTempView("timetable")
    sqlDF = spark.sql("SELECT type, sum(total_sessions) as total_sessions FROM timetable GROUP BY type;")
    sqlDF.createOrReplaceTempView('sessions')
    output = spark.sql("SELECT type, total_sessions from sessions WHERE type='LEC'")

    total_sessions = output.select(col('total_sessions').alias('total_sessions')).first().total_sessions
    session_type = output.select(col('type').alias('session_type')).first().session_type

    print("Total number of sessions of type (", session_type, ") for Spring 20/21 semester(current) at University of Limerick: ", total_sessions)

def createAdditionalColumns(calculate_udf,df):
    df = df.withColumn('from',(regexp_replace(split(col('time'),'-')[0],':','.')).cast('double'))
    df = df.withColumn("to",(regexp_replace(split(col("time"),'-')[1],':','.')).cast('double'))
    df = df.withColumn("duration",(col("to") - col("from")).cast('integer')).drop("from").drop("to")
    df = df.withColumn("week_array",split(col("weeks"),","))
    df = df.withColumn("total_sessions",calculate_udf("week_array").cast('Integer'))
    df = df.withColumn("year",col("year").cast('Integer'))
    df = df.drop("Unnamed: 0")
    df = df.drop("week_array")
    return df

def registerUDF(spark):
    spark.udf.register("calculateWithPython", calculate_lecs)
    calculate_udf = udf(calculate_lecs)
    return calculate_udf

def createDataFrame(spark,schema):
    # df = spark.read.format("csv") \
    #     .option("header", True) \
    #     .schema(schema) \
    #     .load(file_path)
    pandasDF = readCsvFromBucket()
    sparkDF=spark.createDataFrame(pandasDF)
    calculate_udf = registerUDF(spark)
    df = createAdditionalColumns(calculate_udf,sparkDF)
    print("Dataframe created with below schema")
    df.printSchema()
    return df

def readCsvFromBucket():
    fs = gcsfs.GCSFileSystem(project=project_name)
    with fs.open(bucket_name) as f:
        pandasDF = pd.read_csv(f,encoding='latin-1')
    pandasDF = pandasDF.fillna('')
    print("Read CSV from Bucket")
    return pandasDF

def createSchema():
    schema = StructType() \
      .add("course_code",StringType(),True) \
      .add("course",StringType(),True) \
      .add("year",IntegerType(),True) \
      .add("day",StringType(),True) \
      .add("time",StringType(),True) \
      .add("module",StringType(),True) \
      .add("type",StringType(),True) \
      .add("location",StringType(),True) \
      .add("professor",StringType(),True) \
      .add("weeks",StringType(),True) \
      .add("course_for",StringType(),True) \
      .add("from",DoubleType(),True) \
      .add("to",DoubleType(),True) \
      .add("duration",IntegerType(),True) 
    return schema

def LoadBigQueryTable(dataFrame):
    print("Load BigQuery Table..")
    dataFrame.write \
      .mode("overwrite") \
      .format("bigquery") \
      .option("temporaryGcsBucket","20097786-ultimetable") \
      .save("dm_timetable_spring21.ul_timetable")

def main(spark):
    schema = createSchema()
    df = createDataFrame(spark,schema)
    getTotalLectureSessions(df,spark)
    LoadBigQueryTable(df)

if __name__ == "__main__":
	spark = SparkSession.builder.appName(APP_NAME).getOrCreate()
	print("Spark session started - get total lecture session for Spring 20/21")
	main(spark)