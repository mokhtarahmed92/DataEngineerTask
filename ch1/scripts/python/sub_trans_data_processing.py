from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as func
from collections import Counter


def see(s):
    """
    Function to print a value to the console
    """
    print("---- %s -----" % s)


def see(s, v):
    """
    Function to print a key and value to the console
    """
    print("---- %s -----" % s)
    print(v)


def is_int(num):
    """
    Function to check if a value is an integer or not
    """
    try:
        if num is None:
            return False;
        int(num)
        return True
    except ValueError:
        return False


def extract_result(services):
    """
    Function to extract the 1st and  the 2nd most used services
    """
    services = list(services)
    if len(services) == 0:
        return ['', '']
    elif len(services) == 1:
        return [services[0], '']
    else:
        return [services[0], services[1]]


is_int_col = func.UserDefinedFunction(func=is_int, returnType=BooleanType())


def sub_trans_data_processing(intputFilePath, outputFilePath, dpiDuplicatesFilePath, dpiRejectionFilePath):
    """
        Function to extract the subscriber transaction summary from the dpi input file
    """
    spark = SparkSession.builder.appName("challenge1").master("local").getOrCreate()

    mobile_df = spark.read.format("csv").option("header", "true").load(intputFilePath)

    mobile_df_excluded = mobile_df.filter(is_int_col(mobile_df['SubscriberId']) == False)

    subscriber_mobile_df = mobile_df.filter(is_int_col(mobile_df['SubscriberId']))
    unique_subscriber_mobile_df = subscriber_mobile_df.dropDuplicates()

    duplicated_subscriber_mobile_rdd = subscriber_mobile_df.rdd.map(lambda line: (line, 1)).reduceByKey(lambda v1, v2: v1 + v2)
    duplicated_subscriber_mobile_rdd = duplicated_subscriber_mobile_rdd.filter(lambda line: line[1] != 1)
    duplicated_subscriber_mobile_rdd = duplicated_subscriber_mobile_rdd.map(lambda line: line[0])

    mobile_subscriber_summary_df = unique_subscriber_mobile_df.groupBy("SubscriberId").agg(
        func.sum("TotalBytes").alias("Total_Bytes"),
        func.count("TotalBytes").alias("Total_Count"),
        func.current_date().alias("Insertion_Date"),
        func.first('IMEI').alias('IMEI'),
        ((func.unix_timestamp(func.max("EndTime")) - func.unix_timestamp(func.min("StartTime"))) / 60).alias(
            'Total_Time_In_Mins'))

    subscriber_services = unique_subscriber_mobile_df.rdd.map(lambda line: ((line[7], line[19]), 1)).reduceByKey(lambda v1, v2: v1 + v2)
    subscriber_services = subscriber_services.map(lambda line: (line[1], line[0])).sortByKey(False)
    subscriber_services = subscriber_services.map(lambda line: line[1])
    subscriber_services = subscriber_services.groupByKey().map(lambda line: (line[0], extract_result(line[1])[0], extract_result(line[1])[1]))
    subscriber_services = subscriber_services.toDF(["SubscriberId", "MostUsedServiceName_1", "MostUsedServiceName_2"])

    mobile_subscriber_summary_df = mobile_subscriber_summary_df.join(subscriber_services,
                                                                     mobile_subscriber_summary_df.SubscriberId == subscriber_services.SubscriberId)

    mobile_subscriber_summary_df = mobile_subscriber_summary_df.withColumn("Event_Type", func.lit("Mobile"))
    mobile_subscriber_summary_df = mobile_subscriber_summary_df.withColumn("MAC_Address", func.lit("N/A"))
    mobile_subscriber_summary_df = mobile_subscriber_summary_df.withColumn("Access_Point ", func.lit("N/A"))

    mobile_subscriber_summary_df.write.csv(outputFilePath)
    mobile_df_excluded.rdd.saveAsTextFile(dpiRejectionFilePath)
    duplicated_subscriber_mobile_rdd.saveAsTextFile(dpiDuplicatesFilePath)