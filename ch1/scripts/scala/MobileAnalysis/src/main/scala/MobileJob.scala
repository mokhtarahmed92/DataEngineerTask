import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
  * MobileJob is used to analyze the subscriber data
  */
object MobileJob {
  /**
    * extract Result is a function to extract the top 2 services for a subscriberId from a list which is sorted in desc order
    * @param subscriberId : Takes the subscriberId mainly for debugging
    * @param subscriberServices: A list of subscriber Services
    * @return
    */
  def extractResult(subscriberId: Any, subscriberServices: Iterable[Any]): Array[String]={
    val services = subscriberServices.toArray

    if(services.length == 0){
      return  Array("","")
    }else if (services.length == 1) {
      var service1 = services(0).asInstanceOf[String]
      if(service1.equalsIgnoreCase("null"))
        service1 = ""

      return  Array(service1,"")
    } else{

      var service1 = services(0).asInstanceOf[String]
      if(service1.equalsIgnoreCase("null"))
        service1 = ""

      var service2 = services(1).asInstanceOf[String]
      if(service2.equalsIgnoreCase("null"))
        service2 = ""

      return Array(service1, service2)
    }
  }

  /**
    * isInt Function is to check if a given values is an integer or not
    * @param value
    * @return
    */
  def isInt(value: String):Boolean={
    try{
      value.toInt
      return true
    }
    catch{
      case e: Exception => return false
    }
  }

  /**
    * main Function is the runner for the mobile job
    * "" scala MobileJob file1 file2 file3 file4 [option] file5 ""
    * @param args
    *              arg[0] : the path of the input file for the job
    *              arg[1] : the path of the output file
    *              arg[2] : the path of the rejected records file
    *              arg[3] : the path of the duplicated records file
    *              arg[4] [optional]: the path of the hadoop_home_dir which is used to run the job locally
    *
    *
    */


  def main(args: Array[String]): Unit ={

    // The following paths for the testing purposes only
    val base_test_dir = "E:/mokhtar_scala/data"
    var inputFilePath = base_test_dir + "/mobile_data.csv"
    var hadoop_home_dir = base_test_dir
    var outputFilePath = base_test_dir + "/output.csv"
    var dpiRejectionFilePath = base_test_dir + "/rejected.txt"
    var dpiDuplicatesFilePath =base_test_dir + "/duplicated.txt"

    if(args.length >= 4){
       inputFilePath = args(0)
       outputFilePath =  args(1)
       dpiRejectionFilePath = args(2)
       dpiDuplicatesFilePath = args(3)

    }

    if(args.length == 5){
      hadoop_home_dir = args(4)
    }

    // setting the hadoop home dir actually this step is required if you gonna to run the job in local mode
    // you should also copy the file winutils.exe to base_test_dir/bin
    System.setProperty("hadoop.home.dir",hadoop_home_dir)

    // instantiating the spark session and setting the app name and the running mode
    val spark = SparkSession.builder.appName("challenge1").master("local").getOrCreate()

    // register the user defined function to check if values in a col is int or not
    val isColInt = spark.udf.register("isColInt",isInt _)

    // read the input file which is in comma separated format
    val mobile_df = spark.read.format("csv").option("header", "true").load(inputFilePath)

    // filter out the wrong formatted subscriber rows which i assumed that the will be numeric column
    val mobile_df_excluded = mobile_df.filter(not(isColInt(mobile_df("SubscriberId"))))

    // filter out the correct formatted subscribers
    val subscriber_mobile_df = mobile_df.filter(isColInt(mobile_df("SubscriberId")))

    // drop out the duplicates rows from the input data
    val unique_subscriber_mobile_df = subscriber_mobile_df.dropDuplicates()

    // filter out the duplicated rows to be written later
    val duplicated_subscriber_mobile_rdd = subscriber_mobile_df.rdd
      .map(line => (line,1))
      .reduceByKey(_+_)
      .filter(line => line._2 != 1)
      .map(line => line._1)

    // summarize the subscriberId data like calculating the total bytes consumed and count of transactions into a dataframe
    val mobile_subscriber_summary_df = unique_subscriber_mobile_df
      .groupBy(unique_subscriber_mobile_df("SubscriberId")).agg(
      sum("TotalBytes").alias("Total_Bytes"),
      count("TotalBytes").alias("Total_Count"),
      current_date.alias("Insertion_Date"),
      first("IMEI").alias("IMEI"),
      ((unix_timestamp(max("EndTime")) - unix_timestamp(min("StartTime"))) / 60).alias("Total_Time_In_Mins")
    )

    // calculate the subscriber top 2 consumed services by sorting the all services in desc order
    val subscriber_services_rdd = unique_subscriber_mobile_df.rdd.map(line => ((line(7), line(19)), 1))
      .reduceByKey(_+_)
      .map(line => (line._2, line._1))
      .sortByKey(false)
      .map(line => line._2)
      .groupByKey()
      .map(line => (line._1, extractResult(line._1, line._2)(0), extractResult(line._1, line._2)(1)))

    // convert the subscriber top 2 rdd into a dataframe
    val subscriber_services_df = spark.createDataFrame(subscriber_services_rdd)
      .toDF("SubscriberId", "MostUsedServiceName_1", "MostUsedServiceName_2")

    //join the subscriber summary df with the subscriber top 2 services dataframe
    val mobile_subscriber_summary_df_joined = mobile_subscriber_summary_df.join(subscriber_services_df, ("SubscriberId"))

    // add the hardcoded columns to the final dataframe
    val mobile_subscriber_summary_df_final = mobile_subscriber_summary_df_joined
      .withColumn("Event_Type", lit("Mobile"))
      .withColumn("MAC_Address", lit("N/A"))
      .withColumn("Access_Point ",lit("N/A"))

    // write the output file
    mobile_subscriber_summary_df_final.write.csv(outputFilePath)

    // write the rejected rows with have a wrong formatted subscriberId
    mobile_df_excluded.rdd.saveAsTextFile(dpiRejectionFilePath)

    // write the duplciated rows in the input file
    duplicated_subscriber_mobile_rdd.saveAsTextFile(dpiDuplicatesFilePath)

  }
}

