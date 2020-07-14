package untitled1

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DataTypes


object ICP10 {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val sparkSession = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .master("local[*]")
      .getOrCreate()

    val SQLContext = sparkSession.sqlContext

    //                           PART - 1
    //----------------------------------------------------------------
    // 1.) Import the dataset and create data frames directly on import.

    val df = SQLContext.read.option("header", true).csv("survey.csv")
    df.show(5)

    // 2.) Save data to file.

    df.write.mode("overwrite").option("header","true").csv("output")

    // 3.) Check for Duplicate records in the dataset.

    val distinctCount = df.distinct().count()
    val recordCount = df.count()
    if (recordCount == distinctCount) {
      println("No Duplicates (Distinct Rows: "+ distinctCount +" = Totals Rows: " + recordCount + ")")
    } else {
      println("There are " + ((recordCount - distinctCount)/2) + "duplicate records")
    }

    // 4.) Apply Union operation on the dataset and order the output by Country Name alphabetically.

    val treatYes = df.filter("treatment = 'Yes'")
    val treatNo = df.filter("treatment = 'No'")

    val unionSet = treatYes.union(treatNo).orderBy("Country")
    unionSet.show(100)

    // 5.) Use Groupby Query based on treatment.

    df.groupBy("treatment").count().show(2)

    //                         PART - 2
    //----------------------------------------------------------------
    // 1) Apply the basic queries related to Joins and aggregate functions (at least 2)

    val df1 = treatNo.select("Age","Country","Gender","state","mental_health_consequence")
    val df2 = treatYes.select("Age","Country","Gender","state","mental_health_interview")
    val joindf = df1.join(df2, df1("Country") === df2("Country"), "outer")
    joindf.show(false)

    val udf = df2.union(df1)
    val uniondf =udf.withColumn("Age", udf.col("Age").cast(DataTypes.IntegerType))
    uniondf.orderBy("Country").show(200)

    uniondf.groupBy("Country").count().show()
    uniondf.groupBy("Country").mean("Age").show()

    // 2) Write a query to fetch 13th Row in the dataset

    println("13th Record ----> " + df.take(13).last)

    sparkSession.stop()
  }

}
