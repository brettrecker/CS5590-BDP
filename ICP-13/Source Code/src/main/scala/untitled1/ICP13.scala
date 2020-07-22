package untitled1

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._

object ICP13 {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local[2]").setAppName("Graph")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("ICP 12 - Graph Frames and GraphX")
      .config(conf = conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // 1. Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created.

    println("1. ===== Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created. =====")

    val trip = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .load("201508_trip_data.csv")
      .toDF()

    val station = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .load("201508_station_data.csv")
      .toDF()

    trip.printSchema()
    station.printSchema()

    trip.createOrReplaceTempView("trip")
    station.createOrReplaceTempView("station")
    val nstation = spark.sql("select * from station")
    val ntrips = spark.sql("select * from trip")

    val v = nstation
      .withColumnRenamed("name","id").distinct()
    val e = ntrips
      .withColumnRenamed("Trip ID", "TripID")
      .withColumnRenamed("Duration", "Duration")
      .withColumnRenamed("Start Date", "StartDate")
      .withColumnRenamed("Start Station", "src")
      .withColumnRenamed("Start Terminal", "StartTerminal")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("End Station", "dst")
      .withColumnRenamed("End Terminal", "EndTerminal")
      .withColumnRenamed("Bike #", "BikeNumber")
      .withColumnRenamed("Subscriber Type", "SubscriberType")
      .withColumnRenamed("Zip Code", "ZipCode")

    val g = GraphFrame(v, e)

    v.cache()
    e.cache()

    // 2. Triangle Count

    println("2. ===== Triangle Count =====")

    val stationTC = g.triangleCount.run()
    stationTC.select("id","count").show()

    // 3. Find Shortest Paths w.r.t. Landmarks

    println("3. ===== Find Shortest Paths w.r.t. Landmarks =====")

    val SP = g.shortestPaths.landmarks(Seq("Townsend at 7th","Market at 4th")).run
    SP.show(false)

    // 4. Apply Page Rank algorithm on the dataset.

    println("4. ===== Apply Page Rank algorithm on the dataset. =====")

    val PG = g.pageRank.resetProbability(0.15).tol(0.01).run()
    PG.vertices.select("id", "pagerank").show()
    PG.edges.select("src", "dst", "weight").show()

    val PG2 = g.pageRank.resetProbability(0.15).maxIter(10).run()
    val PG3 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId("Santa Clara at Almaden").run()

    // 5. Save graphs generated to a file.

    println("5. ===== Save graphs generated to a file. =====")

    g.vertices.write.mode("overwrite").parquet("vertices13")
    g.edges.write.mode("overwrite").parquet("edges13")

  }

}
