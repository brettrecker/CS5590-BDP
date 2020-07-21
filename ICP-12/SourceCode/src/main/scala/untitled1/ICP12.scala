package untitled1

import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._

object ICP12 {

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

    val trip_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .load("201508_trip_data.csv")

    val station_df = spark.read
      .format("csv")
      .option("header", "true") //reading the headers
      .load("201508_station_data.csv")

    println("1. ===== Import the dataset as a csv file and create data frames directly on import than create graph out of the data frame created. =====")

    trip_df.show(10)
    station_df.show(10)

    // 2. Concatenate chunks into list & convert to DataFrame

    println("2. ===== Concatenate chunks into list & convert to DataFrame =====")

    trip_df.select(concat(col("Start Date"), lit(","), col("End Date")).as("Trip Endpoints")).show(10, false)

    // 3. Remove duplicates

    println("3. ===== Remove duplicates =====")

    var distinctTrip = trip_df.dropDuplicates()
    var distinctStation = station_df.dropDuplicates()
    distinctTrip.show()
    distinctStation.show()

    // 4. Name Columns

    //Trip ID	Duration	Start Date	Start Station	Start Terminal	End Date	End Station	End Terminal	Bike #	Subscriber Type	Zip Code

    val rename_distinctTrip = distinctTrip.withColumnRenamed("Trip ID", "TripID")
      .withColumnRenamed("Duration", "Duration")
      .withColumnRenamed("Start Date", "StartDate")
      .withColumnRenamed("Start Station", "StartStation")
      .withColumnRenamed("Start Terminal", "src")
      .withColumnRenamed("End Date", "EndDate")
      .withColumnRenamed("End Station", "EndStation")
      .withColumnRenamed("End Terminal", "dst")
      .withColumnRenamed("Bike #", "BikeNumber")
      .withColumnRenamed("Subscriber Type", "SubscriberType")
      .withColumnRenamed("Zip Code", "ZipCode")

    //station_id	name	lat	long	dockcount	landmark	installation

    val rename_distinctStation = distinctStation.withColumnRenamed("station_id", "StationID")
      .withColumnRenamed("name","Name")
      .withColumnRenamed("lat","Latitude")
      .withColumnRenamed("long","Longitude")
      .withColumnRenamed("dockcount","DockCount")
      .withColumnRenamed("landmark","Landmark")
      .withColumnRenamed("installation","Installation")

    // 5. Output DataFrame

    println("5. ===== Output DataFrame =====")

    rename_distinctTrip.show(10, false)
    rename_distinctStation.show(10, false)

    // 6. Create vertices

    println("6. ===== Create vertices =====")

    val vertices = distinctStation.select(col("station_id").as("id"),
      col("name"),
      concat(col("lat"), lit(","), col("long")).as("lat_long"),
      col("dockcount"),
      col("landmark"),
      col("installation"))


    val edges = rename_distinctTrip.select("src", "dst", "TripID", "StartDate", "StartStation", "EndDate", "EndStation", "BikeNumber", "SubscriberType", "ZipCode")
    edges.show(10, false)

    val g = GraphFrame(vertices, edges)

    // 7. Show some vertices

    println("7. ===== Show some vertices =====")

    g.vertices.select("*").orderBy("id").show(10)

    // 8. Show some edges

    println("8. ===== Show some edges =====")

    g.edges.groupBy("src", "StartDate", "dst", "EndDate").count().orderBy(desc("count")).show(10)

    // 9. Vertex in-Degree

    println("9. ===== Vertex in-Degree =====")

    val in_degree = g.inDegrees
    in_degree.orderBy(desc("inDegree")).show(10)

    // 10. Vertex out-Degree

    println("10. ===== Vertex out-Degree =====")

    val out_degree =g.outDegrees
    out_degree.orderBy(desc("outDegree")).show(10)

    // 11. Apply the motif findings.

    println("11. ==== Apply the motif findings. =====")

    val motifs = g.find("(a)-[c]->(d)").show()

    // 12. Apply Stateful Queries.

    println("12. ==== Apply Stateful Queries. =====")

    g.edges.filter("BikeNumber > 700").show()
    g.edges.filter("ZipCode = 95051").show()

    // 13. Subgraphs with a condition.

    println("13. ==== Subgraphs with a condition. =====")

    val v2 = g.vertices.filter("dockcount > 10")
    val e2 = g.edges.filter("src > 10")
    val g2 = GraphFrame(v2, e2)
    g2.vertices.select("*").orderBy("id").show(10)


    // BONUS PART

    // 1. Vertex degree

    println("B1. ==== Vertex degree =====")

    g.degrees.show(10)

    // 2. What are the most common destinations in the dataset from location to location?

    println("B2. ==== What are the most common destinations in the dataset from location to location? =====")

    g.edges.groupBy("src", "dst").count().orderBy(desc("count")).show(10)

    // 3. What is the station with the highest ratio of in degrees but fewest out degrees. As in, what station acts as almost a pure trip sink. A station where trips end at but rarely start from.

    println("B3. ==== What is the station with the highest ratio of in degrees but fewest out degrees. As in, what station acts as almost a pure trip sink. A station where trips end at but rarely start from. =====")

    val inDF = in_degree.orderBy(desc("inDegree"))
    val outDF = out_degree.orderBy("outDegree")
    val df = inDF.join(outDF, Seq("id")).selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio")
    df.orderBy(desc("degreeRatio")).limit(10).show()

    // 4. Save graphs generated to a file.

    println("B4. ==== Save graphs generated to a file. =====")

    g.vertices.write.mode("overwrite").parquet("vertices")
    g.edges.write.mode("overwrite").parquet("edges")

    spark.stop()
  }
}