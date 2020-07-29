import org.apache.spark._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.log4j._
import org.graphframes._


object projectexam2 {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val conf = new SparkConf().setMaster("local").setAppName("projectexam2")
    val sc = new SparkContext(conf)
    val spark = SparkSession
      .builder()
      .appName("Project Exam 2-4 - Spark Graphx Task")
      .config(conf = conf)
      .getOrCreate()

    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    //Load group-edges.csv to edges_df data frame
    val edges_df = spark.read
      .format("csv")
      .option("header","true") //reading the headers
      .load("group-edges.csv")
      .toDF()

    println("===== Show Edges Schema =====")
    edges_df.printSchema()

    //Load meta-groups.csv to vertices_df data frame
    val vertices_df = spark.read
      .format("csv")
      .option("header","true")
      .load("meta-groups.csv")
      .toDF()

    println("===== Show Vertices Schema =====")
    vertices_df.printSchema()

    edges_df.createOrReplaceTempView("ge_df")
    vertices_df.createOrReplaceTempView("mg_df")


    val group_edges = spark.sql("select * from ge_df")
    val meta_groups = spark.sql("select * from mg_df")

    //rename vertices columns to include id
    val v = vertices_df
      .withColumnRenamed("group_id", "id")

    //rename edges columns to include src, dst, and rename weight
    val e = edges_df
      .withColumnRenamed("group1", "src")
      .withColumnRenamed("group2", "dst")
      .withColumnRenamed("weight", "weight2")

    //create the graph frame using the vertices and edges
    val g = GraphFrame(v, e)

    e.cache()
    v.cache()

    //display the graph frame vertices and edges
    println("===== Display graph frame vertices =====")
    g.vertices.show()
    println("===== Display graph frame edges =====")
    g.edges.show()

    //Run PageRank until convergence to tolerance "tol"
    val PG = g.pageRank.resetProbability(0.15).tol(0.01).run()
    //Display resulting pageranks and final edge weights
    println("===== Display pageranks after running until convergence to tolerance 'tol' =====")
    PG.vertices.select("id", "pagerank").sort(desc("pagerank")).show()
    println("===== Display final edge weights after running until convergence to tolerance 'tol' =====")
    PG.edges.select("src", "dst", "weight").sort(desc("weight")).show()

    //Run PageRank for a fixed number of iterations (10)
    val PG2 = g.pageRank.resetProbability(0.15).maxIter(10).run()
    //Display resulting pageranks
    println("===== Display pageranks after running for a fixed number of iterations (10) =====")
    PG2.vertices.select("id", "pagerank").sort(desc("pagerank")).show()

    //Ran PageRank personalized for vertex "19654655"
    val PG3 = g.pageRank.resetProbability(0.15).maxIter(10).sourceId("19654655").run()
    //Display resulting pageranks
    println("===== Display pageranks after running for vertex '19654655' =====")
    PG3.vertices.select("id", "pagerank").show()

  }

}
