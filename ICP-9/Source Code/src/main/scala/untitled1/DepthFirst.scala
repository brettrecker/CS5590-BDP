package untitled1

import org.apache.spark.{SparkConf, SparkContext}

object DepthFirst {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MergeSort")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    type Vertex = Int
    type Graph = Map[Vertex, List[Vertex]]
    //      val g: Graph=Map(1 -> List(2,4,6), 2 -> List(4,7))

    val g: Graph=Map(1 -> List(1, 2, 3), 2 -> List(3, 4, 6), 3 -> List(1, 3), 4 -> List(4, 5), 6 -> List(2, 4), 5 -> List(2, 6))

    def DepthFirst(start: Vertex, g: Graph): List[Vertex] = {
      def DFS(vertex: Vertex,visited: List[Vertex]): List[Vertex] = {
        println(vertex)

        println(visited)
        if(visited.contains(vertex)) {
          visited
        }
        else {
          val newNeighbor = g(vertex).filterNot(visited.contains)
          println(newNeighbor)
          newNeighbor.foldLeft(vertex :: visited)((b, a) => DFS(a, b))
        }
      }

      DFS(start, List()).reverse
    }
    val dfsresult=DepthFirst(1,g)

    println("DFS Output",dfsresult.mkString(","))
  }
}
