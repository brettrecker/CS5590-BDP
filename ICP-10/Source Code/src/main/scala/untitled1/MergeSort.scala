package untitled1

import org.apache.spark.{SparkConf, SparkContext}

object MergeSort {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("MergeSort")

    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    def mergeSort(mergeList: List[Int]): List[Int] = {

      val listSplit = mergeList.length / 2
      if (listSplit == 0) mergeList
      else {
        val (left, right) = mergeList splitAt(listSplit)
        merge(mergeSort(left), mergeSort(right))
      }
  }

    println(mergeSort(List(5,5,9,1,0,20,77,11,3,15)))

  }

  def merge(left: List[Int], right: List[Int]): List[Int] =
    (left, right) match {
      case (_, Nil) => left
      case (Nil, _) => right
      case(leftHead :: leftTail, rightHead :: rightTail) =>
        if (leftHead < rightHead) leftHead::merge(leftTail, right)
        else rightHead :: merge(left, rightTail)
    }

}