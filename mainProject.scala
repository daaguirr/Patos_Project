package mainPatos

/**
  * Created by dagum on 6/12/2017.
  */
import org.apache.spark.{SparkConf, SparkContext}


object mainProject {
  def main(args: Array[String]): Unit = {

    def w_mentioned(numMentions : Int, numArticles : Int, numSources: Int) = {
      val a = 1
      val factor = 1.2
      (a* (numMentions.toDouble/numArticles))**(numSources*factor)
    }

    def w_root(isRoot : Int) = {
      val a = 1
      a ** isRoot
    }
    def metric(x : Array[String]) : Double = {
      val isRoot = x(25).toInt
      val gold = x(30).toDouble
      val numMentions = x(31).toInt
      val numArticles = x(33).toInt
      val numSources = x(32).toInt
      val b = 1.8
      w_mentioned(numMentions,numArticles,numSources)*b**gold + w_root(isRoot)
    }

    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("project").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val input = sc.textFile(inputFile)


    val actor1_val = input.map(line => {
      val split = line.split("\t")
      Tuple2(split(61),(Array(split(34).toDouble), Array(metric(split))))
    })

    val actor2_val = input.map(line => {
      val split = line.split("\t")
      Tuple2(split(62), (Array(split(34).toDouble), Array(metric(split))))
    })

    val country_val = input.map(line => {
      val split = line.split("\t")
      Tuple2(split(63),(Array(split(34).toDouble), Array(metric(split))))
    })

    val general = sc.union(Array(actor1_val, actor2_val, country_val))

    val country_tuples = general.reduceByKey((a ,b) => {
      (a._1 ++ b._1, a._2 ++ b._2)
    })


  }
}
