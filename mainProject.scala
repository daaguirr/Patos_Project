package mainPatos

/**
  * Created by dagum on 6/12/2017.
  */
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.pow

object mainProject {
  def main(args: Array[String]): Unit = {

    implicit class PowerInt(val i:Double) extends AnyVal {
      def ** (exp:Double):Double = pow(i,exp)
    }

    def w_mentioned(numMentions : Int, numArticles : Int, numSources: Int) = {
      val a = 1
      val factor = 1.2
      (a* (numMentions.toDouble/numArticles))**(numSources*factor)
    }

    def w_root(isRoot : Int) = {
      val a = 1
      a ** isRoot
    }

    def color(x: Double) = x match {
      case y if y>= -100 && y < -50 => 0.0
      case y if y>= -50 && y < 0 => 1.0
      case y if y>= 0 && y < 50 => 2.0
      case y if y>= 50 && y <= 100 => 3.0
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
      Tuple2(split(61),(Array(color(split(34).toDouble)), Array(metric(split))))
    })

    val actor2_val = input.map(line => {
      val split = line.split("\t")
      Tuple2(split(62), (Array(color(split(34).toDouble)), Array(metric(split))))
    })

    val country_val = input.map(line => {
      val split = line.split("\t")
      Tuple2(split(63),(Array(color(split(34).toDouble)), Array(metric(split))))
    })

    val general = sc.union(Array(actor1_val, actor2_val, country_val))

    val country_tuples = general.reduceByKey((a ,b) => {
      (a._1 ++ b._1, a._2 ++ b._2)
    })

    val real_tuples = country_tuples.map(a => (a._1, a._2._1.zip(a._2._2), a._2._1.min, a._2._1.max))

    val best5 = real_tuples.map(a=> (a._1, a._2.filter( b=> b._2 >= a._4 - (a._4- a._3) * 0.05)))
    val worst5 = real_tuples.map(a=> (a._1, a._2.filter( b=> b._2 <= a._3 + (a._4- a._3) * 0.05)))
    val average = real_tuples.map(a=> (a._1, a._2.filter( b=> {
      b._2 >= a._3 + (a._4- a._3) * 0.05 && b._2 <= a._4 - (a._4- a._3) * 0.05})))

    //val best5avg = best5.map( a => (a._1, a._2.map(b => b._1).sum / a._2.length , a._2.map(b => b._2).sum / a._2.length))
    val avgs = Array(best5, worst5, average).map(c =>
      c.map( a => (a._1, a._2.map(b => b._1).sum / a._2.length , a._2.map(b => b._2).sum / a._2.length)))

    avgs(0).saveAsTextFile(outputFile + "/best5")
    avgs(1).saveAsTextFile(outputFile + "/worst5")
    avgs(2).saveAsTextFile(outputFile + "/avg")
  }
}
