package mainPatos

/**
  * Created by dagum on 6/12/2017.
  */
import org.apache.spark.{SparkConf, SparkContext}
import scala.math.pow
import scala.math.max

object mainProject {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/Program Files/Hadoop/")

    def w_mentioned(numMentions : Int, numArticles : Int, numSources: Int) = {
      val a = 1
      val factor = 1.2
      pow(a* (numMentions.toDouble/numArticles), numSources * factor)
    }

    def w_root(isRoot : Int) = {
      val a = 1
      pow(a, isRoot)
    }

    def metric(x : Array[String]) : Double = {
      try
      {
        val isRoot = x(25).toInt
        val gold = x(30).toDouble
        val numMentions = x(31).toInt
        val numArticles = x(33).toInt
        val numSources = x(32).toInt
        val b = 1.8
        val result = pow(w_mentioned(numMentions,numArticles,numSources)*b,gold) + w_root(isRoot)
        if (!result.isNaN) result  else 0.0
      }
      catch {
        case _ => 0.0
      }

    }


    val inputFile = args(0)
    val outputFile = args(1)
    val conf = new SparkConf().setAppName("project").setMaster("local[*]")

    val sc = new SparkContext(conf)
    val input1 = sc.textFile(inputFile)

    val input = input1.filter(a=> {
      val split1 = a.split('\t')
      split1.length == 64 && split1(34) != "" && split1(25) != "" && split1(30) != "" && split1(31) != "" && split1(32) != "" && split1(33) != ""
    })

    val general1 = input.flatMap(line => {
      val split = line.split('\t')
      val met = (Array(split(34).toDouble), Array(metric(split)))
      Array(split(61),split(62),split(63)).map(b => (b, met))
    })

    val country_tuples = general1.reduceByKey((a ,b) => {
      (a._1 ++ b._1, a._2 ++ b._2)
    }).cache()

    val real_tuples = country_tuples.map(a => (a._1, a._2._1.zip(a._2._2), a._2._1.min, a._2._1.max))

    val best5 = real_tuples.map(a=> (a._1, a._2.filter( b=> b._1 >= a._4 - (a._4- a._3) * 0.05))).coalesce(1)
    val worst5 = real_tuples.map(a=> (a._1, a._2.filter( b=> b._1 <= a._3 + (a._4- a._3) * 0.05))).coalesce(1)
    val average = real_tuples.map(a=> (a._1, a._2.filter( b=> {
      b._1 >= a._3 + (a._4- a._3) * 0.05 && b._1 <= a._4 - (a._4- a._3) * 0.05}))).coalesce(1)

    //val best5avg = best5.map( a => (a._1, a._2.map(b => b._1).sum / a._2.length , a._2.map(b => b._2).sum / a._2.length))
    val avgs = Array(best5, worst5, average).map(c =>
      c.map( a => (a._1, a._2.map(b => b._1).sum / max(1.0,a._2.length) , a._2.map(b => b._2).sum / max(1.0,a._2.length))))

    avgs(0).saveAsTextFile(outputFile + "/best5")
    avgs(1).saveAsTextFile(outputFile + "/worst5")
    avgs(2).saveAsTextFile(outputFile + "/avg")
  }
}
