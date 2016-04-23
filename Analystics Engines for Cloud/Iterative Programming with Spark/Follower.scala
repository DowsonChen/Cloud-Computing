import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object Follower {
	def main (args: Array[String]) {
	    val conf = new SparkConf().setAppName("Follower Application")
    	val sc = new SparkContext(conf)
		val twitterGraph = sc.textFile("hdfs:///TwitterGraph.txt").distinct().cache()
		val followerNum = twitterGraph.map(line => (line.split("\t")(1), 1)).reduceByKey(_ + _)
		followerNum.map(u => "%s\t%s".format(u._1, u._2)).saveAsTextFile("hdfs:///follower-output")
	}
}
