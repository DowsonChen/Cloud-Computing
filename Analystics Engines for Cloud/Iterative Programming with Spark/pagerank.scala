import org.apache.spark.{SparkConf, SparkContext}

object PageRank { 
  def main(args: Array[String]) {
  	// Read from HDFS, 4 threads
  	val sparkConf = new SparkConf().setAppName("PageRank")
  	val sc = new SparkContext(sparkConf)
	val twitterGraph = sc.textFile("hdfs:///TwitterGraph.txt")
	// Follower list
	var followList = twitterGraph.map(line => (line.split("\\s+")(0), line.split("\\s+")(1))).groupByKey()
	// All followees
	val followee = followList.keys
	// All followers
	val follower = followList.values.flatMap(f => f).distinct()
	// All users
	val allUser = twitterGraph.flatMap(line => line.split("\\s+")).distinct()
	// Dangling users list
	val noFollowee = allUser.subtract(followee).collect()
	var danglingList = for (d <- noFollowee) yield (d, List("empty"))
	// All followee list
	val allFollowee = followList ++ sc.parallelize(danglingList)
	// No follwer user
	val noFollower = allUser.subtract(follower).collect()
	val noFollowerList = for (f <- noFollower) yield (f, 0.0)
	// All users
	val numNodes = 2315848
	// Initialize rank
	var ranks = allFollowee.map(all => (all._1,1.0))
	// 10 times ilteration
	for (i <- 1 to 10) {
		val contribs = sc.accumulator(0.0)
		val con = allFollowee.join(ranks).flatMap{
			case(followee,(list,rankVal)) => {
				if (list.mkString == ("empty")) {
					contribs += rankVal
					List()
				} else {
					list.map(node => (node, rankVal / list.size))
				}
			}
		}
		con.count()
		val contriVal = contribs.value
		// Get rank
		ranks = con.union(sc.parallelize(noFollowerList)).reduceByKey(_ + _).mapValues(a => 0.15 + 0.85 * (contriVal / numNodes + a))
	}
	ranks.map(t => "%s\t%s".format(t._1,t._2)).repartition(1).saveAsTextFile("hdfs:///pagerank-output")
  }
}