import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang.StringUtils

object CitationGraph {
	def computeNumberOfShortestPaths(adjacencyList: collection.immutable.Map[String, Seq[String]], source: String) : Vector[Int] = {
		val distance = collection.mutable.Map[String, Int](source -> 0).withDefaultValue(Int.MaxValue)
		val queue = collection.mutable.Queue[String](source)
		val visited = collection.mutable.Map[String, Boolean](source -> true).withDefaultValue(false)

		while(!queue.isEmpty){
			val current = queue.dequeue
			
			adjacencyList.contains(current) match {
				case true => {
					val neighbors = adjacencyList(current)
					for(neighbor <- neighbors){
						if(visited(neighbor) == false){
							visited(neighbor) = true
							distance(neighbor) = distance(current) + 1
			
							if(distance(neighbor) <= 4){ // put 4 into a final variable
								queue += neighbor
							}
						}
					}
				}
				case false =>
			}			
		}

		Vector.fill[Int](4)(0)
			.zipWithIndex
			.map(x => distance.values.count(_ == x._2 + 1))
	}

	def mergeNodes(tuple1: (String, Set[String]), tuple2: (String, Set[String])): (String, Set[String]) =
		(tuple2._1, tuple1._2 union tuple2._2)
	
	def main(args: Array[String]): Unit = {
		// uncomment below line and change the placeholders accordingly
		// val sc = SparkSession.builder().master("spark://carson-city:30365").getOrCreate().sparkContext

		// to run locally in IDE,
		// But comment out when creating the jar to run on cluster
		// val sc = SparkSession.builder().master("local").getOrCreate().sparkContext
		val sparkSession = 
			SparkSession
				.builder()
				.getOrCreate()

		val sparkContext = sparkSession.sparkContext

      	import sparkSession.implicits._

		// to run with yarn, but this will be quite slow, if you like try it too
		// when running on the cluster make sure to use "--master yarn" option
		// val sc = SparkSession.builder().master("yarn").getOrCreate().sparkContext

		// ==== Load Data ===== //

		val publishedDatesRDD = 
			sparkContext
				.textFile(s"${args(0)}/published-dates.txt")

		val citationsRDD =
			sparkContext
				.textFile(s"${args(0)}/citations.txt")

		// ============================== Task 1 ============================== //

		// ==== Calculate Year Wise Nodes Set, their counts and Paper-Year Pairs ==== //

		val yearWisePapersSet =
			publishedDatesRDD
				.filter(line => !line.contains("#"))
				.map(line => line.split("\\s+"))
				.map(x => {
					val year = x(1).trim.split("-")(0)
					val paperId = x(0).trim
					(paperId, (year, paperId))
				})
				.reduceByKey((a, b) => if(a._1 < b._1) a else b)
				.map(_._2)
				.groupBy(_._1)
				.mapValues(_.map(_._2).toSet)
				.collectAsMap
				.toList
				.sortBy(_._1)
				.scanLeft (("", Set.empty[String]))(mergeNodes)
				.filter(_._1 != "")
				.toMap

		val yearWisePapersCount =
			yearWisePapersSet
				.map(x => (x._1, x._2.size))
				.toSeq
				.toDF("Year", "n(t)")
				.orderBy(asc("Year"))

		// ==== Filter Edges that do not correspond to valid nodes ==== //

		val citationEdgePairs = 
			citationsRDD
				.filter(line => !line.contains("#"))
				.map(line => line.split("\\s+"))
				.map(x => (x(0), x(1)))
				.collect
				.toSet

		// ==== Calculate Year Wise Edges Set and their counts ==== //

		val yearWiseCitationsSet = // keep self edges and duplicate reverse edges
			yearWisePapersSet
				.map(x => {
					val year = x._1
					val yearNodes = x._2

					val validEdges =
						citationEdgePairs
							.filter(y => yearNodes.contains(y._1) && yearNodes.contains(y._2))
					
					(x._1, validEdges.toSet)
				})

		val yearWiseCitationsCount =
			yearWiseCitationsSet
				.map(x => (x._1, x._2.size))
				.toSeq
				.toDF("Year", "e(t)")
				.orderBy(asc("Year"))
	
		yearWisePapersCount
			.coalesce(1)
			.write
			.mode(SaveMode.Append)
			.option("header", "true")
			.csv(args(1))

		yearWiseCitationsCount
			.coalesce(1)
			.write
			.mode(SaveMode.Append) 
			.option("header", "true")
			.csv(args(1))

		// =========================== Task 2 ============================== //

		val years = 1992 to 2002
		
		val columns = List("Year", "d(1)", "d(2)", "d(3)", "d(4)")
		
		val yearWiseNumberOfShortestPaths =
			years
				.map(year => {
					val yearNodes =
						yearWisePapersSet(year.toString).toSeq

					val yearEdges =
						yearWiseCitationsSet(year.toString)

					val adjacencyList = 
						yearEdges
							.flatMap(edge => Seq(edge, edge.swap))
							.groupBy(_._1)
							.mapValues(_.map(_._2).toSeq)

					val numberOfShortestPaths =
						yearNodes
							.foldLeft (Vector(0, 0, 0, 0)) {(result, e) => {
								val nodeAnswer = computeNumberOfShortestPaths(adjacencyList, e)
								Vector(result, nodeAnswer).transpose.map(_.sum) 
							}
						}

					(year, numberOfShortestPaths)
				})
				.sortBy(_._1)
				.map(x => (x._1, x._2.map(_ / 2)))
				.map(x => (x._1, x._2.scanLeft(0)(_ + _).drop(1)))
				
		val df = 
			yearWiseNumberOfShortestPaths
				.map(x => Vector(x._1) ++ x._2)
				.toDF("YearWithDistances")

		val vecToArray = udf((xs: Array[Int]) => xs.toArray)
		val dfArr = df.withColumn("All", vecToArray($"YearWithDistances")) 
		val sqlExpr = columns.zipWithIndex.map { case(alias, idx) => col("All").getItem(idx).as(alias) }
		
		val yearWiseNumberOfShortestPathsDF =
			dfArr.select(sqlExpr: _*)

		yearWiseNumberOfShortestPathsDF
			.coalesce(1)
			.write
			.mode(SaveMode.Append)
			.option("header", "true")
			.csv(args(1))
  	}
}
