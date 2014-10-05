import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object CalcDiv {

	def splitDividendbyStock(s: String): (String, Double) = {
		val splitstg = s.split(",")
		Tuple2(splitstg(1), splitstg.last.toDouble)
	}

  def main(args: Array[String]) {
	
	//val srcfilename = "hdfs://localhost:54310/NYSE_daily.csv"
		val srcfilename = "hdfs://priya-Aspire-V3-571:54310/NYSE_dividends.csv"
    	val conf = new SparkConf().setAppName("NYSE Application")
    	val sc = new SparkContext(conf)
    	val distdata = sc.textFile(srcfilename)
    	//val distdata = sc.parallelize( logdata) 
    	val stockDivpairs = distdata.map(x => splitDividendbyStock(x))
    	val stockAvgDiv = stockDivpairs.mapValues((_, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues{ case (sum, count) => (1.0 * sum)/count}.collectAsMap()
    	// Count the number of lines 
    	stockAvgDiv.foreach(println) 
    	//println(stockAvgDiv)
    	//println("Number of lines" +  logdata.count)
}
}
