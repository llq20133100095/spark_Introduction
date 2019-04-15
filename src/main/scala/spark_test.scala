import org.apache.spark.{SparkConf, SparkContext}
//import com.tencent.tdw.spark.SparkContext

object spark_test {
  def main(args: Array[String]) {
    val logFile = "file:///F:/scala_workground/textllq/data/README.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    val a = Seq("20190402","20190403","20190404","20190405","20190406","20190407","20190408")
    println(a.map(row => "p_"+row))
  }
}
