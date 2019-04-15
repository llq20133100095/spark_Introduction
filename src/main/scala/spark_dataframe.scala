import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.udf

object spark_dataframe {
  def main(args: Array[String]): Unit ={
    val spark=SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val df = spark.read.json("file:///F:/scala_workground/textllq/data/people.json").cache()
//    val result = df.map(row => (row.getString(0), 1))
//    result.toDF().show()
    df.show()

    //where operation
    df.where("age = 19").show()

    //groupByKey
    df.select("name").rdd.map(row => (row.getString(0), 1)).groupByKey().foreach(println)

    //groupBy in dataframe
    val name_group = df.groupBy("name").count()
    name_group.where("count > 1" ).show()

    //重命名
    df.selectExpr("name as id").show()

    //去重
    df.dropDuplicates(Seq("name", "age")).show()

    //插入一列
    val code = (arg: String) => {
      "scut"
    }
    val addCol = udf(code)
    df.withColumn("school", addCol(df("name"))).show()
  }
}
