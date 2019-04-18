import org.apache.spark.sql.SparkSession

object spark_dataframe2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local").getOrCreate()
    import spark.implicits._
    val df = spark.read.json("file:///F:/scala_workground/textllq/data/people.json").cache()
    df.show()

    //随机抽样: false为无放回抽样
    val sample_df = df.sample(false, 0.2, 2020)
    sample_df.show()

    //取差集
    val except_df = df.except(sample_df)
    except_df.show()
    println(except_df.count())
  }
}
