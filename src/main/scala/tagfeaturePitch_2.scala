package game.spread.nlp

//import java.util.regex.Pattern
//import com.tencent.tdw.spark.toolkit.tdw.{TDWSQLProvider, TDWUtil}
//import game.spread.util.GeneralArgParser
//import org.apache.spark.SparkConf
//import org.apache.spark.sql.SparkSession
//
//import scala.collection.mutable
//import scala.collection.mutable.ArrayBuffer

object tagfeaturePitch2 {
  def main(args: Array[String]): Unit = {
//    val cmdArgs = new GeneralArgParser(args)
//    val user = cmdArgs.getStringValue("user", DEFAULT_USER)
//    val password = cmdArgs.getStringValue("password", DEFAULT_PASSWORD)
//
//    val tag_table = cmdArgs.getStringValue("tag_table", "grprcmd_tag_pipeline_version2_user_to_tags_and_scores")
//    val out_table = cmdArgs.getStringValue("out_table", "tag_feature_Pitch_sample")
//
//    val par_name = cmdArgs.getStringValue("par_name", "20190402")
//
//    val sparkConf = new SparkConf().setAppName("userCharacter")
//      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//      .set("spark.hadoop.validateOutputSpecs", "false").
//      registerKryoClasses(
//        Array(classOf[Set[String]]))
//      .set("spark.driver.maxResultSize", "1g")
//    val sparkSession = SparkSession.builder.config(sparkConf).getOrCreate
//
//    val db_from = new TDWSQLProvider(sparkSession, user, password, "pgc_qq_recommend_application")
//    val db_out = new TDWSQLProvider(sparkSession, user, password, "pcg_social_game_data_app")
//
//    import sparkSession.implicits._
//    //原始tag表，tag信息获取
//    var df_tag = db_from.table(tag_table,Seq("p_20190402","p_20190403","p_20190404","p_20190405","p_20190406","p_20190407","p_20190408"))
//      .select("ftime","uin", "tag","value")
//      .filter("tag is not null")
//      .filter("uin is not null")
//
//    //得到活水用户tag信息
//    var df_liushui = db_out.table("qq_game_sample_data",Seq("p_20190402","p_20190403","p_20190404","p_20190405","p_20190406","p_20190407","p_20190408"))
//      .select("user_id")
//      .distinct()
//      .filter("user_id is not null")
//    //重命名user_id为uin
//    val schemas= Seq("uin")
//    df_liushui = df_liushui.toDF(schemas: _*)
//    df_liushui = df_liushui.join(df_tag,Seq("uin"), "left")
//
//    //统计每一个tag出现的次数  uin  "ftime", "tag","value"  yes
//    val count_tag = df_liushui.map(
//      row => (row.getString(2),1)
//    ).rdd.groupBy(_._1).mapValues(_.size).filter(_._2 > 500)
//     .map(
//          row => {
//            (row._1,row._2)
//          }
//     ).toDF("tag","count")
//
//    //每天数据入库
//    //tag  uin  "ftime","value"  count
//    var df_tag_04 = df_liushui
//      .join(count_tag, Seq("tag"))
//
//    val tag_feature_info_0402 =  df_tag_04.flatMap(
//      row => {
//        val use_tag_to_score_result = new ArrayBuffer[(String,String,String,String,String,String)]()
//        val tag = row.get(0).toString
//        val uin = row.get(1).toString
//        val ftime = row.get(2).toString
//        val value = row.get(3).toString
//        val count = row.get(4).toString
//        if(ftime.equals(par_name)){
//          use_tag_to_score_result.append((par_name,uin,"game_tag",tag,value,count.toString))
//        }
//        (use_tag_to_score_result)
//      }
//    ).toDF("ftime","uin","primary_key","secondary_key","value","coverage")
//
//    val db_utl = new TDWUtil("pcg_social_game_data_app")
//    if (!db_utl.partitionExist(out_table, "p_"+par_name))
//      db_utl.createListPartition(out_table, "p_"+par_name, par_name)
//    db_out.saveToTable(tag_feature_info_0402,out_table , "p_"+par_name, overwrite = false)
//
//
//    println("join_ds end")

  }
}


