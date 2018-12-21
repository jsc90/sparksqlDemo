package sqarkSql

import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * Created by jiasichao on 2018/5/10.
  */
object DataSetWordCount {

  def main(args: Array[String]): Unit = {

    //获得一个sparkSessiion
    val session  = SparkSession.builder().appName("DataSetWordCount").master("local[2]").getOrCreate()

    //导入session对象中的隐式转换
    import session.implicits._

    val lines: Dataset[String] = session.read.textFile("/Users/jiasichao/miaozhen/data/tmp/words")


    val words: Dataset[String] = lines.flatMap(_.split("\\t"))

    //DSL
    //为了可以使用app中的聚合函数，导入spark sql 中的函数
    import org.apache.spark.sql.functions._
    val result = words.groupBy($"value" as "word").count().sort($"counts" desc)
//    val result: Dataset[Row] = words.groupBy($"value" as "word").agg(count("*") as "counts").orderBy($"counts" desc)


    println(result.show())

  }

}
