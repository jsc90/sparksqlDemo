package sqarkSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by jiasichao on 2018/5/10.
  * 使用DataFrame或者Sql处理数据，先将非结构化数据转换成结构化数据。
  * 然后注册视图，执行sql（tranfaction），最后触发acction
  *
  */
object SqlWordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SqlWordCount").master("local[2]").getOrCreate()
    //
//    val lines: DataFrame = spark.read.format("text").load("/Users/jiasichao/miaozhen/data/tmp/words")
    val lines: Dataset[String] = spark.read.textFile("/Users/jiasichao/miaozhen/data/tmp/words")
    //导入sparksession中的隐式转换
    import spark.implicits._
    val words: Dataset[String] = lines.flatMap(_.split("[\t]"))


    //SQL  转换成dataframe
    val wordDF:DataFrame = words.withColumnRenamed("value","word")

    //执行sql
    val tempView = wordDF.createTempView("v_wordCount")
    val sql: DataFrame = spark.sql("select count(*) as counts,word from v_wordCount group by word order by counts desc")
    sql.show()

    spark.stop()
  }

}
