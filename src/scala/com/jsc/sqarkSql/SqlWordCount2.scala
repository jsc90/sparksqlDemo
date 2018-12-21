package sqarkSql

import com.mysql.jdbc.Connection
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * Created by jiasichao on 2018/5/10.
  * 使用DataFrame或者Sql处理数据，先将非结构化数据转换成结构化数据。
  * 然后注册视图，执行sql（tranfaction），最后触发acction
  *
  */
object SqlWordCount2 {

  def dataToMysql(part: Iterator[Row]):Unit = {
    //创建jdbc连接
    val conn :Connection = null
    part.foreach( r =>{
      val word= r.getAs[String](0)
      val c = r.getAs[String](1)
    })
  }

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("SqlWordCount").master("local[2]").getOrCreate()
    //
    val lines: DataFrame = spark.read.format("text").load("/Users/jiasichao/miaozhen/data/tmp/words")

    import spark.implicits._


    val dataSet: Dataset[String] = {
      lines.flatMap(r => r.getAs[String]("value").split("[\t]"))
    }

//    val df = dataSet.toDF()
    //dsl风格
    //导入spark的聚合函数
    import org.apache.spark.sql.functions._
    val sort: Dataset[Row] = dataSet.groupBy($"value" as "word").agg(count("*") as "counts").sort($"counts" desc)

    sort.show()

    sort.foreachPartition(part =>{
      dataToMysql(part)
    })

    spark.stop()
  }



}
