package sqarkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

/**
  * Created by jiasichao on 2018/5/8.
  */
object OldSparkSQL {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OldSparkSQL").setMaster("local[2]")

    val sc = new SparkContext(conf)

    //指定读取hdfs读取数据
    val lines: RDD[String] = sc.textFile("hdfs://127.0.0.1:8000/spark/sparkSqlTrain")

    //对数据进行整理并映射成 case class

    val personRDD = lines.map(line => {
      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      Person(id, name, age, fv)
    })
    //创建sqlContext
    val sqlContext = new SQLContext(sc)

    //引入隐式转换
    import sqlContext.implicits._

    //将RDD转换成DataFrame
    val personDf: DataFrame = personRDD.toDF()

    val personTable = personDf.registerTempTable("t_person")

    val sql: DataFrame = sqlContext.sql("select name,age,fv from t_person order by fv desc , age asc ")

    println(sql.show)


    val result: Dataset[Row] = personDf.select("id","name").where(personDf.col("id")>2)

    println(result.show())

    sc.stop()
  }

}

case class Person(id:Long,name:String,age:Int,fv:Int)
