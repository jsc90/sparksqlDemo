package sqarkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._

/**
  * Created by jiasichao on 2018/5/9.
  */
object HelloDataSet {

  def main(args: Array[String]): Unit = {

//    val conf = new SparkConf
    val session = SparkSession.builder().appName("HelloDataSet").master("local[2]").getOrCreate()

    val lines: RDD[String] = session.sparkContext.textFile("hdfs://127.0.0.1:8000/spark/sparkSqlTrain")

    val rowRDD: RDD[Row] = lines.map(line => {
      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      Row(id, name, age, fv)
    })


    val schema = StructType(
      List(
        StructField("id",LongType,true),
        StructField("name",StringType,true),
        StructField("age",IntegerType,true),
        StructField("fv",IntegerType,true)
      )
    )


    val dataFrame = session.createDataFrame(rowRDD,schema)

    val tempView = dataFrame.createTempView("v_person")

    val dataFrame2: DataFrame = session.sql("select * from v_person where id > 1 order by fv desc")

   dataFrame2.show()

    session.close()
  }

}
