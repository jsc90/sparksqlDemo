package sqarkSql

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * Created by jiasichao on 2018/5/20.
  */
object JoinTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().master("local[2]").appName("JoinTest").getOrCreate()

    import spark.implicits._

    val lines: Dataset[String] = spark.createDataset(List("1,laozhao,china","2,laoduan,usa","3,laoduan,can"))

    //对数据进行整理
    val tpDs: Dataset[(Long, String, String)] = lines.map(line => {
      val fileds = line.split(",")
      val id = fileds(0).toLong
      val name = fileds(1)
      val nation = fileds(2)
      (id, name, nation)
    })
    val df1: DataFrame = tpDs.toDF("id","name","nation")


    val nations: Dataset[String] = spark.createDataset(List("china,中国","usa,美国"))

    val nationDS: Dataset[(String, String)] = nations.map(line => {
      val fields = line.split(",")
      val ename = fields(0)
      val cname = fields(1)
      (ename, cname)
    })
    val nationDF: DataFrame = nationDS.toDF("ename","cname")

    //第一种，创建试图
//    df1.createTempView("v_users")
//    nationDF.createTempView("v_nation")
//    val result: DataFrame = spark.sql("select u.name,n.cname from v_users u join v_nation n on u.nation = n.ename")


    //第二种
    val result: DataFrame = df1.join(nationDF,$"nation" === $"ename","left")


    result.show()
    spark.stop()

  }
}
