package sqarkSql

import java.sql.{Connection, DriverManager}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.{SparkConf, SparkContext, broadcast}

/**
  * Created by jiasichao on 2018/5/4.
  */
object IPLocation {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("IPLocation").master("local[2]").getOrCreate()


    //指定以后从哪里读取数据
    //1.ip规则数据
    val lines = spark.read.textFile(args(0))

    import spark.implicits._

    val rules: Dataset[(Long, Long, String)] = lines.map(line => {
      val fields = line.split("[|]")
      val startNum = fields(2).toLong
      val endNum = fields(3).toLong
      val provice = fields(6)

      (startNum, endNum, provice)
    })
    val ruleDF: DataFrame = rules.toDF("snum", "enum", "province")


    //2.网站的访问日志
     val accesslog: Dataset[String] = spark.read.textFile(args(1))
    //整理数据
    val ipds: Dataset[Long] = accesslog.map(line => {
      val fields = line.split("[|]")
      val ipNum = IpTest.ip2Long(fields(1))

      ipNum
    })

    val ipdf: DataFrame = ipds.toDF("ip")

    ruleDF.createTempView("v_rule")
    ipdf.createTempView("v_ip")

    val result: DataFrame = spark.sql("select province ,count(*) count from v_ip join v_rule on (ip >= snum and ip <= enum) group by province order by count desc")

    result.show()

    spark.stop()
  }


}


object IpTest{

    def ip2Long(ip:String):Long ={
      val fragments = ip.split("[.]")
      var ipNum = 0l
      for(i <- 0 until fragments.length ){
        ipNum = fragments(i).toLong | ipNum << 8L
      }
      ipNum
    }
  def binarySearch(lines:Array[(Long,Long,String)],ip:Long):Int ={
    var low = 0
    var high = lines.length-1
    while (low <= high){
      val middle = (low + high) /2
      if ((ip >= lines(middle)._1) && ip <= lines(middle)._1){
        return middle
      }
      if(ip < lines(middle)._1){
        high = middle -1
      }else{
        low = middle +1
      }
    }
    -1
  }
  def data2MySQL(part:Iterator[(String,Int)]):Unit ={
    //创建一个JDBC连接
    val conn:Connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata","root","123456")
    val prepareStatement = conn.prepareStatement("INSERT INTO access_log values (?,?)")

    //写入数据
    part.foreach(line =>{
      prepareStatement.setString(1,line._1)
      prepareStatement.setInt(2,line._2)
      prepareStatement.executeUpdate()
    })
    prepareStatement.close()
    conn.close()
  }

  def main(args: Array[String]): Unit = {
    val ip ="123.113.96.30"
    val num = ip2Long(ip)
    println(num)
  }
}
