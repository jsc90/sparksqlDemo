package sqarkSql

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by jiasichao on 2018/5/7.
  */
object FvTopN {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("FvTopN").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val lines: RDD[String] = sc.textFile("/Users/jiasichao/miaozhen/data/tmp/sparkSqlTrain")

    val userRDD: RDD[(Long, String, Int, Int)] = lines.map(line => {
      val fields = line.split("[,]")
      val id = fields(0).toLong
      val name = fields(1)
      val age = fields(2).toInt
      val fv = fields(3).toInt
      (id, name, age, fv)
    })
    val sorted = userRDD.sortBy(t=>User(t._1,t._2,t._3,t._4),false)
    println(sorted.collect().toBuffer)
    sc.stop()
  }
}

case class User(id:Long,name:String,age:Int,fv:Int) extends Ordered[User]{
  override def compare(that: User): Int = {
    if(this.fv == that.fv){
      this.age -that.age
    }else{
      that.fv - this.age
    }
  }
}
