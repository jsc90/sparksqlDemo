package sqarkSql

/**
  * Created by jiasichao on 2018/5/21.
  */
object TestPow {
  def main(args: Array[String]): Unit = {
    val i = 1*2*3*4*5*6*7*8*9*10

//    val i =3

    val r = Math.pow(i,1.toDouble/10)

    println(r)
  }

}
