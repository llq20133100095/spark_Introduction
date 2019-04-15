object test1 {
  def main(args: Array[String]): Unit ={
    var a = 0
    println(addInt(10,1))
    val f = (x: Int) => x + 3
    println(f(1))
  }

  def addInt(a: Int, b: Int): Int ={
    var sum: Int = 0
    sum = a + b
    return sum
  }
}
