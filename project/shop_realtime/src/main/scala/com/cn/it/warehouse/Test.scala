package com.cn.it.warehouse

import scala.io.StdIn

object Test {
  def main(args: Array[String]): Unit = {
    val seconds: Double = StdIn.readDouble()
    val sex = StdIn.readChar()
    if (seconds < 8.0) {
      if (sex.toString.equals("男")) {
        println("男")
      } else {
        println("女")
      }
    }
    val age = StdIn.readInt()
    val month = StdIn.readInt()
    if (month == 4 || month == 10) {
      if (18 < age && age < 60) {
        print(60)
      } else if (age < 18) {
        print(30)
      } else {
        print(20)
      }
    } else {
      if (18 < age && age < 60) {
        print(40)
      } else {
        print(20)
      }
    }
  }
}
