package org.example

/**
 * @author ${user.name}
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {
    println( "sofiane est le meilleur!" )
    println("concat arguments = " + foo(args))
  }

}
