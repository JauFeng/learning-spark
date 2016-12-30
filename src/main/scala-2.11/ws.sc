case class A()

case class B()

val a = A()
val b = B()

a match {
  case x: A  B ⇒
    println(x)
  case _ ⇒
    println("unknown")
}