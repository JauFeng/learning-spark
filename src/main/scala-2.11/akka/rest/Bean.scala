package akka.rest

import com.google.inject.Guice

trait Bean {

  val injector = Guice createInjector(

  )

}