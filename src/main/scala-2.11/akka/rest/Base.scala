package akka.rest

import akka.actor.ActorSystem
import akka.event.LoggingAdapter
import akka.stream.ActorMaterializer
import com.typesafe.config.Config

import scala.concurrent.ExecutionContextExecutor

trait Base {
  implicit val _system: ActorSystem
  implicit val _materializer: ActorMaterializer
  implicit val _executionContextExecutor: ExecutionContextExecutor

  def config: Config
  val logger: LoggingAdapter
}
