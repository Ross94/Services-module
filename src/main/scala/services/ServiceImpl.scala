package services

import spi.service.Service


class ServiceImpl extends Service {

  override def init(): Unit = {
    import org.json4s._
    implicit val _ = DefaultFormats

    val dataEvaluator = new DataEvaluator()
    dataEvaluator.jsonStream().subscribe(elem => println("value: " + (elem \ "value").extract[Int]))
  }

  override def restart(): Unit = {}

  override def dispose(): Unit = {}
}

