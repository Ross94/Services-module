package services

import api.sensors.Sensors
import spi.service.Service

import _root_.rx.lang.scala.Observable
import Sensors.Observation


class ServiceImpl extends Service {
  private[this] var sensor: Observable[Observation] = _
  private[this] var values: List[Observation] = _

  override def init(obsBus: Observable[Observation]): Unit = {
    sensor = obsBus
    values = List()
  }

  override def restart(): Unit = {
    sensor.foreach(e => {
      values :+= e
      println(e)
    })
  }

  override def dispose(): Unit = values = List()
}

