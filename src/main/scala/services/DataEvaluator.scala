package services

import api.sensors.DevicesManager
import rx.lang.scala.Observable


class DataEvaluator {

  import org.json4s._
  import org.json4s.JsonDSL._
  implicit val _ = DefaultFormats

  def valueValidator(sensorType: String, value: Any, threshold: Any): Boolean = sensorType match {
    case "temperature" =>
      if(value.toString.toInt > threshold.toString.toInt) true else false
    case _ => false
  }

  private[this] val stream = DevicesManager.obsBus.take(10).map(elem => {
    var jsonElem = JObject()
    jsonElem ~= ("type" -> elem.parentDataStream.observedProperty.name)
    jsonElem ~= ("value" -> elem.result.toString.toInt)
    jsonElem ~= ("timestamp" -> elem.resultTime.toString)
    jsonElem ~= ("alarm" -> valueValidator(elem.parentDataStream.observedProperty.name, elem.result, 20))
    jsonElem
  })

  def jsonStream(): Observable[JObject] = stream.doOnSubscribe()

}
