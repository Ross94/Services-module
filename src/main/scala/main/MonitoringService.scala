package main

import java.net.URI
import java.util.concurrent.TimeUnit

import api.events.SensorsHubEvents.DeviceCreated
import api.internal.DriversManager
import api.sensors.DevicesManager
import api.sensors.Sensors.Encodings
import io.javalin.embeddedserver.jetty.websocket.WebSocketHandler
import spi.service.{Service, ServiceMetadata}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration.Duration
import rx.lang.scala
import rx.lang.scala.Observable

class MonitoringService extends Service {
  implicit val _ = DefaultFormats

  def valueValidator(sensorType: String, value: Any, threshold: Any): Boolean = sensorType match {
    case "temperature" =>
      if(value.toString.toDouble > threshold.toString.toDouble) true else false
    case _ => false
  }

  private[this] var jSonStream: scala.Observable[String] = _

  DevicesManager.events.subscribe(_ match {
      case deviceCreated: DeviceCreated =>
        def sensorStream(): Observable[String] = deviceCreated.ds.dataStreams.head.observable.sample(Duration(800, TimeUnit.MILLISECONDS)).map(elem => {
          var jsonElem = JObject()
          jsonElem ~= ("name" -> deviceCreated.ds.name)
          jsonElem ~= ("type" -> elem.parentDataStream.observedProperty.name)
          jsonElem ~= ("value" -> elem.result.toString.toDouble)
          jsonElem ~= ("timestamp" -> elem.resultTime.toString)
          jsonElem ~= ("alarm" -> valueValidator(elem.parentDataStream.observedProperty.name, elem.result, 25))
          compact(render(jsonElem))
        })
        jSonStream == null match {
          case true => jSonStream = sensorStream
          case false => jSonStream = jSonStream merge sensorStream
        }
      case _ =>
  })

  def getStream(): Option[Observable[String]] = if(jSonStream == null) Option.empty else Option(jSonStream.doOnSubscribe())

  override def init(metadata: ServiceMetadata): Unit = {}

  override def restart(): Unit = {}

  override def dispose(): Unit = {}

}

object Test extends App {

  val t = new MonitoringService()
  t.init(null)

  val d1 = DriversManager.instanceDriver("driver 1")
  d1.foreach {
    drv =>
      drv.controller.init()
      drv.controller.start()
      drv.config.configure("conf.conf")
      DevicesManager.createDevice("t1", "", Encodings.PDF, new URI(""), drv)
      DevicesManager.createDevice("t2", "", Encodings.PDF, new URI(""), drv)
  }

  import io.javalin.Javalin

  val app = Javalin.start(8000)
  app.ws("/jsonStream", (ws: WebSocketHandler) => {
    val commands = "start => get json stream\n"
    ws.onConnect(session => session.send("WELCOME!\n" + "Command list:\n" + commands))
    ws.onMessage((session, message) => {
      message match {
        case "start" => if(t.getStream().nonEmpty)
          t.getStream().get.subscribe(jsonElem => session.getRemote.sendString(jsonElem))
          else session.getRemote.sendString("empty stream: no sensors found")
        case _ => session.getRemote.sendString("UNKNOWN COMMAND!\n" + commands)
      }
    })
  })

  //if(t.getStream().nonEmpty) t.getStream().get.subscribe(e => println(e)) else println("empty")
}