package main

import scala.io.Source
import java.net.URI
import java.util.concurrent.TimeUnit

import api.events.SensorsHubEvents.{DeviceCreated, DeviceDeleted}
import api.internal.DriversManager
import api.sensors.DevicesManager
import api.sensors.Sensors.Encodings
import io.javalin.embeddedserver.jetty.websocket.WebSocketHandler
import io.reactivex.subjects.PublishSubject
import spi.service.{Service, ServiceMetadata}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.concurrent.duration.Duration
import rx.lang.scala.Subscription

case class Procedure(sensor: String, rules: List[Rule])
case class Rule(sign: String, threshold: Double, alarm: Int)

class MonitoringService extends Service {
  implicit val _ = DefaultFormats

  private[this] val rules = (parse(Source.fromFile("assets/config/thresholds.json").mkString) \ "allRules").extract[List[Procedure]]

  def valueValidator(sensorType: String, value: Any): Int = {

    def operation(rule: String, value: Double, threshold: Double): Boolean = rule match {
      case "<" => value < threshold
      case "<=" => value <= threshold
      case "==" => value == threshold
      case ">" => value > threshold
      case ">=" => value >= threshold
      case _ => false
    }

    var ret = -1
    rules.filter(_.sensor equals sensorType).foreach(procedure =>
      procedure.rules.sortBy(_.alarm).foreach(rule => if (operation(rule.sign, value.toString.toDouble, rule.threshold)) ret = rule.alarm))
    ret
  }

  private[this] var sensorStreams = Map[Int, Subscription]()
  private[this] val jSonStream = PublishSubject.create[String]()

  DevicesManager.events.subscribe(_ match {
    case deviceCreated: DeviceCreated =>
      deviceCreated.ds.dataStreams.foreach(stream => {
        //adesso aggiungo l'id potrebbe non bastare se un sensore ha più sensori
        sensorStreams += deviceCreated.ds.id -> stream.observable.sample(Duration(800, TimeUnit.MILLISECONDS)).map(elem => {
          var jsonElem = JObject()
          jsonElem ~= ("name" -> deviceCreated.ds.name) //potrebbe servire la pos e non solo il nome del sensore
          jsonElem ~= ("type" -> elem.parentDataStream.observedProperty.name)
          jsonElem ~= ("value" -> elem.result.toString.toDouble)
          jsonElem ~= ("timestamp" -> elem.resultTime.toString)
          jsonElem ~= ("alarm" -> valueValidator(elem.parentDataStream.observedProperty.name, elem.result))
          compact(render(jsonElem))
        }).subscribe(elem => jSonStream.onNext(elem))
      })
    case deviceDeleted: DeviceDeleted =>
      sensorStreams(deviceDeleted.ds.id).unsubscribe()
    case _ =>
  })

  def getStream(): PublishSubject[String] = jSonStream

  override def init(metadata: ServiceMetadata): Unit = {}

  override def restart(): Unit = {}

  override def dispose(): Unit = sensorStreams.foreach(stream => stream._2.unsubscribe())

}

object MonitoringServiceTest extends App {

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

  //t.getStream().subscribe(e => println(e))

  import io.javalin.Javalin

  val app = Javalin.start(8000)
  app.ws("/jsonStream", (ws: WebSocketHandler) => {
    val commands = "start => get json stream\n"
    ws.onConnect(session => session.send("WELCOME!\n" + "Command list:\n" + commands))
    ws.onMessage((session, message) => {
      message match {
        case "start" =>
          t.getStream().subscribe(jsonElem => session.getRemote.sendString(jsonElem))
        case _ => session.getRemote.sendString("UNKNOWN COMMAND!\n" + commands)
      }
    })
  })

  Thread.sleep(10000)

  d1.foreach {
    drv =>
      DevicesManager.createDevice("t3", "", Encodings.PDF, new URI(""), drv)
      DevicesManager.deleteDevice(0)
      println("DELETED!")
  }
}