package main

import scala.io.Source
import java.net.URI
import java.util.concurrent.Executors

import api.config.Preferences
import api.events.{EventBus, EventLogging}
import api.events.SensorsHubEvents.{DeviceCreated, DeviceDeleted}
import api.internal.{DeviceController, DriversManager, TaskingSupport}
import api.sensors.DevicesManager
import api.sensors.Sensors.Encodings
import fi.oph.myscalaschema.extraction.ObjectExtractor
import io.javalin.embeddedserver.jetty.websocket.WebSocketHandler
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import org.apache.log4j.{Level, LogManager}
import spi.service.{Service, ServiceMetadata}
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import scala.collection.concurrent.TrieMap

case class Procedure(sensor: String, rules: List[Rule])
case class Rule(sign: String, threshold: Double, alarm: Int)

class MonitoringService extends Service {
  implicit val _ = DefaultFormats

  private[this] val rules = (parse(Source.fromFile("assets/config/thresholds.json").mkString) \ "allRules").extract[List[Procedure]]

  Preferences.configure("sh-prefs.conf")

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

  private[this] val sensorStreams = TrieMap[Int, Disposable]()
  private[this] val jSonStream = PublishSubject.create[String]()

  EventBus.events.subscribe(_ match {
    case deviceCreated: DeviceCreated =>
      deviceCreated.ds.dataStreams.foreach(stream => {
        //adesso aggiungo l'id potrebbe non bastare se un sensore ha più sensori
        sensorStreams.put(deviceCreated.ds.id,
          stream.observable.map[String](elem => {
          var jsonElem = JObject()
          jsonElem ~= ("name" -> deviceCreated.ds.name) //potrebbe servire la pos e non solo il nome del sensore
          jsonElem ~= ("type" -> elem.parentDataStream.observedProperty.name)
          jsonElem ~= ("value" -> elem.result.toString.toDouble)
          jsonElem ~= ("timestamp" -> elem.resultTime.toString)
          jsonElem ~= ("alarm" -> valueValidator(elem.parentDataStream.observedProperty.name, elem.result))
          compact(render(jsonElem))
        }).subscribe(elem => jSonStream.onNext(elem)))
      })
    case deviceDeleted: DeviceDeleted =>
      sensorStreams(deviceDeleted.ds.id).dispose()
    case _ =>
  })

  def getStream(): PublishSubject[String] = jSonStream

  override def init(metadata: ServiceMetadata): Unit = {}

  override def restart(): Unit = {}

  override def dispose(): Unit = sensorStreams.foreach(stream => stream._2.dispose())

  override def start(): Unit = {}
}

object MonitoringServiceTest extends App {
  //Remove jetty log
  System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog")
  System.setProperty("org.eclipse.jetty.LEVEL", "OFF")

  import scala.collection.JavaConverters._
  LogManager.getCurrentLoggers.asScala foreach {
    case l: org.apache.log4j.Logger =>
      if(!l.getName.startsWith("sh.")) l.setLevel(Level.OFF)
  }

  val service = new MonitoringService()
  service.init(null)

  ObjectExtractor.overrideClassLoader(DriversManager.cl)

  val temperatureDriver = DriversManager.instanceDriver("driver 1")
  temperatureDriver.foreach {
    drv =>
      drv.controller.init()
      drv.controller.start()
      drv.config.configure("temperature.conf")
      DevicesManager.createDevice("temp", "", Encodings.PDF, new URI(""), drv)
  }

  val heartbeatDriver = DriversManager.instanceDriver("driver 2")
  heartbeatDriver.foreach {
    drv =>
      drv.controller.init()
      drv.controller.start()
      drv.config.configure("heartbeat.conf")
      DevicesManager.createDevice("heart", "", Encodings.PDF, new URI(""), drv)
  }

  val beepDriver = DriversManager.instanceDriver("driver 3")
  beepDriver.foreach {
    drv =>
      drv.controller.init()
      drv.controller.start()
      DevicesManager.createDevice("bell", "", Encodings.PDF, new URI(""), drv)
  }

  //service.getStream().subscribe(e => println(e))

  import io.javalin.Javalin

  val app = Javalin.start(args.headOption.getOrElse("8000").toInt)
  app.ws("/jsonStream", (ws: WebSocketHandler) => {
    ws.onConnect(session => service.getStream().observeOn(Schedulers.from(Executors.newSingleThreadExecutor()))
        .subscribe(jsonElem => session.send(jsonElem)))
    ws.onMessage((_, message) => {
      message match {
        case _ =>
          beepDriver.foreach {
            drv =>
              drv.controller match {
                case ctrl: DeviceController with TaskingSupport =>
                  ctrl.send("ring-bell", """{"duration":1000, "sleep":250}""").subscribe(e => println("maybe: " + e))
              }
          }
          println("unknown response: " + message)
      }
    })
  })
  /*
  Thread.sleep(10000)

  d1.foreach {
    drv =>
      DevicesManager.createDevice("t3", "", Encodings.PDF, new URI(""), drv)
      DevicesManager.deleteDevice(0)
      println("DELETED!")
  }
  */
}

object Malevole extends App {
    java.awt.Toolkit.getDefaultToolkit.beep()
}