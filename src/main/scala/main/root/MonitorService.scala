package main.root

import java.util.concurrent.Executors

import api.config.Preferences
import api.events.EventBus
import api.events.SensorsHubEvents.{DeviceCreated, DeviceDeleted}
import api.internal.{DeviceController, TaskingSupport}
import api.sensors.DevicesManager
import com.fasterxml.jackson.core.JsonParseException
import io.javalin.embeddedserver.jetty.websocket.WebSocketHandler
import io.reactivex.disposables.Disposable
import io.reactivex.schedulers.Schedulers
import io.reactivex.subjects.PublishSubject
import org.apache.log4j.BasicConfigurator
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods._
import spi.service.{Service, ServiceMetadata}

import scala.collection.concurrent.TrieMap
import scala.io.Source

private case class Procedure(sensor: String, rules: List[Rule])
private case class Rule(sign: String, threshold: Double, alarm: Int)

private case class AlarmData(alarmType: String, sender: Int, level: Int)

class MonitorService extends Service {
  //avoid log4j warning
  BasicConfigurator.configure()
  implicit val _ = DefaultFormats

  private[this] var webSocket: MonitorServiceWebSocket = _
  private[this] var rules: List[Procedure] = _

  Preferences.configure("sh-prefs.conf")

  def valueChecker(sensorType: String, value: Any): Int = {

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
    if(ret != 0) alarmTrigger(sensorType, ret)
    ret
  }

  def alarmTrigger(sensorType: String, alarmValue: Int): Unit = {
    DevicesManager.devices().map(dev => dev.driver).find(_.metadata.name equals "simulatedBellDriver").foreach { drv =>
      drv.controller match {
        case ctrl: DeviceController with TaskingSupport =>
          ctrl.send("ring-bell", "{\"duration\":"+alarmValue+",\"sleep\":750}")
      }
    }
  }

  private[this] val sensorStreams = TrieMap[Int, List[Disposable]]()
  private[this] val jSonStream = PublishSubject.create[String]()

  def getStream(): PublishSubject[String] = jSonStream

  override def init(metadata: ServiceMetadata): Unit = {

    rules = (parse(Source.fromFile(metadata.rootDir + "/assets/config/thresholds.json").mkString) \ "allRules").extract[List[Procedure]]
    webSocket = MonitorServiceWebSocket(this, showLog = false, 7000)

    EventBus.events.subscribe(_ match {
      case deviceCreated: DeviceCreated =>
        deviceCreated.ds.dataStreams.foreach(stream => {
          sensorStreams += deviceCreated.ds.id -> (sensorStreams.getOrElse(deviceCreated.ds.id, List()) :+
            stream.observable.map[String](elem => {
              var jsonElem = JObject()
              jsonElem ~= ("name" -> deviceCreated.ds.name)
              jsonElem ~= ("type" -> elem.parentDataStream.observedProperty.name)
              jsonElem ~= ("value" -> elem.result.toString.toDouble)
              jsonElem ~= ("timestamp" -> elem.resultTime.toString)
              jsonElem ~= ("level" -> valueChecker(elem.parentDataStream.observedProperty.name, elem.result))
              compact(render(jsonElem))
            }).subscribe(elem => jSonStream.onNext(elem)))
        })
      case deviceDeleted: DeviceDeleted =>
        sensorStreams(deviceDeleted.ds.id).foreach(_.dispose())
      case _ =>
    })
  }

  override def start(): Unit = webSocket.start()

  override def restart(): Unit = {}

  override def dispose(): Unit = {
    sensorStreams.foreach(entry => entry._2.foreach(_.dispose()))
    webSocket.stop()
  }
}

private class MonitorServiceWebSocket(
   private[this] val monitorService: MonitorService,
   private[this] val showLog: Boolean = true,
   private[this] val port: Int = 8000) {
  import io.javalin.Javalin

  showLog match {
    case true =>
    case false =>
      //Remove jetty log
      System.setProperty("org.eclipse.jetty.util.log.class", "org.eclipse.jetty.util.log.StdErrLog")
      System.setProperty("org.eclipse.jetty.LEVEL", "OFF")

      import org.apache.log4j.{Level, LogManager}
      import scala.collection.JavaConverters._
      LogManager.getCurrentLoggers.asScala foreach {
        case l: org.apache.log4j.Logger =>
          if(!l.getName.startsWith("sh.")) l.setLevel(Level.OFF)
      }
  }

  private[this] val javalinWs = Javalin.create()
  javalinWs.port(port)
  javalinWs.ws("/jsonStream", (ws: WebSocketHandler) => {
    ws.onConnect(session => monitorService.getStream().observeOn(Schedulers.from(Executors.newSingleThreadExecutor()))
      .subscribe(jsonElem => session.send(jsonElem)))
    ws.onMessage((session, message) => {
      def soundAlarm(duration: Int, sleep: Int): Unit = {
        DevicesManager.devices().map(dev => dev.driver).find(_.metadata.name equals "simulatedSoundDriver").foreach { drv =>
          drv.controller match {
            case ctrl: DeviceController with TaskingSupport =>
              ctrl.send("play-sound", "{\"duration\":"+duration+",\"sleep\":"+sleep+"}")
          }
        }
      }
      implicit val _ = DefaultFormats
      try {
        val response = parse(message).extract[AlarmData]
        soundAlarm(response.level,500)
      }
      catch {
        case _: JsonParseException =>
          //session.send("{\"error\":\"malformed json\"}")
      }
    })
  })

  def start(): Unit = javalinWs.start()

  def stop(): Unit = javalinWs.stop()
}

private object MonitorServiceWebSocket {
  def apply(monitorService: MonitorService, showLog: Boolean = true, port: Int = 8000): MonitorServiceWebSocket =
    new MonitorServiceWebSocket(monitorService, showLog, port)
}

object MonitorServiceTest extends App {
  import java.net.URI
  import api.internal.DriversManager
  import api.sensors.Sensors.Encodings
  import fi.oph.myscalaschema.extraction.ObjectExtractor

  //url: ws://localhost:7000/jsonStream
  //send: {"sender":1,"alarmType":"temp","level":2}

  private def createSensor(driverName: String, configFile: String = "", sensorName: String): api.devices.Devices.Device = {
    val driver = DriversManager.instanceDriver(driverName)
    var device: api.devices.Devices.Device = null
    driver.foreach {
      drv =>
        drv.controller.init()
        drv.controller.start()
        if(!configFile.equals("")) drv.config.configure(configFile)
        device = DevicesManager.createDevice(sensorName, "", Encodings.PDF, new URI(""), drv)
    }
    device
  }

  private val service = new MonitorService()
  service.init(ServiceMetadata("monitorServiceTest","0","test service",System.getProperty("user.dir")))
  service.start()

  ObjectExtractor.overrideClassLoader(DriversManager.cl)

  createSensor("simulatedTemperatureDriver", "temperature.conf", "temperature")
  createSensor("simulatedHeartbeatDriver", "heartbeat.conf", "heartbeat")
  createSensor(driverName = "simulatedBellDriver", sensorName = "bell")
  createSensor(driverName = "simulatedSoundDriver", sensorName = "sound")
  //private val complexDev: api.devices.Devices.Device = createSensor(driverName = "simulatedComplexDeviceDriver", sensorName = "complex")

  //service.getStream().subscribe(e => println(e))

  Thread.sleep(5000)

  //DevicesManager.deleteDevice(complexDev.id)
  println("deleted complexDevice")

}