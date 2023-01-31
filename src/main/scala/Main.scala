import java.time.Duration
import java.util.Properties

import org.apache.kafka.streams.scala.StreamsBuilder
import org.apache.kafka.streams.scala.kstream._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.streams.kstream.{GlobalKTable, JoinWindows, TimeWindows, Windowed}
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala._
import org.apache.kafka.streams.scala.kstream.{KGroupedStream, KStream, KTable}
import org.apache.kafka.streams.scala.serialization.Serdes
import org.apache.kafka.streams.scala.serialization.Serdes._
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}

import java.time.Duration
import java.time.temporal.ChronoUnit
import java.util.Properties
import scala.concurrent.duration._
import Array._
import play.api.libs.json.Json


object Main extends App {

  import org.apache.kafka.streams.scala.serialization.Serdes._
  import org.apache.kafka.streams.scala.ImplicitConversions._

  println("Preparing...")
  val config: Properties = {
    val p = new Properties()
    p.put(StreamsConfig.APPLICATION_ID_CONFIG, "twc-stream-app-1")
    val bootstrapServers = if (args.length > 0) args(0) else "iot-kafka-0.iot-kafka-headless.iot:9092,iot-kafka-1.iot-kafka-headless.iot:9092,iot-kafka-2.iot-kafka-headless.iot:9092"
    p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)

    p
  }
  val builder = new StreamsBuilder()
  //pre filtering distance to adjust lid state
  val twcRaw: KStream[String, String] = builder.stream[String, String]("twc-values-demo")
  //State of the Lid -> o is closed, 1 -> opened
  var stol = 0
  var previousCorrectFill = 0
  case class twcValues(var stol: Int,var buzz: Int, reqp: Int, errm: Int, humi: Double, temp: Double, tvoc: Int, co2e: Int, dist: Int,var fill: Int)
  
  def dump[T](value:T):T = {
    println(value)
    value
  }

  def dumpKV[K,V](k:K,v:V):(K,V) = {
    //println(s"key=$k, value=$v")
    (k,v)
  }

  def dumpKVwPrint[K,V](k:K,v:V):(K,V) = {
    println(s"key=$k, value=$v")
    (k,v)
  }

  def cleanRawData(k:String,v:String):(String,String) = {
    val twcNoMagicByte = v.replaceAll("\u0000","").replace("\u0002","")

    val twcRawJson = Json.parse(twcNoMagicByte)

    var dataUnfiltered = twcValues(
        (twcRawJson \ "stol").as[Int],
        (twcRawJson \ "buzz").as[Int],
        (twcRawJson \ "reqp").as[Int],
        (twcRawJson \ "errm").as[Int],
        (twcRawJson \ "humi").as[Double],
        (twcRawJson \ "temp").as[Double],
        (twcRawJson \ "tvoc").as[Int],
        (twcRawJson \ "co2e").as[Int],
        (twcRawJson \ "dist").as[Int],
        (twcRawJson \ "fill").as[Int]
    )

    //Clean negative fill and turn it into lid = open, overwrite negative fill with previously correct fill level
    if (dataUnfiltered.fill < 0 || dataUnfiltered.dist > 50) {
        dataUnfiltered.stol = 1
        dataUnfiltered.fill = previousCorrectFill
    }else{
        previousCorrectFill = dataUnfiltered.fill
    }

    if (dataUnfiltered.stol == 0){
        dataUnfiltered.buzz = 0
    }

    val filteredStol = dataUnfiltered.stol
    val filteredBuzz = dataUnfiltered.buzz
    val filteredReqp = dataUnfiltered.reqp
    val filteredErrm = dataUnfiltered.errm
    val filteredHumi = dataUnfiltered.humi
    val filteredTemp = dataUnfiltered.temp
    val filteredTvoc = dataUnfiltered.tvoc
    val filteredCo2e = dataUnfiltered.co2e
    val filteredDist = dataUnfiltered.dist
    val filteredFill = dataUnfiltered.fill

    val transferFilteredData = s"""{"stol":$filteredStol,"buzz":$filteredBuzz,"reqp":$filteredReqp,"errm":$filteredErrm,"humi":$filteredHumi,"temp":$filteredTemp,"tvoc":$filteredTvoc,"co2e":$filteredCo2e,"dist":$filteredDist,"fill":$filteredFill}"""

    (k,transferFilteredData)
  }

  println("Starting...")
  val filteredTwcData: KStream[String, String] = twcRaw.map((a,b)=> cleanRawData(a,b))

  //Send clean output to new kafka topic                                         
  filteredTwcData.map((k,v)=>(k,v.toString())).to("twc-values-streamapp")


  val streams: KafkaStreams = new KafkaStreams(builder.build(), config)

  // Always (and unconditionally) clean local state prior to starting the processing topology.
  // We opt for this unconditional call here because this will make it easier for you to play around with the example
  // when resetting the application for doing a re-run (via the Application Reset Tool,
  // https://docs.confluent.io/platform/current/streams/developer-guide/app-reset-tool.html).
  //
  // The drawback of cleaning up local state prior is that your app must rebuilt its local state from scratch, which
  // will take time and will require reading all the state-relevant data from the Kafka cluster over the network.
  // Thus in a production scenario you typically do not want to clean up always as we do here but rather only when it
  // is truly needed, i.e., only under certain conditions (e.g., the presence of a command line flag for your app).
  // See `ApplicationResetExample.java` for a production-like example.
  
  streams.cleanUp()
  streams.start()
  

  // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(10))
  }

}