package org.keedio.flume.interceptor

import java.io.File
import java.net.InetSocketAddress

import com.google.common.base.Charsets
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.avro.ipc.NettyServer
import org.apache.avro.ipc.specific.SpecificResponder
import org.apache.commons.lang.RandomStringUtils
import org.apache.flume.agent.embedded.EmbeddedAgent
import org.apache.flume.event.EventBuilder
import org.apache.flume.source.avro.AvroSourceProtocol
import org.keedio.flume.interceptor.collector.EventCollector
import org.testng.Assert._
import org.testng.annotations._

import scala.collection.JavaConversions._

/**
 * Tests the FileDumpInterceptor using an embedded flume agent.
 *
 * Created by Luca Rosellini <lrosellini@keedio.com> on 10/2/15.
 */
class FileDumpInterceptorTest extends LazyLogging {
  val port = 44444
  val hostname = "localhost"
  var agent: EmbeddedAgent = _
  val eventCollector = new EventCollector
  val responder = new SpecificResponder(classOf[AvroSourceProtocol], eventCollector)
  val nettyServer = new NettyServer(responder, new InetSocketAddress(hostname, port))

  var dumpFile: String = _

  @BeforeSuite
  def createAgent(): Unit = {
    nettyServer.start()

    // give the server a second to start
    Thread.sleep(500L)
  }

  @AfterSuite
  def stopNetty(): Unit = {
    nettyServer.close()
  }

  @BeforeMethod
  def initAgent(): Unit = {
    dumpFile = s"${System.getProperty("java.io.tmpdir")}${File.separator}filedumpinterceptortest"
    logger.info("Log files: " + dumpFile)

    val properties = Map(
      "channel.type" -> "memory",
      "channel.capacity" -> "100",
      "sinks" -> "sink1",
      "sink1.type" -> "avro",
      "sink1.hostname" -> hostname,
      "sink1.port" -> port.toString,
      "processor.type" -> "default",
      "source.interceptors" -> "i1",
      "source.interceptors.i1.dump.filename" -> dumpFile,
      "source.interceptors.i1.dump.maxFileSize" -> "1kb",
      "source.interceptors.i1.dump.maxBackups" -> "10",
      "source.interceptors.i1.type" -> "org.keedio.flume.interceptor.FileDumpInterceptorBuilder")

    agent = new EmbeddedAgent("testagent")
    agent.configure(properties)
    agent.start()
  }

  @AfterMethod
  def stopAgent(): Unit = {
    agent.stop()
    getLogFiles.foreach(f => f.delete())
  }

  def assertDumpFileLength(l: Int): Unit = {
    val lines = getLogFiles map { f => val src = io.Source.fromFile(f)
      val l = src.getLines().size
      src.close()
      l
    }
    assertEquals(lines.sum, l)
  }

  def getLogFiles: Array[File] = {
    new File(System.getProperty("java.io.tmpdir")).listFiles.filter(_.getName.startsWith("filedumpinterceptortest"))
  }

  @Test(enabled = false)
  def testPut(): Unit = {

    val headers = Map("key1" -> "value1")
    val body = "body".getBytes(Charsets.UTF_8)
    agent.put(EventBuilder.withBody(body, headers))

    val event = Iterator.continually({
      Thread.sleep(100)
      eventCollector.poll()

    }).dropWhile(_ == null).next()

    assertNotNull(event)

    assertEquals(event.getHeaders.size(), 1)
    assertTrue(event.getHeaders.containsKey("key1"))
    assertEquals(event.getHeaders.get("key1"), "value1")
    assertNotNull(event.getBody)
    assertEquals(event.getBody, body)

    assertDumpFileLength(1)
  }

  @Test
  def testPutAll(): Unit = {

    val numEvents = 100

    val body = "body"

    val events = for (i <- 0 to numEvents - 1;
                      body = RandomStringUtils.randomAscii(i).getBytes(Charsets.UTF_8);
                      headers = Map(s"key$i" -> s"value$i")) yield {
      EventBuilder.withBody(body, headers)
    }
    agent.putAll(events)

    val resEvents = Iterator.continually({
      Thread.sleep(50)
      eventCollector.poll()

    }).dropWhile(_ == null).take(numEvents)

    assertNotNull(resEvents)
    assertFalse(resEvents.hasDefiniteSize)

    resEvents.zipWithIndex.foreach({ case (e, idx) => assertTrue(e.getHeaders.containsKey(s"key$idx"))})
    resEvents.zipWithIndex.foreach({ case (e, idx) => assertEquals(e.getHeaders.get(s"key$idx"), s"value$idx")})
    resEvents.zipWithIndex.foreach({ case (e, idx) => assertNotNull(e.getBody)})
    resEvents.zipWithIndex.foreach({ case (e, idx) => assertTrue(e.getBody.length == idx)})
    assertDumpFileLength(numEvents)
  }
}
