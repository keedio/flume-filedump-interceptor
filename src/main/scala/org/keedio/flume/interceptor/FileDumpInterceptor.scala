package org.keedio.flume.interceptor

import java.io.OutputStream
import java.nio.file.{OpenOption, FileSystems, Files, Path}
import java.util

import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor
import org.apache.flume.serialization.{HeaderAndBodyTextEventSerializer, EventSerializer}
import scala.collection.JavaConversions._
import java.nio.file.StandardOpenOption._



/**
 * Simple Flume Interceptor that dumps the incoming
 * flow of events to an append-only file.
 *
 * Created by luca on 10/2/15.
 */
class FileDumpInterceptor(ctx: Context) extends Interceptor with LazyLogging {
  val ROOT_TMP_DIR = FileSystems.getDefault.getPath(System.getProperty("java.io.tmpdir"))

  private var outputStream: Option[OutputStream] = None
  private var serializer: EventSerializer = _

  /**
   * {@inheritdoc}
   */
  override def initialize(): Unit = {
    val serBuilder = new HeaderAndBodyTextEventSerializer.Builder

    val dumpFile = Some(FileSystems.getDefault.getPath(ctx.getString("dump.filename")))

    outputStream = Some(Files.newOutputStream(dumpFile.get, CREATE_NEW, APPEND))
    logger.info(s"Dumping events to ${dumpFile.get.toFile.getAbsolutePath}")
    serializer = serBuilder.build(ctx, outputStream.get)
  }

  /**
   * {@inheritdoc}
   */
  override def close(): Unit = {
    outputStream.get.close()
  }

  /**
   * {@inheritdoc}
   */
  override def intercept(event: Event): Event = {
    serializer.write(event)
    event
  }

  /**
   * {@inheritdoc}
   */
  override def intercept(events: util.List[Event]): util.List[Event] = {
    events.foreach(intercept(_))
    events
  }

  /**
   * Creates a temp file located in the system-wide temp dir
   * @return
   */
  def createTmpFile : Path = Files.createTempFile(ROOT_TMP_DIR, "flume-dumpfile-", null)
}

/**
 * File dumper interceptor builder
 */
class FileDumpInterceptorBuilder extends Interceptor.Builder {
  private var ctx: Context = _

  /**
   * {@inheritdoc}
   */
  override def build(): Interceptor = new FileDumpInterceptor(ctx)

  /**
   * {@inheritdoc}
   */
  override def configure(context: Context): Unit = ctx = context
}
