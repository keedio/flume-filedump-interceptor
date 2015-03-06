package org.keedio.flume.interceptor

import java.io.OutputStream
import java.nio.file.{FileSystems, Files, Path}
import java.util

import ch.qos.logback.classic.{Logger, LoggerContext}
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.rolling.{SizeBasedTriggeringPolicy, FixedWindowRollingPolicy, RollingFileAppender}
import com.typesafe.scalalogging.slf4j.LazyLogging
import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor
import org.apache.flume.serialization.{HeaderAndBodyTextEventSerializer, EventSerializer}
import ch.qos.logback.core.Appender
import org.slf4j.LoggerFactory
import scala.collection.JavaConversions._



/**
 * Simple Flume Interceptor that dumps the incoming
 * flow of events to an append-only file.
 *
 * Created by luca on 10/2/15.
 */
class FileDumpInterceptor(ctx: Context) extends Interceptor with LazyLogging {
  val ROOT_TMP_DIR = FileSystems.getDefault.getPath(System.getProperty("java.io.tmpdir"))

  private var outputStream: OutputStream = _
  private var serializer: EventSerializer = _
  private var fileLogger :Logger = _

  /**
   * {@inheritdoc}
   */
  override def initialize(): Unit = {
    val serBuilder = new HeaderAndBodyTextEventSerializer.Builder

    val dumpFile :String = ctx.getString("dump.filename")
    val maxFileSize = ctx.getString("dump.maxFileSize")
    val maxBackups = ctx.getInteger("dump.maxBackups")

    initFileLogger(dumpFile, maxFileSize, maxBackups)

    // outputStream = Some(Files.newOutputStream(dumpFile.get, CREATE_NEW, APPEND))
    serializer = serBuilder.build(ctx, outputStream)
  }

  /**
   * {@inheritdoc}
   */
  override def close(): Unit = {
    fileLogger.detachAndStopAllAppenders
    // outputStream.get.close()
  }

  /**
   * {@inheritdoc}
   */
  override def intercept(event: Event): Event = {
    // fileLogger.info(event.toString)
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
  // def createTmpFile : Path = Files.createTempFile(ROOT_TMP_DIR, "flume-dumpfile-", null)

  def initFileLogger(dumpFile :String, maxFileSize :String = "100MB", maxBackups :Int = 10): Unit = {
    val loggerContext = LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]

    val rollingPolicy = new FixedWindowRollingPolicy
    rollingPolicy.setMinIndex(1)
    rollingPolicy.setMaxIndex(maxBackups)
    rollingPolicy.start()

    val triggeringPolicy = new SizeBasedTriggeringPolicy
    triggeringPolicy.setMaxFileSize(maxFileSize)
    triggeringPolicy.start()

    val fileAppender = new RollingFileAppender
    fileAppender.setName("fileLogger");
    fileAppender.setContext(loggerContext);
    fileAppender.setFile(dumpFile);
    fileAppender.setAppend(true)
    fileAppender.setRollingPolicy(rollingPolicy)
    fileAppender.setTriggeringPolicy(triggeringPolicy)

    val encoder = new PatternLayoutEncoder();
    encoder.setContext(loggerContext);
    encoder.setPattern("%msg%n");
    encoder.start();

    fileAppender.setEncoder(encoder.asInstanceOf[Encoder[Nothing]]);
    fileAppender.start();

    fileLogger = loggerContext.getLogger(classOf[org.keedio.flume.interceptor.FileDumpInterceptor])
    fileLogger.addAppender(fileAppender.asInstanceOf[Appender[ILoggingEvent]]);

    outputStream = fileAppender.getOutputStream

    logger.info(s"Setting log for event dumping to ${dumpFile}")

  }
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
