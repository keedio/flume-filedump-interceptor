package org.keedio.flume.interceptor

import java.io.OutputStream
import java.nio.file.FileSystems
import java.util

import ch.qos.logback.classic.{Logger, LoggerContext}
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.encoder.Encoder
import ch.qos.logback.core.rolling.{SizeBasedTriggeringPolicy, FixedWindowRollingPolicy, RollingFileAppender}
import ch.qos.logback.core.Appender
import com.typesafe.scalalogging.slf4j.LazyLogging

import org.slf4j.LoggerFactory

import org.apache.flume.{Context, Event}
import org.apache.flume.interceptor.Interceptor
import org.apache.flume.serialization.{HeaderAndBodyTextEventSerializer, EventSerializer}
import scala.collection.JavaConversions._


/**
 * Simple Flume Interceptor that dumps the incoming
 * flow of events to a log file.
 *
 * Created by luca on 10/2/15.
 */

class FileDumpInterceptor(ctx: Context) extends Interceptor with LazyLogging {

  val ROOT_TMP_DIR = FileSystems.getDefault.getPath(System.getProperty("java.io.tmpdir"))
  val FILELOGGER_APPENDER_NAME = "FILELOGGER"
  private val flumeContext: Context = ctx

  private var outputStream: OutputStream = _
  private var serializer: EventSerializer = _
  private var fileLogger: Logger = _

  /**
   * {@inheritdoc}
   */
  override def initialize(): Unit = {

    initFileLogger match {
      case (l, s) => fileLogger = l; outputStream = s
    }

    val serBuilder = new HeaderAndBodyTextEventSerializer.Builder
    serializer = serBuilder.build(ctx, outputStream)
  }

  /**
   * {@inheritdoc}
   */
  override def close(): Unit = {
    // outputStream.close()
  }

  /**
   * {@inheritdoc}
   */
  override def intercept(event: Event): Event = {
    fileLogger.info("headers: " + event.getHeaders.toString + " body: " + new String(event.getBody))
    // serializer.write(event)
    event
  }

  /**
   * {@inheritdoc}
   */
  override def intercept(events: util.List[Event]): util.List[Event] = {
    events.foreach(intercept)
    events
  }

  /**
   * Creates a temp file located in the system-wide temp dir
   * @return
   */

  def initFileLogger: (Logger, OutputStream) = {
    val dumpFile: String = flumeContext.getString("dump.filename")
    val maxFileSize = flumeContext.getString("dump.maxFileSize")
    val maxBackups = flumeContext.getInteger("dump.maxBackups")

    logger.debug("Building rolling file logger using: " + dumpFile + " " + maxFileSize + " " + maxBackups)

    val loggerContext = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]

    val fileAppender = new RollingFileAppender
    fileAppender.setContext(loggerContext)
    fileAppender.setName(FILELOGGER_APPENDER_NAME)
    fileAppender.setFile(dumpFile)
    fileAppender.setAppend(true)

    val encoder = new PatternLayoutEncoder()
    encoder.setContext(loggerContext)
    encoder.setPattern("%msg%n")
    encoder.start()

    val rollingPolicy = new FixedWindowRollingPolicy
    rollingPolicy.setContext(loggerContext)
    rollingPolicy.setParent(fileAppender)
    rollingPolicy.setFileNamePattern(dumpFile + "%i")
    rollingPolicy.setMinIndex(1)
    rollingPolicy.setMaxIndex(maxBackups)

    val triggeringPolicy = new SizeBasedTriggeringPolicy
    triggeringPolicy.setContext(loggerContext)
    triggeringPolicy.setMaxFileSize(maxFileSize)

    fileAppender.setEncoder(encoder.asInstanceOf[Encoder[Nothing]])
    fileAppender.setTriggeringPolicy(triggeringPolicy)
    fileAppender.setRollingPolicy(rollingPolicy)

    fileLogger = LoggerFactory.getLogger("fileLogger").asInstanceOf[Logger]
    fileLogger.setAdditive(true)
    fileLogger.addAppender(fileAppender.asInstanceOf[Appender[ILoggingEvent]])

    // TODO: check order
    fileAppender.start()
    rollingPolicy.start()
    triggeringPolicy.start()

    (fileLogger, fileAppender.getOutputStream)
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
