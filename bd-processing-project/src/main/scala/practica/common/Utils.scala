package practica.common

object Utils {

  def setupLogging() = {

    import org.apache.log4j.{Level, Logger}
    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
  }
}
