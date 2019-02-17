package practica.faseii

import org.apache.spark.sql.types.{DateType, DoubleType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import practica.common.Constants
import practica.common.Utils._

object FaseII {

  // Definición de la estructura de los datos leídos
  val realStateAveragePriceSchema = new StructType()
      .add("localidad", StringType)
      .add("precioMetro2medio", DoubleType)
      .add("timestamp", DateType)

  def main(args: Array[String]): Unit = {

    // Definición de constantes
    val sourceDirectoryPath = Constants.realStatePath
    val checkpointPath = s"${Constants.checkpointsPath}/fase-ii"

    // Definimos constantes propias de este objeto
    val lim: Double = 3500

    // Construcción de la sesión de Spark
    val sparkSession = SparkSession.builder()
      .appName("FaseII")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", s"${checkpointPath}")
      .getOrCreate()

    // Configuración del nivel de logs
    setupLogging()

    import sparkSession.implicits._

    // Realizamos el broadcast de las constantes al cluster
    val broadcastLim = sparkSession.sparkContext.broadcast(lim)

    // Configuración de la escucha de directorio.
    val realStatesRawDF = sparkSession.readStream
      .schema(realStateAveragePriceSchema)
      .json(sourceDirectoryPath)


    // Realizamos la agrupación por localidad, precio medio y lo introducimos en una ventana temporal de 1 hora
    val realStatesOrderedData = realStatesRawDF.groupBy($"localidad", $"precioMetro2medio", window(org.apache.spark.sql.functions.current_timestamp(), "1 hour"))
      .count().orderBy($"precioMetro2medio".desc)


    // Lanzar alerta
    realStatesOrderedData.writeStream
      .format("console")
      .outputMode("complete")
      .foreachBatch{ (batchDF: DataFrame, batchId: Long) => {

        println("###########################################################")
        println("Analizando batchDF schema:" + batchDF.printSchema())
        println("Analizando batchId:" + batchId)
        println("Analizando batchDF:" + batchDF.show())
        println("Cantidad de inmuebles:" + batchDF.count())
        println("###########################################################")

      } }
      .start().awaitTermination()



    //realStatesOrderedData.writeStream.format("console").outputMode("complete").start()



    def enviarAlerta(realStatesAlerta: DataFrame) = {
      println(s"Enviando email de alerta generada por los inmuebles con precio medio mayor que (${broadcastLim.value}€/m2" +
        s"):")
      realStatesAlerta.collect().foreach(println)
      // enviarEmail(realStatesAlerta)
    }

    // Enviamos las alertas con los inmuebles que superen el límite
    val queryEnvioAlertas = realStatesOrderedData.writeStream
      .format("console")
      .outputMode("complete")
      .foreachBatch{ (batchDF: DataFrame, batchId: Long) => {

        println("Analizando batchId:" + batchId)
        val realStatesDFAlerta = batchDF.filter(r => r.getDouble(1) > broadcastLim.value)
        enviarAlerta(realStatesDFAlerta)
      } }
      .start()

    queryEnvioAlertas.awaitTermination()


    sparkSession.stop()




  }

}
