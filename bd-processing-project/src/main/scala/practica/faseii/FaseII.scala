package practica.faseii

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import practica.Main.args
import practica.common.Constants
import practica.common.Utils._

object FaseII {

  // Definición de la estructura de los datos leídos
  val realStateAveragePriceSchema = new StructType()
      .add("localidad", StringType)
      .add("precioMetro2medio", DoubleType)
      .add("alertaEnviada", BooleanType)

  def main(args: Array[String]): Unit = {

    var lim: Double = 3500

    if (args.length >= 1) {
      lim = args(0).toDouble
    }

    println("Configurando el valor del límite a: " + lim)

    // Definición de constantes
    val sourceDirectoryPath = Constants.realStatePath
    val checkpointPath = s"${Constants.checkpointsPath}/fase-ii"

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
      .withColumn("alertaEnviada", lit(false))


    // Realizamos la agrupación por localidad, precio medio y lo introducimos en una ventana temporal de 1 hora
    val realStatesOrderedData = realStatesRawDF.groupBy($"localidad", $"precioMetro2medio", $"alertaEnviada", window(org.apache.spark.sql.functions.current_timestamp(), "1 day"))
      .count().orderBy($"precioMetro2medio".desc)


    def enviarAlerta(realStatesAlerta: DataFrame) = {
      println(s"Enviando email de alerta generada por los inmuebles con precio medio mayor que (${broadcastLim.value}€/m2" +
        s"):")
      realStatesAlerta.collect().foreach(println)
      // enviarEmail(realStatesAlerta)
    }

    // Enviamos las alertas con los inmuebles que superen el límite
    // La idea era agrupar los posibles inmuebles repetidos en una ventana temporal de 1 día
    // con el fin de no generar varias alertas para un mismo inmueble repetido con el mismo valor.
    // El caso es que con el planteamiento actual, cada vez que se ejecute el siguiente filtro,
    // el valor que le dará el streaming será de envío de alerta a false, por lo que de momento,
    // cualquier inmueble que supere el límite generará dicha alerta :/
    val queryEnvioAlertas = realStatesOrderedData.writeStream
      .format("console")
      .outputMode("complete")
      .foreachBatch{ (batchDF: DataFrame, batchId: Long) => {

        println("Analizando batchId:" + batchId)
        val realStatesDFAlerta = batchDF.filter(r => r.getDouble(1) > broadcastLim.value && !r.getBoolean(2))
            .map(r => {
              Row(r.getString(0), r.getDouble(1), true)
            })(RowEncoder(realStateAveragePriceSchema))
            .toDF()

        enviarAlerta(realStatesDFAlerta)
      } }
      .start()


    queryEnvioAlertas.awaitTermination()

    sparkSession.stop()

  }

}
