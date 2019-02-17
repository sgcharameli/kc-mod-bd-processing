package practica.fasei

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import practica.common.Constants
import practica.common.Utils.setupLogging

object FaseI {

  def main(args: Array[String]): Unit = {

    val datasetsPath = Constants.datasetsPath
    val resultsPath = Constants.realStatePath
    val checkpointPath = s"${Constants.checkpointsPath}/fase-i"

    val sparkSession = SparkSession.builder()
      .appName("FaseI")
      .master("local[*]")
      .config("spark.sql.streaming.checkpointLocation", s"${checkpointPath}")
      .getOrCreate()

    // Configuración del nivel de logs
    setupLogging()

    import sparkSession.implicits._

    // Definimos constantes propias de este objeto
    val dolarEuroCoef: Double = 0.89
    val squareFeetSquareMetersCoef: Double = 0.09290304

    // Realizamos el broadcast de las constantes al cluster
    val broadcastDolarEuroCoef = sparkSession.sparkContext.broadcast(dolarEuroCoef)
    val broadcastFeetMetersCoef = sparkSession.sparkContext.broadcast(squareFeetSquareMetersCoef)

    // Realizamos la lectura del fichero de entrada en formato csv
    val realStatesRaw = sparkSession.read.option("header", true)
      .option("delimiter", ",")
      .option("inferschema", true)
      .csv(s"${datasetsPath}RealEstate.csv")

    // Mostramos el esquema inferido
    println("Mostrando información del esquema inferido:")
    realStatesRaw.printSchema()

    // Realizamos las transformaciones correspondientes
    val realStates = realStatesRaw.map(row => (row.getString(1).trim.toUpperCase, row.getDouble(2), row.getInt(5)))
      .map(row => (row._1, row._2 * broadcastDolarEuroCoef.value, row._3))
      .map(row => (row._1, row._2, row._3 * broadcastFeetMetersCoef.value))
      .map(row => (row._1, row._2 / row._3))

    // Convertimos a Dataframe, damos nombre a las columnas y cacheamos después de las transformaciones
    val realStatesDF = realStates.toDF("localidad", "precioMetro2").cache()
    realStatesDF.show()
    realStatesDF.printSchema()

    // Realizamos la agrupación por localidad y renombramos la columna agregada
    val realStatesAveragePriceGroupedDF = realStatesDF.groupBy("localidad").agg(avg("precioMetro2").alias("precioMetro2medio")).orderBy("localidad")

    // Compactamos las particiones y escribimos a file system el resultado
    realStatesAveragePriceGroupedDF.coalesce(1).write.format("json").save(s"${resultsPath}")

    // Liberamos las constantes del cluster
    broadcastDolarEuroCoef.unpersist
    broadcastFeetMetersCoef.unpersist
  }
}
