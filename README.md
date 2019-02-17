# kc-mod-bd-processing
Práctica del módulo big data processing del Bootcamp III Edición

## Entrega

- Se ha generado una aplicación Scala con IntelliJ y sbt para la gestión de la configuración.
  - Directorio: bd-processing-project
- Se ha generado un directorio con un dataset inicial en csv
  - Directorio: datasets
- Se ha generado un directorio donde se configurarán los checkpoints de la aplicación Scala
  - Directorio: checkpoints
  - Contenido agregado al fichero .gitignore
- Se ha generado un directorio para dejar los resultados de la ejecución de la fase I
  - Directorio: real-state
- Se ha generado un directorio con un dataset inicial en csv
  - fichero: replicarParaPruebas.json

### Aplicación

El código se ha organizado en tres paquetes principales
- common
  - Contiene dos objetos Scala cuyo contenido es utilizado por el resto de la aplicación:
    - Constants.scala Contiene valores constantes para la configuración de rutas de la aplicación
    - Utils.scala Contiene la definición de la configuración del nivel de log
- fasei
  - Contiene el objeto FaseI: Este objeto tiene la lógica de la fase I. Toma como entrada el fichero RealState.csv del directorio datasets. El resultado lo deja en el directorio real-state
- faseii
  - Contiene el objeto FaseII: Este objeto tiene la lógica de la fase II. Toma como entrada los ficheros .json que encuentre en el directorio real-state. El resultado de esta ejecución no escribe en el file system, simula un envío de correo.


