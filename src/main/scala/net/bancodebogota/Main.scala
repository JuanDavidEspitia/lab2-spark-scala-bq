package net.bancodebogota

import org.apache.spark.sql.SparkSession
import scopt.OptionParser

case class Config (
                  pathInput: String = null,
                  schema: String = null,
                  table: String = null,
                  partition: String = null
                  )

object Main {

  //Tipos de variables
  //Var: mutables
  //val: Inmutables
  var pathInput: String = null
  var schema: String = null
  var table: String = null
  var partition: String = null

  var nameApp: String = "LabIngestToBQ"
  val spark = SparkSession.builder
    .appName(nameApp)
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    if(args.length > 0) {

      val parser = new OptionParser[Config]("LabIngestToBQ") {
        head("scopt", "1.0", "Permite leer los argumentos")
        opt[String]('i', "pathInput")
          .required()
          .action((x, c) => c.copy(pathInput = x))
          .text("Path de entrada de los datos")
        opt[String]('s', "schema")
          .required()
          .action((x, c) => c.copy(schema = x))
          .text("Schema/Dataset de los datos destino")
        opt[String]('t', "table")
          .required()
          .action((x, c) => c.copy(table = x))
          .text("Tabla de los datos destino")
        opt[String]('p', "partition")
          .required()
          .action((x, c) => c.copy(partition = x))
          .text("Particion de Tabla de los datos en el destino")
        help("help").text("Imprime la ayuda de argumentos")

      }

        parser.parse(args, Config()) map { config =>
          pathInput =  config.pathInput
          schema = config.schema
          table =  config.table
          partition = config.partition

          /**
           * Instanciar la clase de Ingestion
           */
          var ingestion =  new Ingestion(pathInput, schema, table, partition)
          ingestion.process()
          spark.stop()
        }

    } else {
      println("No hay argumentos para trabajar")
    }









  }






}
