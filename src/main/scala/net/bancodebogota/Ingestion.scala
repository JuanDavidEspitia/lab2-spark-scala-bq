package net.bancodebogota

import org.apache.spark.sql.DataFrame
import net.bancodebogota.Main._
import org.apache.spark.sql.functions.{col, concat, lit, sha2, to_date, trim}

class Ingestion (
                  pathInput: String,
                  schema: String,
                  table: String,
                  partition: String
                ){

  var dfSource:DataFrame = spark.emptyDataFrame
  var delimiter:String = "|"
  val tmpBucketGSC:String = "bdb-gcp-qa-cds-storage-dataproc"

  def process (): Unit ={

    dfSource = spark.read.format("com.databricks.spark.csv")
      .option("delimiter", delimiter)
      .option("header", "true")
      .option("quoteMode", "NONE")
      .option("charset","UTF-8")
      .option("inferSchema", "true")
      .option("treatEmptyValuesAsNulls","true")
      .option("nullValue", "null")
      .option("timestampFormat", "yyyy-MM-dd hh:mm:ss")
      .option("mode", "PERMISSIVE")
      .load(pathInput)
    println(s"The numbers rows are: ${dfSource.count()}")
    println(s"The number colums are: ${dfSource.columns.length}")
    dfSource.show(5)
    dfSource.printSchema()
    println(s"s[END] Load data table of: ${table}")

    /**
     * Transformaciones
     */
    dfSource = dfSource.withColumn("hash_value",
      sha2(concat(trim(col("tipo_identificacion")), trim(col("numero_identificacion"))), 256))
      .drop(col("numero_identificacion"))
      .drop((col("tipo_identificacion")))
    dfSource.show(5)

    dfSource = dfSource.withColumn("partition", to_date(lit("2021-12-22")))


    /**
     * Escritura en BQ
     */
    println(s"s[START] Write data table in BQ With partition")
    dfSource.write
      .format("bigquery")
      .mode("append")
      .option("temporaryGcsBucket", tmpBucketGSC)
      .option("partitionField", "partition")
      .option("partitionType", "DAY")
      .option("table", schema + "." + table) //customers_dataset.customers_output  --> esta opcion quedara obsoleta a futuro
      .save()
    println(s"s[END] Write data table in BQ")




  }

}
