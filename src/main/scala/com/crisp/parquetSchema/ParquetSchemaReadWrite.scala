package com.crisp.parquetSchema

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SQLContext, SaveMode}
import org.apache.spark.{SparkConf, SparkContext}

case class FileSchema(fileName: String, fileSchema: String)

object ParquetSchemaReadWrite {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("ParquetSchema").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)

    val entityDataFilePath = "s3://atom-dev-edl/harsh-8aug/entityDataFilePath.csv"
    val entityDataFileDF = sqlContext.read.option("header", "true").option("delimiter", ",").csv(entityDataFilePath)
    val entityDataFilePathList = entityDataFileDF.select("path").rdd.map(_.getString(0)).collect().toList

    import sqlContext.implicits._
    val fileSchemaDF = entityDataFilePathList.map { entityDataFilePath =>
      getParquetSchema(sqlContext, entityDataFilePath)
    }.toDF()

    fileSchemaDF
      .coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .option("delimiter", ",")
      .csv("s3://atom-dev-edl/harsh-8aug/outputSchemaEntityDataFilePath.csv")
  }

  def getParquetSchema(sqlContext: SQLContext, path: String): FileSchema = {

    val fileSystem = FileSystem.get(new Configuration())
    val exists = fileSystem.exists(new Path(path))

    if (exists) {
      val parquetFileSchema = sqlContext.read.parquet(path).schema

      val fileNameWithTypeOption = path.split("/").lastOption
      val fileNameWithType = fileNameWithTypeOption.getOrElse(
        throw new IllegalArgumentException(s"File path ${path} is invalid")
      )
      val fileNameOption = fileNameWithType.split("\\.").headOption
      val fileName = fileNameOption.getOrElse(
        throw new IllegalArgumentException(s"File Name ${fileNameWithType} doesn't contain file type")
      )
      FileSchema(fileName, parquetFileSchema.toString)
    } else {
      throw new IllegalArgumentException(s"File with path ${path} does not exist")
    }

  }
}
