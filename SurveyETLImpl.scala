package com.chartercomm.qualtrics.survey


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType


class SurveyETLImpl(spark: SparkSession) {

  import spark.implicits._

  def loadSurveyToDataLake(inputPath:String,hiveTableName:String, outputTablePath:String, genericRun:String = "false", stagingPath:String = "", stagingTableName:String = ""): Unit = {

    // Reading the input file to raw dataframe
    val raw_df = spark.read.option("escape","\"").option("multiLine","true").option("header", "true").option("ignoreLeadingWhiteSpace","true").option("ignoreTrailingWhiteSpace","true").csv(inputPath)

    // To filter the junk rows, we used type casting start date with timestamp . FIlter the nulls i.e. rows which dont have proper start date
    var processedDF = if(genericRun.equalsIgnoreCase("false")) raw_df.withColumn("StartDate", expr("CAST(StartDate as timestamp)")).filter("StartDate IS NOT NULL")
      .withColumn("EndDate", expr("CAST(EndDate as timestamp)")).withColumn("Duration", expr("CAST(`Duration (in seconds)` AS INT) AS Duration")).drop("Duration (in seconds)")
                  	  else raw_df

    // As HIve doesn't support space and "_" in column names, changing the column names as expected
    val spacedColumnNames = processedDF.columns.filter(col => col.contains(" ") || col.contains("+"))
    spacedColumnNames.foreach { spacedColName => {
      processedDF = processedDF.withColumn(spacedColName.replaceAll(" ", "_").replaceAll("\\+", "_"), expr(s"`${spacedColName}`")).drop(spacedColName)
    }
    }

    var stagingDF = processedDF

    // Load in the Staging Table if required

    if(!stagingPath.equals("") && !stagingTableName.equals("")){

      if(spark.catalog.tableExists(stagingTableName)) spark.sql(s"DROP TABLE ${stagingTableName}")

      val my_schema = processedDF.schema
      val cols = my_schema.fields.map(x => s"`${x.name}` ${x.dataType.typeName}").mkString(",")
      val creatStatement = s"CREATE TABLE ${stagingTableName} ($cols) STORED AS PARQUET location '$stagingPath'"
      print(creatStatement)
      spark.sql(creatStatement)
      processedDF.write.format("parquet").option("compression", "gzip").insertInto(stagingTableName)
      stagingDF = spark.read.table(stagingTableName)
    }

    // Check if the table is present. If not create the table dynamically using columns of dataframe
    if (!spark.catalog.tableExists(hiveTableName)) {
      val my_schema = stagingDF.schema
      val cols = my_schema.fields.map(x => s"`${x.name}` ${x.dataType.typeName}").mkString(",")
      val creatStatement = s"CREATE EXTERNAL TABLE ${hiveTableName} ($cols) STORED AS PARQUET location '$outputTablePath'"
      print(creatStatement)
      spark.sql(creatStatement)
    }


    //  Compare the columns in raw with column which are already there in table. In case of schema change, alter table to add new columns

    val existingColumns = spark.catalog.listColumns(hiveTableName).map(x => x.name.toLowerCase()).collect
    val newColumnList = stagingDF.columns.filter(x => !existingColumns.contains(x.toLowerCase))
    newColumnList.foreach { newCol => {
      spark.sql(s"ALTER TABLE ${hiveTableName} ADD COLUMNS (`${newCol}` STRING)")
    }
    }
    // In order to make dataframe and table in sync, add columns to dataframe and populate it as nulls
    val colList = stagingDF.columns.map(x => x.toLowerCase())
    existingColumns.filter(x => !colList.contains(x)).foreach { x => {
      stagingDF = stagingDF.withColumn(x, lit(null).cast(StringType))
    }
    }
    // Saving to output table in parquet format
    stagingDF.write.format("parquet").option("compression", "gzip").insertInto(hiveTableName)
  }

}