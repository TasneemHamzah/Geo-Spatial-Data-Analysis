package cse512

import org.apache.log4j.{Level, Logger}

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HotzoneAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotZoneAnalysis(spark: SparkSession, pointPath: String, rectanglePath: String): DataFrame = {

    var pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pointDf.createOrReplaceTempView("point")

    // Parse point data formats
    spark.udf.register("trim",(string : String)=>(string.replace("(", "").replace(")", "")))
    pointDf = spark.sql("select trim(_c5) as _c5 from point")
    pointDf.createOrReplaceTempView("point")

    // Load rectangle data
    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(rectanglePath);
    rectangleDf.createOrReplaceTempView("rectangle")

    // Join two datasets
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>(HotzoneUtils.ST_Contains(queryRectangle, pointString)))
    val joinDf = spark.sql("select rectangle._c0 as rectangle, point._c5 as point from rectangle,point where ST_Contains(rectangle._c0,point._c5)")
    
    joinDf.createOrReplaceTempView("joinResult")
    
    // YOU NEED TO CHANGE THIS PART
    // Get count of points
    //val retDf = spark.sql("select rectangle, count(point) from joinResult group by rectangle order by rectangle")
    val groupDf = joinDf.groupBy("rectangle").count()
    //val groupDf = joinDf.groupBy("rectangle._0").count()
    //Console.println(groupDf1)
    
    // Sort ascending by rectangle
    val sortDf = groupDf.sort("rectangle")
    //Console.println(retDf.show())
    return sortDf.coalesce(1)
    // Return sorted df
    //return  sortDf
  }

}