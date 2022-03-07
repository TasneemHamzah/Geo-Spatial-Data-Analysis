package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
  {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")
    pickupInfo.show()

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY",(pickupPoint: String)=>((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ",(pickupTime: String)=>((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")

    pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
    pickupInfo.show()

    // Define the min and max of x, y, z
    val minX = -74.50/HotcellUtils.coordinateStep
    val maxX = -73.70/HotcellUtils.coordinateStep
    val minY = 40.50/HotcellUtils.coordinateStep
    val maxY = 40.90/HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  // YOU NEED TO CHANGE THIS PART
  pickupInfo.createOrReplaceTempView("hotspot")
  
  val hotspotCouns = spark.sql("select x,y,z,count(*) as cc from hotspot group by z,y,x order by z,y,x ").persist()
  hotspotCouns.createOrReplaceTempView("hotspotCouns")
  //hotspotCouns.show()

  spark.udf.register("sqrCalc", (inputX: Int) => (HotcellUtils.sqrCalc(inputX)))
  val pSum = spark.sql("select count(*) as cnt, sum(cc) as sumcc, sum(sqrCalc(cc)) as sumSquared from hotspotCouns  ")
  pSum.createOrReplaceTempView("pSum")
  //pSum.show()

  val psum0= pSum.first().getLong(1)
  val psum1= pSum.first().getDouble(2)
  val psum2= pSum.first().getLong(0)
  
  val mean =(psum0.toDouble / numCells.toDouble).toDouble;
  val std = math.sqrt(((psum1.toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble
  //println(mean)
  //println(SD)
  //println(numCells)
  
  spark.udf.register("calcNeibor", (px1: Int, py1: Int, pz1: Int, px2: Int, py2: Int, pz2: Int,px:Int, py:Int, pz:Int) => (HotcellUtils.calcNeibor(px1, py1, pz1, px2, py2, pz2,px,py,pz)))
  val neighb = spark.sql("select calcNeibor("+minX + "," + minY + "," + minZ + "," + maxX + "," + maxY + "," + maxZ + "," + "a1.x,a1.y,a1.z) as nCount, count(*) as countall, a1.x as x,a1.y as y,a1.z as z, sum(a2.cc) as sumtotal from hotspotCouns as a1, hotspotCouns as a2 where (a2.x=a1.x+1 or a2.x=a1.x or a2.x=a1.x-1) and (a2.y=a1.y+1 or a2.y=a1.y or a2.y=a1.y-1) and (a2.z=a1.z+1 or a2.z=a1.z or a2.z=a1.z-1) group by a1.z,a1.y,a1.x order by a1.z,a1.y,a1.x  ").persist()
  neighb.createOrReplaceTempView("neighbdf")
  //neighb.show()

  spark.udf.register("gScore", (x:Int,y:Int,z:Int,mean:Double,std:Double,countn:Int,sumn:Int,numcells:Int) => ((HotcellUtils.gScores(x,y,z,mean,std,countn,sumn,numcells))))
  

  val resultDf1 = spark.sql("select gScore(x,y,z,"+mean+","+std+",ncount,sumtotal,"+numCells+") as gScore1,x,y,z from neighbdf order by gScore1 desc, x desc, z desc, y desc")
  resultDf1.createOrReplaceTempView("resultDf1")
  resultDf1.show()
  val result = spark.sql("select x, y, z from resultDf1 ")
  result.createOrReplaceTempView("result")
  //result.show()
  return result
//.lemit(50)
  
  
  //return pickupInfo // YOU NEED TO CHANGE THIS PART
}
}

