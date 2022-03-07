package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  // YOU NEED TO CHANGE THIS PART
  def sqrCalc(point:Int):Double= {return(point*point).toDouble}

  def calcNeibor(px1:Int, py1:Int, pz1:Int, px2:Int, py2:Int, pz2:Int, px:Int, py:Int, pz:Int): Int = {
    
    var x = 0
    if (px == px1 || px == px2)
      {
        x += 1
      }
    if (py == py1 || py == py2)
      {
        x += 1
      }
    if (pz == pz1 || pz == pz2)
      {
        x += 1
      }
    if (x == 1) {return 18}
    else if (x == 2){return 12}
    else if (x == 3){return 8}
    else{return 27}
  }
  
  def gScores(x:Int, y:Int, z:Int, mean:Double, std:Double, countNumber:Int, sumNumber:Int, numOfCells:Int): Double =
  {
    val x = (sumNumber.toDouble - (mean*countNumber.toDouble))
    val y = std*math.sqrt((((numOfCells.toDouble*countNumber.toDouble) -(countNumber.toDouble*countNumber.toDouble))/(numOfCells.toDouble-1.0).toDouble).toDouble).toDouble
    return (x/y ).toDouble
    //return score.toDouble
  }
}
