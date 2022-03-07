package cse512



object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // Implemented ST contains
     val qrange = queryRectangle.split(",")
     val rangep11 = qrange(0).toDouble
     val rangep12 = qrange(1).toDouble
     val rangep21 = qrange(2).toDouble
     val rangep22 = qrange(3).toDouble
     val p11 = pointString.split(",")(0).toDouble
     val p12 = pointString.split(",")(1).toDouble

     ((rangep21 >= p11 && p11 >= rangep11 ) || (rangep21 <= p11 && p11 <= rangep11)) && ((rangep22 >= p12 && p12 >= rangep12) || (rangep22 <= p12 && p12 <=rangep12))
  
  
  }


}

  // YOU NEED TO CHANGE THIS PART


