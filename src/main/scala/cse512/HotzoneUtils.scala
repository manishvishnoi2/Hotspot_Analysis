package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    // YOU NEED TO CHANGE THIS PART
    var rectangle = new Array[String](4)
    rectangle = queryRectangle.split(",")
    var r_x1 = rectangle(0).trim.toDouble
    var r_y1 = rectangle(1).trim.toDouble
    var r_x2 = rectangle(2).trim.toDouble
    var r_y2 = rectangle(3).trim.toDouble

    var point = new Array[String](2)
    point = pointString.split(",")
    var p_x1 = point(0).trim.toDouble
    var p_y1 = point(1).trim.toDouble

    var low_x = math.min(r_x1, r_x2)
    var high_x = math.max(r_x1, r_x2)
    var low_y = math.min(r_y1, r_y2)
    var high_y = math.max(r_y1, r_y2)


    if(p_x1  >= low_x && p_x1 <= high_x && p_y1 >= low_y && p_y1 <= high_y )
      return true
    else
      return false

  }

}
