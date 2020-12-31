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
  pickupInfo.createOrReplaceTempView("pickupPoints")
  pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)
  spark.udf.register("squared", (inputX: Int) => (((inputX*inputX).toDouble)))
  
  // Select cells with co-ordinates in the given range
  pickupInfo = spark.sql("select x,y,z from pickupPoints where x>= " + minX + " and x<= " + maxX + " and y>= " + minY + " and y<= " + maxY + " and z>= " + minZ + " and z<= " + maxZ + " order by z,y,x")
  pickupInfo.createOrReplaceTempView("usedPickupPoints")
  
 // Count total no of pickups for each cell
  pickupInfo = spark.sql("select x, y, z, count(*) as totalPickups from usedPickupPoints group by x, y, z order by z,y,x")
  pickupInfo.createOrReplaceTempView("pickupCounts")
  
  // Calculate total Pickups for all cells
  var totalSumDf = spark.sql("select sum(totalPickups) as pickupSum, sum(squared(totalPickups)) as sqPickupSum from pickupCounts")
  totalSumDf.createOrReplaceTempView("totalSum")
  
  // find mean and SD
  val mean = (totalSumDf.first().getLong(0).toDouble / numCells.toDouble).toDouble

  
  val standardDeviation = scala.math.sqrt(((totalSumDf.first().getDouble(1).toDouble / numCells.toDouble) - (mean.toDouble * mean.toDouble))).toDouble
  
  
  spark.udf.register("countNeighbours", (inputX: Int, inputY: Int, inputZ: Int, minX: Int, maxX: Int, minY: Int, maxY: Int, minZ: Int, maxZ: Int) => ((HotcellUtils.countNeighbours(inputX, inputY, inputZ, minX, minY, minZ, maxX, maxY, maxZ))))
  
  // find neighbour cells along with their count
  var neighbourCellDf = spark.sql("select countNeighbours(sch1.x, sch1.y, sch1.z, " + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ") as neighbourCellCount,"
  		+ "sch1.x as x, sch1.y as y, sch1.z as z, "
  		+ "sum(sch2.totalPickups) as pickupSum "
  		+ "from pickupCounts as sch1, pickupCounts as sch2 "
  		+ "where (sch2.x = sch1.x+1 or sch2.x = sch1.x or sch2.x = sch1.x-1) "
  		+ "and (sch2.y = sch1.y+1 or sch2.y = sch1.y or sch2.y = sch1.y-1) "
  		+ "and (sch2.z = sch1.z+1 or sch2.z = sch1.z or sch2.z = sch1.z-1) "
  		+ "group by sch1.z, sch1.y, sch1.x "
  		+ "order by sch1.z, sch1.y, sch1.x")
	neighbourCellDf.createOrReplaceTempView("neighbourCells")
  
    
  spark.udf.register("zScore", (neighbourCellCount: Int, pickupSum: Int, numCells: Int, x: Int, y: Int, z: Int, mean: Double, standardDeviation: Double) => ((HotcellUtils.calculateZScore(neighbourCellCount, pickupSum, numCells, x, y, z, mean, standardDeviation))))

  // cells along with their ZScore
  var hotCells = spark.sql("select zScore(neighbourCellCount, pickupSum, "+ numCells + ", x, y, z," + mean + ", " + standardDeviation + ") as getisOrdStatistic, x, y, z from neighbourCells order by getisOrdStatistic desc")
  
  hotCells.createOrReplaceTempView("zScores")
  // Top 50 hot Cells
  hotCells = spark.sql("select x, y, z from zScores limit 50")

  hotCells.show()
  return hotCells

}
}
