import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.IntParam
import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.util.StatCounter
import org.apache.log4j._


case class Flight(Year:String, Month:String, DayofMonth:String,DayOfWeek:String,DepTime:String,
                  CRSDepTime:String,ArrTime:String,CRSArrTime:String,UniqueCarrier:String,FlightNum:String,
                 TailNum:String,ActualElapsedTime:String,CRSElapsedTime:String,AirTime:String,
                  ArrDelay:String,DepDelay:String,Origin:String,Dest:String,Distance:String,TaxiIn:String,TaxiOut:String,
                  Cancelled:String,CencellationCode:String,Diverted:String,CarrierDelay:String, WeatherDelay:String,
                  NASDelay:String, SecurityDelay:String, LateAircraftDelay:String)
object SuvaAssignment{


  def main(args:Array[String]): Unit ={

    Logger.getLogger("org").setLevel(Level.ERROR)

    def parseFlight(str:String):Flight={
      val line=str.split(",")

      Flight(line(0),line(1),line(2),line(3),line(4),
        line(5),line(6),line(7),line(8),line(9),
        line(10),line(11),line(12),line(13),line(14),
        line(15),line(16),line(17),line(18),line(19),
        line(20),line(21),line(22),line(23),line(24),line(25),
        line(26),line(27),line(28))

    }

    val sc=new SparkContext("local[*]", "Graphx")

    import scala.util.hashing.MurmurHash3
    // import spark.implicits._
   //val spark=SparkSession.builder().appName("Graphx").master("local[*]").getOrCreate()
   //val data=spark.read.option("header","true").option("inferSchema","true").csv("./src/main/resources/2008.csv.bz2")
    // data.show()
    //val sfoData=data.select("$")
    // data.filter("origin='SFO'").show()
  // val name=sc.textFile("./src/main/resources/2008.csv.bz2")

   // name.take(10).foreach(println)

  //val flightData=name.map(x=>x.split(",")(17).toString).filter(x=>x=="SFO")

    //flightData.foreach(println)
   //import spark.implicits._
  // val flightsFromTo = data.select($"Origin",$"Dest")
   //val airportCodes = data.select($"Origin", $"Dest").flatMap(x => Iterable(x(0).toString, x(1).toString))

    //airportCodes.show()
  //val airportVertices: RDD[(VertexId, String)] = airportCodes.distinct().map(x=> (MurmurHash3.stringHash(x),x))

   val flightsData=sc.textFile("./src/main/resources/2008.csv.bz2")

   val flights=flightsData.map(parseFlight).cache()

    val airportCodes = flights.flatMap { flight => Seq(flight.Origin, flight.Dest) }
    val airportVertices: RDD[(VertexId, String)] =
      airportCodes.distinct().map(x => (MurmurHash3.stringHash(x).toLong, x))

    val flightEdges = flights.map(f =>
      ((MurmurHash3.stringHash(f.Origin), MurmurHash3.stringHash(f.Dest)), 1))
      .reduceByKey(_+_)
      .map {
        case ((src, dest), attr) => Edge(src, dest, attr)
      }

    val graph = Graph(airportVertices, flightEdges)
    if (true) {
      println("\nNumber of airports in the graph:")
      println(graph.numVertices)
      println("\nNumber of flights in the graph:")
      println(graph.numEdges)
    }



  }
}
