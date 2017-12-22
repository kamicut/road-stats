import org.apache.spark.sql._
import scala.util.{Success, Failure}
import org.apache.spark.rdd.RDD
import java.time.Instant
import vectorpipe._

object StatsJob {
  def main(args: Array[String]) {
    val path: String = "/Users/marc/data/maryland.orc"

    implicit val ss: SparkSession =
      SparkSession.builder.master("local[*]").appName("Stats Job").enableHiveSupport.getOrCreate

    osm.fromORC(path) match {
      case Failure(err) => {
        println("failure")
      }
      case Success((ns,ws,rs)) => {
        val roadsOnly: RDD[(Long, osm.Way)] =
          ws.filter(_._2.meta.tags.contains("highway"))

        val roadsAfterTimestamp = roadsOnly.filter(_._2.meta.timestamp.isAfter(Instant.parse("2017-11-01T00:00:00.00Z")))
        println(roadsAfterTimestamp.count())
      }  /* (RDD[(Long, Node)], RDD[(Long, Way)], RDD[(Long, Relation)]) */
    }
    ss.stop()
  }
}