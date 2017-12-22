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

        def newer(e1: osm.Way, e2: osm.Way): osm.Way =
          if (e1.meta.version > e2.meta.version) e1 else e2

        val roadsBeforeTimestamp = roadsOnly.filter(_._2.meta.timestamp.isBefore(Instant.parse("2017-11-01T00:00:00.00Z")))
        val roadsAfterTimestamp = roadsOnly.filter(_._2.meta.timestamp.isAfter(Instant.parse("2017-11-01T00:00:00.00Z")))

        val latestRoadBeforeUpdate = roadsBeforeTimestamp.reduceByKey(newer)
        val latestRoadAfterUpdate = roadsOnly.reduceByKey(newer)

        /** Roads Created after timestamp */
        val roadsCreated = roadsAfterTimestamp.filter(_._2.meta.version == 1L)
        val roadsCreatedCount = roadsCreated.count()
        println(s"Roads created: $roadsCreatedCount")

        /** Roads deleted */
        val visibleRoads = roadsAfterTimestamp.filter(_._2.meta.visible.equals(true))
        val roadsDeleted = roadsAfterTimestamp.filter(_._2.meta.visible.equals(false))
        val roadsDeletedCount = roadsDeleted.count()
        println(s"Roads deleted: $roadsDeletedCount")

        /** Roads modified */
        val roadsModified = visibleRoads.filter(_._2.meta.version > 1L)
        val roadsModifiedCount = roadsModified.count()
        println(s"Roads modified: $roadsModifiedCount")

        /** Calculating what changes occurred before/after timestamp*/
        /*
        def tagChanges(e1: osm.Way, e2: osm.Way): Map[String, Long] =
          (e1.meta.tags.toSet diff e2.meta.tags.toSet)

        latestRoadAfterUpdate.union(latestRoadBeforeUpdate)

        println(s"Road counts: before: $roadCount1, after: $roadCount2")
        */
      }
    }
    ss.stop()
  }
}