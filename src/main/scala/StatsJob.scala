import org.apache.spark.sql._
import scala.util.{Success, Failure}
import java.time.Instant
import org.apache.spark.rdd.RDD
import vectorpipe._

import geotrellis.proj4.WebMercator
import geotrellis.spark._
import geotrellis.spark.tiling._
import geotrellis.vectortile.VectorTile
import geotrellis.vector.io._

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

        def newerNode(e1: osm.Node, e2: osm.Node): osm.Node =
          if (e1.meta.version > e2.meta.version) e1 else e2

        val roadsBeforeTimestamp = roadsOnly.filter(_._2.meta.timestamp.isBefore(Instant.parse("2017-11-01T00:00:00.00Z")))
        val roadsAfterTimestamp = roadsOnly.filter(_._2.meta.timestamp.isAfter(Instant.parse("2017-11-01T00:00:00.00Z")))

        val latestRoadBeforeUpdate = roadsBeforeTimestamp.reduceByKey(newer)
        val latestRoadAfterUpdate = roadsOnly.reduceByKey(newer)

        /** Edit breakdown **/
        Calculations.printEditBreakdown(roadsAfterTimestamp)

        /** Tag breakdown **/
        Calculations.printTagBreakdown(latestRoadBeforeUpdate, latestRoadAfterUpdate)

        /** Restriction **/
        Calculations.printRestrictions(rs)

        /** Edited Roads **/
        val roadsToMap = roadsAfterTimestamp.reduceByKey(newer).filter(_._2.meta.visible.equals(true)).repartition(100)
        val nodesToMap = ns.filter(_._2.meta.timestamp.isBefore(Instant.parse("2017-11-01T00:00:00.00Z")))
          .reduceByKey(newerNode).repartition(100)

        Calculations.saveEditedGeojson(nodesToMap, roadsToMap, "edited.geojson", ss)

      }
    }
    ss.stop()
  }
}

object Calculations {
  def saveEditedGeojson(nodes: RDD[(Long, osm.Node)], ways: RDD[(Long, osm.Way)], filename: String, ss: SparkSession) = {
    val features = osm.features(nodes, ways, ss.sparkContext.emptyRDD).lines
    val writer = new java.io.PrintWriter(new java.io.File(filename))
    val allFeatures = features.map(f => f.geom.toGeoJson()).collect().mkString("\n")
    writer.append(allFeatures)
    writer.close()
  }

  def printRestrictions(rs: RDD[(Long, osm.Relation)]) = {
    val restrictions: RDD[(Long, osm.Relation)] =
      rs.filter(_._2.meta.tags.getOrElse("type", "") == "restriction")

    def newerRelation(e1: osm.Relation, e2: osm.Relation): osm.Relation =
      if (e1.meta.version > e2.meta.version) e1 else e2

    val restrictionsBeforeTimeStamp = restrictions.filter(_._2.meta.timestamp.isBefore(Instant.parse("2017-11-01T00:00:00.00Z")))
    val latestRestrictionBeforeUpdate = restrictionsBeforeTimeStamp.reduceByKey(newerRelation).count()
    val latestRestrictionAfterUpdate = restrictions.reduceByKey(newerRelation).count()

    println(s"Turn restriction counts: before: $latestRestrictionBeforeUpdate, after: $latestRestrictionAfterUpdate")
  }

  def printEditBreakdown(roadsAfterTimestamp: RDD[(Long, osm.Way)]) = {
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
  }

  def printTagBreakdown(latestRoadBeforeUpdate: RDD[(Long, osm.Way)], latestRoadAfterUpdate: RDD[(Long, osm.Way)]) = {
    /** Calculating what changes occurred before/after timestamp*/
    val hasSurfaceBefore = latestRoadBeforeUpdate.filter(_._2.meta.tags.contains("surface")).count()
    val hasSurfaceAfter = latestRoadAfterUpdate.filter(_._2.meta.tags.contains("surface")).count()
    println(s"Has surface counts: before: $hasSurfaceBefore, after: $hasSurfaceAfter")

    val hasOneWayBefore = latestRoadBeforeUpdate.filter(_._2.meta.tags.contains("oneway")).count()
    val hasOneWayAfter = latestRoadAfterUpdate.filter(_._2.meta.tags.contains("oneway")).count()
    println(s"Has oneway counts: before: $hasOneWayBefore, after: $hasOneWayAfter")

  }
}