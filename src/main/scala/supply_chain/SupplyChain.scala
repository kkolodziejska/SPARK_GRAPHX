package supply_chain

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.EdgeDirection
import supply_chain.Utilities._


object SupplyChain {

  import org.apache.spark._
  import org.apache.spark.graphx.VertexId


  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  lazy val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SupplyChain")
  implicit lazy val sc: SparkContext = new SparkContext(conf)


  def main(args: Array[String]): Unit = {

    val graph = loadGraph().cache()

//    val facts = getAllRelationships(graph)
//    facts.collect.foreach(println(_))

    val sourceId: VertexId = 11

    val initialGraph = graph.mapVertices((vertexId,VD) => (VD, vertexId == sourceId))


    val affected = initialGraph.pregel(false, activeDirection = EdgeDirection.Out)(
      (vertexId, canReach, newCanReach) => (canReach._1, canReach._2 || newCanReach), // Vertex Program
      triplet => { // Send Message
        if (triplet.srcAttr._2 && !triplet.dstAttr._2) {
          Iterator((triplet.dstId, true))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a || b // Merge Message
    )

    val affectedElements =
      affected.vertices
      .filter(e => e._2._2 && e._1 != sourceId)
        .map(e =>
          e._2._1 match {
            case Product(_,_) => (e._2._1, "product")
            case Factory(_,_,_) => (e._2._1, "factory")
          }
        )
        .groupBy(e => e._2)
        .map(e => (e._1, e._2, e._2.size))
        .collect()

    affectedElements.foreach(println(_))



    sc.stop()
  }

}