package supply_chain

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{EdgeDirection, Graph}
import org.apache.spark.rdd.RDD
import supply_chain.Utilities._


object SupplyChain {

  import org.apache.spark._
  import org.apache.spark.graphx.VertexId


  Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("org").setLevel(Level.WARN)

  lazy val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("SupplyChain")
  implicit lazy val sc: SparkContext = new SparkContext(conf)


  def findSubgraph(graph: Graph[VertexProperty, EdgeProperty],
                   sourceVertex: VertexId,
                   relationships: Array[String],
                   directions: Array[EdgeDirection]): Graph[VertexProperty, EdgeProperty] = {

    val initialGraph = graph.mapVertices((vertexId, _) => vertexId == sourceVertex).cache()

    val vertices_out = if (directions.contains(EdgeDirection.Out))
      initialGraph.pregel(false, activeDirection = EdgeDirection.Out)(
      (_, canReach, newCanReach) => canReach || newCanReach, // Vertex Program
      triplet => { // Send Message
        if (relationships.contains(triplet.attr.relationshipName) && triplet.srcAttr && !triplet.dstAttr) {
          Iterator((triplet.dstId, true))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a || b // Merge Message
    ).vertices.filter(v => v._2).map(e => e._1).collect() else Array()

    val vertices_in = if (directions.contains(EdgeDirection.In))
      initialGraph.pregel(false, activeDirection = EdgeDirection.In)(
      (_, canReach, newCanReach) => canReach || newCanReach, // Vertex Program
      triplet => { // Send Message
        if (relationships.contains(triplet.attr.relationshipName) && !triplet.srcAttr && triplet.dstAttr) {
          Iterator((triplet.srcId, true))
        } else {
          Iterator.empty
        }
      },
      (a, b) => a || b // Merge Message
    ).vertices.filter(v => v._2).map(e => e._1).collect() else Array()

    val vertices = vertices_out ++ vertices_in

    graph.subgraph(_ => true, (vertexId, _) => vertices.contains(vertexId))

  }

  def findAffectedProductsAndSites(graph: Graph[VertexProperty, EdgeProperty],
                                   sourceVertex: VertexId):RDD[(String, Iterable[VertexProperty], Int)] = {

    val affectedGraph = findSubgraph(graph, sourceVertex,
      Array("delivers to", "produces"), Array(EdgeDirection.Out))

    val affectedElements = affectedGraph.vertices
      .filter(e => e._1 != sourceVertex)
      .map(e => e._2)
      .groupBy(e => e.vertexType)
      .map(e => (e._1, e._2, e._2.size))

    affectedElements
  }

  def findDeliveryChain(graph: Graph[VertexProperty, EdgeProperty], sourceVertex: VertexId):Unit = {
    val deliveryChain = findSubgraph(graph, sourceVertex,
      Array("delivers to"), Array(EdgeDirection.Out, EdgeDirection.In))

    deliveryChain.vertices.map(e => e._2).collect().foreach(println(_))
    deliveryChain.edges.collect().foreach(println(_))
  }

  def main(args: Array[String]): Unit = {

    val sourceId: VertexId = 5

    val graph = loadGraph().cache()

    val affected = findAffectedProductsAndSites(graph, sourceId)
    affected.collect().foreach(println(_))

    affectedElements.foreach(println(_))



    sc.stop()
  }

}