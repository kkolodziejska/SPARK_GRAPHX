package supply_chain


object Utilities {

  import org.apache.spark._
  import org.apache.spark.graphx._
  import org.apache.spark.rdd.RDD


  val factoriesPath: String = "datasets/supply_chain/factories.csv"
  val productsPath: String = "datasets/supply_chain/products.csv"
  val productsDependenciesPath: String = "datasets/supply_chain/products_dependencies.csv"
  val factoriesDependenciesPath: String = "datasets/supply_chain/factories_dependencies.csv"


  def loadVertices(filePath: String, vertexType: String)(implicit sc: SparkContext): RDD[(VertexId, VertexProperty)] = {
    val rawData = sc.textFile(filePath)
    val header = rawData.first()
    rawData
      .filter(line => line != header)
      .map(line => {
        val lineList = line.split(",")
        vertexType match {
          case "factory" => (lineList(0).toLong, Factory(vertexType, lineList(0).toInt, lineList(1), lineList(2)))
          case "product" => (lineList(0).toLong + 100L, Product(vertexType, lineList(0).toInt, lineList(1)))
        }
      })
  }


  def loadEdges(filePath: String, edgeType: String)(implicit sc: SparkContext): RDD[Edge[EdgeProperty]] = {
    val rawData = sc.textFile(filePath)
    val header = rawData.first()

    rawData
      .filter(line => line != header)
      .map(line => {
        val lineList = line.split(",")
        edgeType match {
          case "factoryToProduct" =>
            Edge(lineList(3).toLong, lineList(0).toLong + 100L,
              Produces("produces", lineList(2).toDouble))
          case "factoryToFactory" =>
            Edge(lineList(0).toLong, lineList(1).toLong,
              DeliversTo("delivers to", lineList(2).toLong + 100L, lineList(3).toDouble, lineList(4).toDouble))
          case "productToProduct" =>
            Edge(lineList(0).toLong + 100L, lineList(1).toLong + 100L, UsedBy("used by", lineList(2).toInt))
        }

      })
  }

  def loadGraph()(implicit sc: SparkContext): Graph[VertexProperty, EdgeProperty] = {

    val factories = loadVertices(factoriesPath, "factory")
    val products = loadVertices(productsPath, "product")


    val vertices = factories ++ products


    val productsFactoriesEdges = loadEdges(productsPath, "factoryToProduct")
    val productsEdges = loadEdges(productsDependenciesPath, "productToProduct")
    val factoriesEdges = loadEdges(factoriesDependenciesPath, "factoryToFactory")


    val edges = productsFactoriesEdges ++ productsEdges ++ factoriesEdges

    Graph(vertices, edges)
  }


  def getAllRelationships(graph: Graph[VertexProperty, EdgeProperty]): RDD[String] = {
    graph.triplets.map(triplet => (triplet.srcAttr, triplet.attr, triplet.dstAttr) match {
      case (Factory(_,_, site, _), Produces(rel, _), Product(_,_, product)) =>
        site + " " + rel + " " + product
      case (Factory(_,_, site1, _), DeliversTo(rel, _, _, _), Factory(_,_, site2, _)) =>
        site1 + " " + rel + " " + site2
      case (Product(_,_, product1), UsedBy(rel, _), Product(_,_, product2)) =>
        product1 + " is " + rel + " " + product2
      case _ => "WRONG RELATIONSHIP"
    })
  }

}