package supply_chain


trait VertexProperty{ def vertexType: String}
trait EdgeProperty{ def relationshipName: String}

case class Factory(vertexType: String, siteId: Int, siteName: String, siteType: String) extends VertexProperty

case class Product(vertexType: String, productId: Int, productName: String) extends VertexProperty

case class Produces(relationshipName: String, price: Double) extends EdgeProperty

case class UsedBy(relationshipName: String, amount: Int) extends EdgeProperty

case class DeliversTo(relationshipName: String, itemId: Long, routeKm: Double, cost: Double) extends EdgeProperty
