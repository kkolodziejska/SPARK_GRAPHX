package supply_chain


class VertexProperty()
class EdgeProperty()

case class Factory(site_id: Int, site_name: String, site_type: String) extends VertexProperty

case class Product(product_id: Int, product_name: String) extends VertexProperty

case class Produces(relationship_name: String, price: Double) extends EdgeProperty

case class UsedBy(relationship_name: String, amount: Int) extends EdgeProperty

case class DeliversTo(relationship_name: String, item_id: Int, route_km: Double, cost: Double) extends EdgeProperty
