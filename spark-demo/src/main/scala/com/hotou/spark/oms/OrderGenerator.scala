package com.hotou.spark.oms

class OrderGenerator {

  // All possible symbols
  val symbols = Array("IBM", "AAPL", "GS", "MS", "MSFT", "SPY")

  // All possible qty
  val qtyList = Array(100, 200, 500, 1000)

  // All actions
  val create="NEW"
  val cancel="CXL"
  val fill="FILL"

  // Next order id
  var nextOrderId = 0

  // Accumulated qty
  var exposure = 0

  // Risk limit qty
  val riskLimit = 10000

  // Live orders
  val orders = scala.collection.mutable.Map[Int, Int]()

  def getNextMessage: String = {

    // There is a chance that we get filled here
    if(exposure > 0 && (Math.random() * 100).toInt % 4 == 0) {
      val id = orders.keys.head
      val qty: Int = orders.remove(id).get
      exposure -= qty
      return s"$fill,$id,$getSymbol,$qty"
    }

    // can still create new order
    if(exposure <= riskLimit) {
      val qty = getQty
      val id = getNextOrderId
      orders.put(id, qty)
      exposure += qty
      return s"$create,$id,$getSymbol,$qty"
    }

    // need to reduce qty now
    val id = orders.keys.head
    val qty: Int = orders.remove(id).get
    exposure -= qty
    s"$cancel,$id,$getSymbol,$qty"
  }

  def getNextOrderId: Int = {
    val res = nextOrderId
    nextOrderId += 1
    res
  }
  private def getSymbol: String = symbols((Math.random() * 100).toInt % symbols.length)
  private def getQty: Int = qtyList((Math.random() * 100).toInt % qtyList.length)
}
