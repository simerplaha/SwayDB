package swaydb.utils

import scala.collection.mutable.ListBuffer

object UtilsTestKit {

  implicit class AggregatorImplicits(aggregator: Aggregator.type) {
    def apply[A](items: A*): Aggregator[A, ListBuffer[A]] = {
      val buffer: Aggregator[A, ListBuffer[A]] = Aggregator.listBuffer[A]
      buffer.addAll(items)
      buffer
    }
  }

}
