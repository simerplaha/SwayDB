package swaydb.utils

import java.util.concurrent.ConcurrentHashMap
import scala.collection.mutable.ListBuffer
import org.scalatest.PrivateMethodTester._

import java.util.concurrent.atomic.AtomicLong

object UtilsTestKit {

  def getJavaMap[K, OV, V <: OV](maps: HashedMap.Concurrent[K, OV, V]): ConcurrentHashMap[K, V] =
    maps invokePrivate PrivateMethod[ConcurrentHashMap[K, V]](Symbol("map"))()

  def getAtomicLong(generator: IDGenerator): AtomicLong =
    generator invokePrivate PrivateMethod[AtomicLong](Symbol("atomicID"))()

  implicit class AggregatorImplicits(aggregator: Aggregator.type) {
    def apply[A](items: A*): Aggregator[A, ListBuffer[A]] = {
      val buffer: Aggregator[A, ListBuffer[A]] = Aggregator.listBuffer[A]
      buffer.addAll(items)
      buffer
    }
  }

}
