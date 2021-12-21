package swaydb.utils

import org.scalatest.PrivateMethodTester._

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.collection.mutable.ListBuffer
import scala.util.Random

object UtilsTestKit {

  def invokePrivate_map[K, OV, V <: OV](maps: HashedMap.Concurrent[K, OV, V]): ConcurrentHashMap[K, V] =
    maps invokePrivate PrivateMethod[ConcurrentHashMap[K, V]](Symbol("map"))()

  def invokePrivate_atomicID(generator: IDGenerator): AtomicLong =
    generator invokePrivate PrivateMethod[AtomicLong](Symbol("atomicID"))()

  implicit class AggregatorImplicits(aggregator: Aggregator.type) {
    def apply[A](items: A*): Aggregator[A, ListBuffer[A]] = {
      val buffer: Aggregator[A, ListBuffer[A]] = Aggregator.listBuffer[A]
      buffer.addAll(items)
      buffer
    }
  }

  implicit class ExtensionsImplicits(extension: Extension.type) {
    def gen(): Extension =
      Random.shuffle(Extension.all.toList).head
  }

}
